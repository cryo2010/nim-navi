## Shared test helper: a one-shot in-process HTTP/1.1 server on a thread.
## Filename has no leading 't' so `nimble test` does not treat it as a suite.

import std/[net, strutils, os]

when defined(windows):
  import std/nativesockets
  from std/winlean import accept   # selectively: winlean's AF_* ints would
                                   # shadow nativesockets' Domain enum values

  proc acceptClient(server: Socket): Socket =
    ## std/net's accept hands Winsock a 16-byte SockAddr, but an inbound IPv6
    ## peer address needs 28: Windows fails that call with WSAEFAULT where POSIX
    ## just truncates. (nativesockets then closes the invalid handle, so the code
    ## that finally surfaces is a misleading WSAENOTSOCK.) Accept through a
    ## storage-sized buffer instead, so the IPv6 servers here work on Windows.
    var storage: array[128, byte]
    var slen = sizeof(storage).SockLen
    let fd = accept(server.getFd, cast[ptr SockAddr](addr storage), addr slen)
    if fd == osInvalidSocket: raiseOSError(osLastError())
    newSocket(fd, getSockDomain(fd), SOCK_STREAM, IPPROTO_TCP)
else:
  proc acceptClient(server: Socket): Socket =
    server.accept(result)

type ServerCtx* = object
  port: int
  portOut: ptr int      ## when set, bind an ephemeral port and report it here
  ready: ptr bool
  ipv6: bool
  payload: string
  failures: int
  count: ptr int        ## when set, count the requests the server answered

proc waitFlag*(flag: ptr bool) =
  ## Poll a cross-thread bool (a server thread's readiness/closed signal), yielding
  ## the CPU between checks. A bare `while not flag[]: discard` busy-spin pins a core
  ## and, under the parallel test load `checkmate` creates (one process per file on a
  ## 2-core CI runner), can starve the very server thread that sets the flag -- which
  ## surfaced as an intermittent `(timed out)` hang. `sleep(1)` here is `os.sleep`;
  ## callers in the async suites use this helper instead of a bare `sleep` (which the
  ## async backends shadow with a `Future`-returning version).
  while not flag[]: sleep(1)

proc hexToBytes*(hex: string): string =
  for i in countup(0, hex.len - 2, 2):
    result.add char(parseHexInt(hex[i .. i + 1]))

proc serveRaw(ctx: ServerCtx) {.thread.} =
  ## Read one request, then send `payload` verbatim and close.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  var req = ""
  while true:
    let c = client.recv(1)
    if c.len == 0: break
    req.add c
    if req.len >= 4 and req[^4 .. ^1] == "\r\n\r\n": break
  client.send(ctx.payload)
  client.close()
  server.close()

proc startRaw*(th: var Thread[ServerCtx], port: int, payload: string) =
  ## Serve a single connection that replies with `payload`.
  var ready = false
  createThread(th, serveRaw, ServerCtx(port: port, ready: addr ready, payload: payload))
  while not ready: sleep(1)

proc serveEchoLine(ctx: ServerCtx) {.thread.} =
  ## Read one request and reply 200 with the request line (verb target version)
  ## as the body, so a test can assert the exact target that was sent.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  var req = ""
  while true:
    let c = client.recv(1)
    if c.len == 0: break
    req.add c
    if req.len >= 4 and req[^4 .. ^1] == "\r\n\r\n": break
  let line = req.splitLines()[0]
  client.send("HTTP/1.1 200 OK\r\nContent-Length: " & $line.len &
              "\r\nConnection: close\r\n\r\n" & line)
  client.close()
  server.close()

proc startEchoLine*(th: var Thread[ServerCtx], port: int) =
  ## Serve a single connection that echoes the request line as the body.
  var ready = false
  createThread(th, serveEchoLine, ServerCtx(port: port, ready: addr ready))
  while not ready: sleep(1)

proc serveHang(ctx: ServerCtx) {.thread.} =
  ## Accept a connection, read the request, then never reply (for timeout tests).
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0), "127.0.0.1")     # ephemeral: no cross-iteration collision
  server.listen()
  ctx.portOut[] = server.getLocalAddr()[1].int
  ctx.ready[] = true
  var client = acceptClient(server)
  var req = ""
  while true:
    let c = client.recv(1)
    if c.len == 0: break
    req.add c
    if req.len >= 4 and req[^4 .. ^1] == "\r\n\r\n": break
  sleep(600)  # hold the request open past the client's timeout, then clean up
  client.close()
  server.close()

proc serveHangCount(ctx: ServerCtx) {.thread.} =
  ## Accept up to `failures` connections; read each request, tally it into `count`,
  ## and hold the connection open WITHOUT replying (never reads a second request on
  ## it). For per-attempt timeout tests: every attempt stalls until the client's
  ## per-attempt deadline fires, and the client opens a fresh connection to retry,
  ## so `count` ends up equal to the number of attempts the client made. Accepts are
  ## not delayed by the holds (held sockets are parked, not serviced), so the count
  ## is reliable regardless of client backoff timing. Exits once `failures`
  ## connections have been accepted, so the thread can be joined.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0), "127.0.0.1")     # ephemeral: no cross-iteration collision
  server.listen()
  ctx.portOut[] = server.getLocalAddr()[1].int
  ctx.ready[] = true
  var held: seq[Socket]
  while held.len < ctx.failures:
    var client = acceptClient(server)
    var req = ""
    while true:
      let c = client.recv(1)
      if c.len == 0: break
      req.add c
      if req.len >= 4 and req[^4 .. ^1] == "\r\n\r\n": break
    if ctx.count != nil: inc ctx.count[]
    held.add client                          # park it open; never reply
  # Keep every connection open a while after the LAST accept, so the client's final
  # attempt also hits its per-attempt timeout instead of seeing a premature close
  # (which would surface as an IOError, not the TimeoutError under test).
  sleep(500)
  for c in held: c.close()
  server.close()

proc startHangCount*(th: var Thread[ServerCtx], port: var int, conns: int, count: ptr int) =
  ## Serve `conns` connections that each accept + read + stall (never reply), on an
  ## ephemeral port (written to `port`), tallying accepted requests into `count`.
  var ready = false
  createThread(th, serveHangCount,
    ServerCtx(portOut: addr port, ready: addr ready, failures: conns, count: count))
  while not ready: sleep(1)

proc serveAlways503(ctx: ServerCtx) {.thread.} =
  ## Answer every request with 503 on one kept-alive connection, forever, tallying
  ## each answered request into `count`. For total-deadline retry tests: the client
  ## keeps retrying a retryable status, so the deadline (not the server) must stop it.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0), "127.0.0.1")       # ephemeral: no cross-iteration collision
  server.listen()
  ctx.portOut[] = server.getLocalAddr()[1].int
  ctx.ready[] = true
  var client = acceptClient(server)
  while true:
    var req = ""
    while true:
      let c = client.recv(1)
      if c.len == 0: break
      req.add c
      if req.len >= 4 and req[^4 .. ^1] == "\r\n\r\n": break
    if req.len == 0: break
    client.send("HTTP/1.1 503 Service Unavailable\r\n" &
                "Content-Length: 0\r\nConnection: keep-alive\r\n\r\n")
    if ctx.count != nil: inc ctx.count[]
  client.close()
  server.close()

proc startAlways503*(th: var Thread[ServerCtx], port: var int, count: ptr int) =
  ## Serve unlimited 503s on an ephemeral port (written to `port`), counting the
  ## requests answered into `count`. The connection is kept alive so retries reuse it.
  var ready = false
  createThread(th, serveAlways503,
    ServerCtx(portOut: addr port, ready: addr ready, count: count))
  while not ready: sleep(1)

proc startHang*(th: var Thread[ServerCtx], port: var int) =
  ## Serve a single connection that accepts but never responds. Binds an
  ## ephemeral port (so a leaked thread never collides on a re-run) and writes it
  ## to `port`, which must be a mutable `var`.
  var ready = false
  createThread(th, serveHang, ServerCtx(portOut: addr port, ready: addr ready))
  while not ready: sleep(1)

proc headerValue(head, name: string): string =
  for line in head.split("\r\n"):
    let idx = line.find(':')
    if idx > 0 and cmpIgnoreCase(line[0 ..< idx].strip, name) == 0:
      return line[idx + 1 .. ^1].strip

proc serveBodyEcho(ctx: ServerCtx) {.thread.} =
  ## Read a Content-Length body and echo it back, reflecting the request's
  ## Content-Type in an x-echo-content-type response header.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  var head = ""
  while true:
    let c = client.recv(1)
    if c.len == 0: break
    head.add c
    if head.len >= 4 and head[^4 .. ^1] == "\r\n\r\n": break
  let cl = headerValue(head, "content-length")
  let n = if cl.len > 0: parseInt(cl) else: 0
  var body = ""
  while body.len < n:
    let part = client.recv(n - body.len)
    if part.len == 0: break
    body.add part
  client.send("HTTP/1.1 200 OK\r\n" &
              "x-echo-method: " & head.split(' ')[0] & "\r\n" &
              "x-echo-content-type: " & headerValue(head, "content-type") & "\r\n" &
              "x-echo-authorization: " & headerValue(head, "authorization") & "\r\n" &
              "Content-Length: " & $body.len & "\r\n" &
              "Connection: close\r\n\r\n" & body)
  client.close()
  server.close()

proc startBodyEcho*(th: var Thread[ServerCtx], port: int) =
  var ready = false
  createThread(th, serveBodyEcho, ServerCtx(port: port, ready: addr ready))
  while not ready: sleep(1)

proc readChunkedBody(client: Socket): string =
  ## Decode a chunked transfer-encoding request body: repeatedly read a hex length
  ## line, then that many bytes plus the trailing CRLF, until the 0-length chunk.
  ## Trailers (if any) after the terminator are drained but not returned.
  while true:
    var sizeLine = ""
    while true:                       # read up to CRLF (the chunk-size line)
      let c = client.recv(1)
      if c.len == 0: return
      sizeLine.add c
      if sizeLine.len >= 2 and sizeLine[^2 .. ^1] == "\r\n": break
    let n = parseHexInt(sizeLine.strip.split(';')[0])   # ignore any chunk extensions
    if n == 0:
      # trailer section (possibly empty): read until the terminating CRLF
      var trl = ""
      while true:
        let c = client.recv(1)
        if c.len == 0: break
        trl.add c
        if trl.len >= 2 and trl[^2 .. ^1] == "\r\n" and
           (trl.len == 2 or trl[^4 .. ^1] == "\r\n\r\n"): break
      return
    var got = 0
    while got < n:
      let part = client.recv(n - got)
      if part.len == 0: return
      result.add part
      got += part.len
    discard client.recv(2)            # the CRLF after the chunk data

proc serve503Once(ctx: ServerCtx) {.thread.} =
  ## Answer exactly one request with 503, reading its body (Content-Length or
  ## chunked) so the socket stays framed, then close and exit. `Connection: close`
  ## so the client releases the socket rather than pooling it (the thread's read
  ## then returns "" and it can join cleanly). For the non-replayable-body test: a
  ## retryable-by-method PUT whose body is a streamed producer must NOT be retried,
  ## so exactly one request is answered (`count == 1`) and the client sees the 503.
  ## A wrongful retry would open a second connection this server never accepts, so
  ## the client would surface an error instead of the clean 503 the test asserts.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0), "127.0.0.1")
  server.listen()
  ctx.portOut[] = server.getLocalAddr()[1].int
  ctx.ready[] = true
  var client = acceptClient(server)
  var head = ""
  while true:
    let c = client.recv(1)
    if c.len == 0: break
    head.add c
    if head.len >= 4 and head[^4 .. ^1] == "\r\n\r\n": break
  if head.len > 0:
    if cmpIgnoreCase(headerValue(head, "transfer-encoding"), "chunked") == 0:
      discard readChunkedBody(client)
    else:
      let cl = headerValue(head, "content-length")
      let n = if cl.len > 0: parseInt(cl) else: 0
      var got = 0
      while got < n:
        let part = client.recv(n - got)
        if part.len == 0: break
        got += part.len
    if ctx.count != nil: inc ctx.count[]
    client.send("HTTP/1.1 503 Service Unavailable\r\n" &
                "Content-Length: 0\r\nConnection: close\r\n\r\n")
  client.close()
  server.close()

proc start503Once*(th: var Thread[ServerCtx], port: var int, count: ptr int) =
  ## Serve exactly one 503 on an ephemeral connection (port written to `port`),
  ## counting the answered request into `count`, then exit.
  var ready = false
  createThread(th, serve503Once,
    ServerCtx(portOut: addr port, ready: addr ready, count: count))
  while not ready: sleep(1)

proc serveProxy(ctx: ServerCtx) {.thread.} =
  ## Minimal HTTP proxy: echoes back the absolute-URI request target so a test
  ## can confirm the client dialed the proxy and used absolute form.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  var head = ""
  while true:
    let c = client.recv(1)
    if c.len == 0: break
    head.add c
    if head.len >= 4 and head[^4 .. ^1] == "\r\n\r\n": break
  let target = head.split(' ')[1]  # request-target from the request line
  client.send("HTTP/1.1 200 OK\r\nContent-Length: " & $target.len &
              "\r\nConnection: close\r\n\r\n" & target)
  client.close()
  server.close()

proc startProxy*(th: var Thread[ServerCtx], port: int) =
  var ready = false
  createThread(th, serveProxy, ServerCtx(port: port, ready: addr ready))
  while not ready: sleep(1)

proc serveCookies(ctx: ServerCtx) {.thread.} =
  ## First request gets a Set-Cookie; the second echoes back whatever Cookie
  ## header it received in the response body. One kept-alive connection.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  for i in 0 .. 1:
    var head = ""
    while true:
      let c = client.recv(1)
      if c.len == 0: break
      head.add c
      if head.len >= 4 and head[^4 .. ^1] == "\r\n\r\n": break
    if head.len == 0: break
    if i == 0:
      client.send("HTTP/1.1 200 OK\r\nSet-Cookie: sid=abc123; Path=/\r\n" &
                  "Content-Length: 0\r\nConnection: keep-alive\r\n\r\n")
    else:
      let body = headerValue(head, "cookie")
      client.send("HTTP/1.1 200 OK\r\nContent-Length: " & $body.len &
                  "\r\nConnection: close\r\n\r\n" & body)
      break
  client.close()
  server.close()

proc startCookies*(th: var Thread[ServerCtx], port: int) =
  var ready = false
  createThread(th, serveCookies, ServerCtx(port: port, ready: addr ready))
  while not ready: sleep(1)

proc serveRetry(ctx: ServerCtx) {.thread.} =
  ## Answer `failures` requests with 503, then one with 200, on a single
  ## kept-alive connection.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  var i = 0
  while true:
    var req = ""
    while true:
      let c = client.recv(1)
      if c.len == 0: break
      req.add c
      if req.len >= 4 and req[^4 .. ^1] == "\r\n\r\n": break
    if req.len == 0: break
    if i < ctx.failures:
      client.send("HTTP/1.1 503 Service Unavailable\r\n" &
                  "Content-Length: 0\r\nConnection: keep-alive\r\n\r\n")
    else:
      let body = "recovered"
      client.send("HTTP/1.1 200 OK\r\nContent-Length: " & $body.len &
                  "\r\nConnection: close\r\n\r\n" & body)
      break
    inc i
  client.close()
  server.close()

proc startRetry*(th: var Thread[ServerCtx], port, failures: int) =
  var ready = false
  createThread(th, serveRetry,
    ServerCtx(port: port, ready: addr ready, failures: failures))
  while not ready: sleep(1)

proc serveRedirect(ctx: ServerCtx) {.thread.} =
  ## First request gets a 302 to /final (relative), the second gets 200.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  for i in 0 .. 1:
    var req = ""
    while true:
      let c = client.recv(1)
      if c.len == 0: break
      req.add c
      if req.len >= 4 and req[^4 .. ^1] == "\r\n\r\n": break
    if req.len == 0: break
    if i == 0:
      client.send("HTTP/1.1 302 Found\r\nLocation: /final\r\n" &
                  "Content-Length: 0\r\nConnection: keep-alive\r\n\r\n")
    else:
      let body = "arrived"
      client.send("HTTP/1.1 200 OK\r\nContent-Length: " & $body.len &
                  "\r\nConnection: close\r\n\r\n" & body)
  client.close()
  server.close()

proc startRedirect*(th: var Thread[ServerCtx], port: int) =
  var ready = false
  createThread(th, serveRedirect, ServerCtx(port: port, ready: addr ready))
  while not ready: sleep(1)

proc serveOnce(ctx: ServerCtx) {.thread.} =
  var server = newSocket(if ctx.ipv6: AF_INET6 else: AF_INET)
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), if ctx.ipv6: "::1" else: "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  var req = ""
  while true:
    let c = client.recv(1)
    if c.len == 0: break
    req.add c
    if req.len >= 4 and req[^4 .. ^1] == "\r\n\r\n": break
  let body = """{"ok":true}"""
  client.send("HTTP/1.1 200 OK\r\n" &
              "Content-Type: application/json\r\n" &
              "Content-Length: " & $body.len & "\r\n\r\n" & body)
  client.close()
  server.close()

proc startServer*(th: var Thread[ServerCtx], port: int, ipv6 = false) =
  ## Launch the one-shot server and block until it is listening.
  var ready = false
  createThread(th, serveOnce, ServerCtx(port: port, ready: addr ready, ipv6: ipv6))
  while not ready: sleep(1)

type KeepAliveCtx* = object
  portOut: ptr int      ## bind an ephemeral port and report it here
  requests: int
  ready: ptr bool
  accepts: ptr int

proc serveKeepAlive(ctx: KeepAliveCtx) {.thread.} =
  ## Accept exactly one connection and answer `requests` keep-alive responses
  ## on it. If the client reuses its pooled connection, every request lands
  ## here and `accepts` stays 1.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0), "127.0.0.1")     # ephemeral: no cross-iteration collision
  server.listen()
  ctx.portOut[] = server.getLocalAddr()[1].int
  ctx.ready[] = true
  var client = acceptClient(server)
  ctx.accepts[] = 1
  for i in 0 ..< ctx.requests:
    var req = ""
    while true:
      let c = client.recv(1)
      if c.len == 0: break
      req.add c
      if req.len >= 4 and req[^4 .. ^1] == "\r\n\r\n": break
    if req.len == 0: break
    let body = "n=" & $i
    client.send("HTTP/1.1 200 OK\r\n" &
                "Content-Length: " & $body.len & "\r\n" &
                "Connection: keep-alive\r\n\r\n" & body)
  client.close()
  server.close()

type KeepAliveStallCtx* = object
  portOut: ptr int      ## bind an ephemeral port and report it here
  ready: ptr bool
  stallMs: int          ## how long to hold the second request silent

proc serveKeepAliveStall(ctx: KeepAliveStallCtx) {.thread.} =
  ## Answer the first keep-alive request in full, then read the second request and
  ## go silent (send nothing) for `stallMs`, modeling a wedged server. Reuses the one
  ## pooled connection, so the second request's response read stalls and must trip the
  ## client's CURRENT read timeout (issue #360: config changed between the two).
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0), "127.0.0.1")
  server.listen()
  ctx.portOut[] = server.getLocalAddr()[1].int
  ctx.ready[] = true
  var client = acceptClient(server)
  proc readReq() =
    var req = ""
    while true:
      let c = client.recv(1)
      if c.len == 0: break
      req.add c
      if req.len >= 4 and req[^4 .. ^1] == "\r\n\r\n": break
  readReq()
  let body = "ok"
  client.send("HTTP/1.1 200 OK\r\n" &
              "Content-Length: " & $body.len & "\r\n" &
              "Connection: keep-alive\r\n\r\n" & body)
  readReq()               # second request lands on the reused connection
  sleep(ctx.stallMs)      # go silent: no response bytes, so the client read stalls
  try: client.close() except CatchableError: discard
  try: server.close() except CatchableError: discard

proc startKeepAliveStall*(th: var Thread[KeepAliveStallCtx], port: var int,
                          stallMs: int) =
  ## Launch the stall-on-second-request keep-alive server and block until it is
  ## listening. Binds an ephemeral port, reported via `port`.
  var ready = false
  createThread(th, serveKeepAliveStall,
    KeepAliveStallCtx(portOut: addr port, ready: addr ready, stallMs: stallMs))
  while not ready: sleep(1)

proc recvUntil(c: Socket, terminator: string): string =
  while not result.endsWith(terminator):
    let ch = c.recv(1)
    if ch.len == 0: break
    result.add ch

proc serveUploadEcho(ctx: ServerCtx) {.thread.} =
  ## Read a chunked request body and echo the decoded bytes back as the
  ## response body. Used to verify streaming uploads.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  discard client.recvUntil("\r\n\r\n") # request head
  var body = ""
  while true:
    let sizeLine = client.recvUntil("\r\n").strip()
    if sizeLine.len == 0: break
    let n = parseHexInt(sizeLine)
    if n == 0:
      discard client.recv(2) # final CRLF
      break
    var chunk = ""
    while chunk.len < n:
      let part = client.recv(n - chunk.len)
      if part.len == 0: break
      chunk.add part
    discard client.recv(2)  # CRLF after the chunk
    body.add chunk
  client.send("HTTP/1.1 200 OK\r\n" &
              "Content-Length: " & $body.len & "\r\n" &
              "Connection: close\r\n\r\n" & body)
  client.close()
  server.close()

proc startUploadEcho*(th: var Thread[ServerCtx], port: int) =
  var ready = false
  createThread(th, serveUploadEcho, ServerCtx(port: port, ready: addr ready))
  while not ready: sleep(1)

proc serveTruncated(ctx: ServerCtx) {.thread.} =
  ## Send response headers declaring `Content-Length: 100` but only `failures` body
  ## bytes, then close the connection mid-body (premature close). Used to prove the
  ## client raises rather than returning the partial body as a complete response.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  discard client.recvUntil("\r\n\r\n")
  client.send("HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n" & repeat('x', ctx.failures))
  client.close()
  server.close()

proc startTruncated*(th: var Thread[ServerCtx], port, bodyBytes: int) =
  ## Serve one connection: 200 with Content-Length 100 but only `bodyBytes` of body,
  ## then close.
  var ready = false
  createThread(th, serveTruncated,
    ServerCtx(port: port, ready: addr ready, failures: bodyBytes))
  while not ready: sleep(1)

proc serveChunkedTrailer(ctx: ServerCtx) {.thread.} =
  ## Send a valid 3-chunk body ("Hello, chunked world!") plus a trailing field
  ## `x-checksum: done`. For the response-sink tests: proves chunked parsing and that
  ## trailers survive a full drain.
  ##
  ## The whole response (status line, chunks, trailer) is written in ONE `send`.
  ## The response-sink tests make the client stop reading and close the connection
  ## early (a sink returning false or raising); if the server were still writing
  ## later chunks at that moment, its blocking `send` to the gone peer can wedge the
  ## server thread indefinitely (std/net's `send` retries on disconnect rather than
  ## failing fast), and `joinThread` then hangs the whole test -- a rare flake that a
  ## slow/instrumented build (ASan, arc) widened enough to fail CI. Sending once,
  ## before the client has read anything, closes that window: the bytes are buffered
  ## and the server moves on to `close` before the client can bail. The chunked
  ## framing (and thus multi-chunk parsing) is unchanged; only the wire is not split
  ## across writes, which TCP never guaranteed anyway.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  discard client.recvUntil("\r\n\r\n")
  var wire = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: x-checksum\r\n\r\n"
  for p in ["Hello, ", "chunked ", "world!"]:
    wire.add(toHex(p.len, 1) & "\r\n" & p & "\r\n")
  wire.add("0\r\nx-checksum: done\r\n\r\n")
  try: client.send(wire) except CatchableError, Defect: discard
  try: client.close() except CatchableError, Defect: discard
  try: server.close() except CatchableError, Defect: discard

proc startChunkedTrailer*(th: var Thread[ServerCtx], port: int) =
  ## Serve one connection: a 3-chunk body ("Hello, chunked world!") plus a trailer.
  var ready = false
  createThread(th, serveChunkedTrailer, ServerCtx(port: port, ready: addr ready))
  while not ready: sleep(1)

proc serveGzipBody(ctx: ServerCtx) {.thread.} =
  ## Send a gzip-encoded body ({"ok":true}) with Content-Encoding: gzip, so a sink
  ## test can prove the body arrives DECODED. The gzip bytes are a fixed fixture (the
  ## same encoding the decompress tests use).
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  discard client.recvUntil("\r\n\r\n")
  let gz = hexToBytes("1f8b0800000000000003ab56cacf56b22a292a4dad0500905fd4a70b000000")
  client.send("HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\n" &
              "Content-Length: " & $gz.len & "\r\nConnection: close\r\n\r\n" & gz)
  client.close()
  server.close()

proc startGzipBody*(th: var Thread[ServerCtx], port: int) =
  ## Serve one gzip-encoded body ({"ok":true}).
  var ready = false
  createThread(th, serveGzipBody, ServerCtx(port: port, ready: addr ready))
  while not ready: sleep(1)

proc serveStatusBody(ctx: ServerCtx) {.thread.} =
  ## Answer one request with status `failures` (reused as the status code) and
  ## `payload` as the body. For the throw-on-non-2xx sink test (a 404 with a body).
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  discard client.recvUntil("\r\n\r\n")
  client.send("HTTP/1.1 " & $ctx.failures & " Status\r\nContent-Length: " &
              $ctx.payload.len & "\r\nConnection: close\r\n\r\n" & ctx.payload)
  client.close()
  server.close()

proc startStatusBody*(th: var Thread[ServerCtx], port, status: int, body: string) =
  ## Serve one response with an arbitrary `status` and `body`.
  var ready = false
  createThread(th, serveStatusBody,
    ServerCtx(port: port, ready: addr ready, failures: status, payload: body))
  while not ready: sleep(1)

proc serveHeadNoBody(ctx: ServerCtx) {.thread.} =
  ## Answer a HEAD request with 200 + Content-Length but no body (correct HEAD), so a
  ## sink test can prove the sink is never called for a HEAD.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  discard client.recvUntil("\r\n\r\n")
  client.send("HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\n")
  client.close()
  server.close()

proc startHeadNoBody*(th: var Thread[ServerCtx], port: int) =
  var ready = false
  createThread(th, serveHeadNoBody, ServerCtx(port: port, ready: addr ready))
  while not ready: sleep(1)

proc serve204(ctx: ServerCtx) {.thread.} =
  ## Answer one request with 204 No Content (no body at all).
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  discard client.recvUntil("\r\n\r\n")
  client.send("HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
  client.close()
  server.close()

proc start204*(th: var Thread[ServerCtx], port: int) =
  var ready = false
  createThread(th, serve204, ServerCtx(port: port, ready: addr ready))
  while not ready: sleep(1)

proc serveDigestThenBody(ctx: ServerCtx) {.thread.} =
  ## Answer the first request 401 with a Digest challenge (and a body that must NOT
  ## reach the sink), then the retried (Authorization-carrying) request 200 with
  ## `payload` -- the protected final body that SHOULD stream to the sink. Two
  ## responses on one kept-alive connection.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  for i in 0 .. 1:
    var head = ""
    while true:
      let c = client.recv(1)
      if c.len == 0: break
      head.add c
      if head.len >= 4 and head[^4 .. ^1] == "\r\n\r\n": break
    if head.len == 0: break
    if i == 0:
      const chalBody = "challenge-body-should-not-reach-sink"
      client.send("HTTP/1.1 401 Unauthorized\r\n" &
        "WWW-Authenticate: Digest realm=\"navi\", nonce=\"abc123\", qop=\"auth\"\r\n" &
        "Content-Length: " & $chalBody.len & "\r\nConnection: keep-alive\r\n\r\n" & chalBody)
    else:
      client.send("HTTP/1.1 200 OK\r\nContent-Length: " & $ctx.payload.len &
                  "\r\nConnection: close\r\n\r\n" & ctx.payload)
      break
  client.close()
  server.close()

proc startDigestThenBody*(th: var Thread[ServerCtx], port: int, body: string) =
  ## Serve a 401 Digest challenge then a 200 with `body` on the retry.
  var ready = false
  createThread(th, serveDigestThenBody,
    ServerCtx(port: port, ready: addr ready, payload: body))
  while not ready: sleep(1)

proc serveTruncatedChunked(ctx: ServerCtx) {.thread.} =
  ## Send a chunked response but close after one chunk, without the terminating
  ## `0\r\n\r\n` -- a truncated chunked body.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client = acceptClient(server)
  discard client.recvUntil("\r\n\r\n")
  client.send("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n")
  client.close()
  server.close()

proc startTruncatedChunked*(th: var Thread[ServerCtx], port: int) =
  var ready = false
  createThread(th, serveTruncatedChunked, ServerCtx(port: port, ready: addr ready))
  while not ready: sleep(1)

type StaleCtx* = object
  portOut: ptr int
  ready: ptr bool
  closed1: ptr bool     ## set once the first (pooled) connection has been closed
  accepts: ptr int

proc serveStalePooled(ctx: StaleCtx) {.thread.} =
  ## Answer one keep-alive request, then close the connection so the client's pooled
  ## copy goes stale. Then accept the client's retry on a second connection and
  ## answer it, echoing the request body so the test can confirm the replay.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0), "127.0.0.1")     # ephemeral: no cross-run collision
  server.listen()
  ctx.portOut[] = server.getLocalAddr()[1].int
  ctx.ready[] = true
  # conn 1: a keep-alive response, then a silent close (no `Connection: close`, so
  # the client pools it as reusable).
  var c1 = acceptClient(server)
  discard c1.recvUntil("\r\n\r\n")
  c1.send("HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: keep-alive\r\n\r\nabc")
  c1.close()
  ctx.accepts[] = 1
  ctx.closed1[] = true
  # conn 2: the client's retry lands here. Read its head + Content-Length body and
  # echo the body back, so the test proves the (non-idempotent) request was replayed.
  var c2 = acceptClient(server)
  ctx.accepts[] = 2
  let head = c2.recvUntil("\r\n\r\n")
  let cl = headerValue(head, "content-length")
  let n = if cl.len > 0: parseInt(cl) else: 0
  var body = ""
  while body.len < n:
    let part = c2.recv(n - body.len)
    if part.len == 0: break
    body.add part
  let respBody = "replayed:" & body
  c2.send("HTTP/1.1 200 OK\r\nContent-Length: " & $respBody.len &
          "\r\nConnection: close\r\n\r\n" & respBody)
  c2.close()
  server.close()

proc startStalePooled*(th: var Thread[StaleCtx], port: var int, closed1: ptr bool,
                       accepts: ptr int) =
  ## Serve one keep-alive request then close it (stale pool), and answer the retry
  ## on a fresh connection. Binds an ephemeral port, reported via `port`.
  var ready = false
  createThread(th, serveStalePooled,
    StaleCtx(portOut: addr port, ready: addr ready, closed1: closed1, accepts: accepts))
  while not ready: sleep(1)

proc serveFreshDrop(ctx: StaleCtx) {.thread.} =
  ## Drop the FIRST (freshly-opened) connection before any response header -- the
  ## keep-alive race on a fresh connection, not a pooled one. The full request is read
  ## first (so the client's send completes and it fails on the READ, a clean pre-header
  ## close, not a write error), then the socket is closed with no reply. The client's
  ## retry lands on a second connection, answered here with the echoed body so the test
  ## can prove the (non-idempotent) request was replayed.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0), "127.0.0.1")
  server.listen()
  ctx.portOut[] = server.getLocalAddr()[1].int
  ctx.ready[] = true
  for accept in 1 .. 2:
    var c = acceptClient(server)
    let head = c.recvUntil("\r\n\r\n")
    let cl = headerValue(head, "content-length")
    let n = if cl.len > 0: parseInt(cl) else: 0
    var body = ""
    while body.len < n:
      let part = c.recv(n - body.len)
      if part.len == 0: break
      body.add part
    ctx.accepts[] = accept
    if accept == 1:
      c.close()                    # fresh connection dropped before any response header
      ctx.closed1[] = true
    else:
      let respBody = "replayed:" & body
      c.send("HTTP/1.1 200 OK\r\nContent-Length: " & $respBody.len &
             "\r\nConnection: close\r\n\r\n" & respBody)
      c.close()
  server.close()

proc startFreshDrop*(th: var Thread[StaleCtx], port: var int, closed1: ptr bool,
                     accepts: ptr int) =
  ## Drop the first (fresh) connection before responding, then answer the retry on a
  ## second connection. Binds an ephemeral port, reported via `port`.
  var ready = false
  createThread(th, serveFreshDrop,
    StaleCtx(portOut: addr port, ready: addr ready, closed1: closed1, accepts: accepts))
  while not ready: sleep(1)

proc serveDropOnce(ctx: StaleCtx) {.thread.} =
  ## Accept exactly ONE connection, read its full request, drop it before any response
  ## header, then EXIT without accepting a second connection. For asserting that a
  ## request is NOT replayed (at-most-once): if the client wrongly retried, the retry
  ## would fail to connect (the listener is gone), surfacing the misbehavior instead of
  ## hanging the test thread waiting for a second accept that a correct client never makes.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0), "127.0.0.1")
  server.listen()
  ctx.portOut[] = server.getLocalAddr()[1].int
  ctx.ready[] = true
  var c = acceptClient(server)
  let head = c.recvUntil("\r\n\r\n")
  let cl = headerValue(head, "content-length")
  let n = if cl.len > 0: parseInt(cl) else: 0
  var body = ""
  while body.len < n:
    let part = c.recv(n - body.len)
    if part.len == 0: break
    body.add part
  ctx.accepts[] = 1
  c.close()                              # dropped before any response header
  server.close()                         # and no second accept: a replay would be refused

proc startDropOnce*(th: var Thread[StaleCtx], port: var int, accepts: ptr int) =
  ## Serve (drop) exactly one connection, then stop listening. Binds an ephemeral port.
  var ready = false
  var closed1 = false
  createThread(th, serveDropOnce,
    StaleCtx(portOut: addr port, ready: addr ready, closed1: addr closed1, accepts: accepts))
  while not ready: sleep(1)

proc startKeepAlive*(th: var Thread[KeepAliveCtx], port: var int, requests: int,
                     accepts: ptr int) =
  ## Launch the keep-alive server and block until it is listening. Binds an
  ## ephemeral port (so looped runs never collide) and writes it to `port`.
  var ready = false
  createThread(th, serveKeepAlive,
    KeepAliveCtx(portOut: addr port, requests: requests, ready: addr ready,
                 accepts: accepts))
  while not ready: sleep(1)

# --- cache-aware server for the middleware tests ------------------------------
# Serves `requests` connections (Connection: close each). Replies 200 with
# Cache-Control (max-age or no-store) and optional ETag; when a conditional
# request carries a matching If-None-Match it replies 304. `count` tallies
# requests actually received, so a test can prove a cache hit skipped the network.

type CacheSrv* = object
  port*: int
  ready*: ptr bool
  count*: ptr int
  requests*, maxAge*: int
  etag*: string
  noStore*: bool

proc serveCache(ctx: CacheSrv) {.thread.} =
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  for _ in 0 ..< ctx.requests:
    var client: Socket
    server.accept(client)
    var reqData = ""
    while not reqData.endsWith("\r\n\r\n"):
      let c = client.recv(1)
      if c.len == 0: break
      reqData.add c
    inc ctx.count[]
    if ctx.etag.len > 0 and
       ("if-none-match: " & ctx.etag).toLowerAscii in reqData.toLowerAscii:
      client.send("HTTP/1.1 304 Not Modified\r\nConnection: close\r\n\r\n")
    else:
      const body = "payload"
      var h = "HTTP/1.1 200 OK\r\nContent-Length: " & $body.len &
              "\r\nConnection: close\r\n"
      if ctx.noStore: h.add "Cache-Control: no-store\r\n"
      else: h.add "Cache-Control: max-age=" & $ctx.maxAge & "\r\n"
      if ctx.etag.len > 0: h.add "ETag: " & ctx.etag & "\r\n"
      client.send(h & "\r\n" & body)
    client.close()
  server.close()

proc startCache*(th: var Thread[CacheSrv], c: var CacheSrv) =
  ## Launch the cache server (fills `c.ready`) and block until it is listening.
  var ready = false
  c.ready = addr ready
  createThread(th, serveCache, c)
  while not ready: sleep(5)

