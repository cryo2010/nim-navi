## Shared test helper: a one-shot in-process HTTP/1.1 server on a thread.
## Filename has no leading 't' so `nimble test` does not treat it as a suite.

import std/[net, strutils, os]
import navi/proto/ws   # sans-io WS core for the shared WebSocket test servers

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
  while not ready: discard

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
  while not ready: discard

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

proc startHang*(th: var Thread[ServerCtx], port: var int) =
  ## Serve a single connection that accepts but never responds. Binds an
  ## ephemeral port (so a leaked thread never collides on a re-run) and writes it
  ## to `port`, which must be a mutable `var`.
  var ready = false
  createThread(th, serveHang, ServerCtx(portOut: addr port, ready: addr ready))
  while not ready: discard

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
  while not ready: discard

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
  while not ready: discard

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
  while not ready: discard

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
  while not ready: discard

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
  while not ready: discard

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
  while not ready: discard

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
  while not ready: discard

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
  while not ready: discard

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
  while not ready: discard

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
  while not ready: discard

proc startKeepAlive*(th: var Thread[KeepAliveCtx], port: var int, requests: int,
                     accepts: ptr int) =
  ## Launch the keep-alive server and block until it is listening. Binds an
  ## ephemeral port (so looped runs never collide) and writes it to `port`.
  var ready = false
  createThread(th, serveKeepAlive,
    KeepAliveCtx(portOut: addr port, requests: requests, ready: addr ready,
                 accepts: accepts))
  while not ready: discard

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

# --- WebSocket test servers (shared by test_ws / test_ws_async / test_ws_chronos) --
# One in-process server per behavior, built from navi's sans-io WS core (server
# frames unmasked). Each binds an ephemeral loopback port and reports it via a
# `var int`, so concurrent test files never collide on a fixed port. Living here
# (not copy-pasted per backend file) means a fix reaches every backend at once.
#
# WS servers use `buffered = false` sockets and `server.accept` (not `acceptClient`,
# whose Windows path returns a buffered socket): interactive small frames must not
# wait for a full buffer, and IPv4 loopback avoids the IPv6-accept bug acceptClient
# works around.

type WsSrv* = object
  ready: ptr bool
  portOut: ptr int

proc wsBind(ctx: WsSrv): Socket =
  ## Ephemeral loopback listener; report the bound port, then mark ready.
  result = newSocket(buffered = false)
  result.setSockOpt(OptReuseAddr, true)
  result.bindAddr(Port(0), "127.0.0.1")
  result.listen()
  ctx.portOut[] = result.getLocalAddr()[1].int
  ctx.ready[] = true

proc wsReadHead(c: Socket): string =
  ## Read the client's HTTP upgrade request up to the blank line; "" if the peer
  ## disconnected mid-request (EOF or a recv error on an abrupt close).
  try:
    while "\r\n\r\n" notin result:
      let b = c.recv(1)
      if b.len == 0: return ""
      result.add b
  except CatchableError: return ""

proc wsHandshake(c: Socket): bool =
  ## Read the upgrade request and answer with the RFC 6455 101 accept. False if the
  ## client vanished before completing the request.
  let head = wsReadHead(c)
  if head.len == 0: return false
  var key = ""
  for line in head.splitLines:
    let i = line.find(':')
    if i > 0 and cmpIgnoreCase(line[0 ..< i].strip, "sec-websocket-key") == 0:
      key = line[i + 1 .. ^1].strip
  c.send("HTTP/1.1 101 Switching Protocols\r\n" &
         "Upgrade: websocket\r\nConnection: Upgrade\r\n" &
         "Sec-WebSocket-Accept: " & acceptFor(key) & "\r\n\r\n")
  true

template wsAcceptOne(ctx: WsSrv, server, c: untyped; body: untyped) =
  ## Bind, accept one connection, run `body`, then tear both down.
  let server = wsBind(ctx)
  var c: Socket
  server.accept(c)
  body
  c.close(); server.close()

proc serveWsEcho(ctx: WsSrv) {.thread.} =
  ## Echo text/binary, pong pings, reply to "please fragment" with two fragments,
  ## and send a server-initiated close on "bye".
  wsAcceptOne(ctx, server, c):
    if wsHandshake(c):
      var dec: WsDecoder
      var running = true
      while running:
        var f: Frame
        while not dec.next(f):
          let chunk = c.recv(4096)
          if chunk.len == 0: running = false; break
          dec.feed(chunk)
        if not running: break
        case f.opcode
        of opText:
          if f.payload == "please fragment":
            c.send(encodeFrame(opText, "frag", masked = false, fin = false))
            c.send(encodeFrame(opContinuation, "-ment", masked = false, fin = true))
          elif f.payload == "bye":
            c.send(encodeFrame(opClose, closePayload(closeNormal), masked = false))
            running = false
          else:
            c.send(encodeFrame(opText, f.payload, masked = false))
        of opBinary: c.send(encodeFrame(opBinary, f.payload, masked = false))
        of opPing: c.send(encodeFrame(opPong, f.payload, masked = false))
        of opClose: running = false
        else: discard

proc serveWsSilent(ctx: WsSrv) {.thread.} =
  ## Handshake, then never respond (ignore pings), reading and discarding until the
  ## client gives up -- a client with keepalive must time out and drop us.
  wsAcceptOne(ctx, server, c):
    if wsHandshake(c):
      try:                                   # an abrupt client close can raise here
        while c.recv(4096).len > 0: discard  # rather than returning EOF; don't die
      except CatchableError: discard         # in-thread (trips AddressSanitizer's join)

proc serveWsStall(ctx: WsSrv) {.thread.} =
  ## Accept and read the upgrade request but never send the 101, so the client's
  ## open blocks until its timeout fires; then hold briefly and tear down.
  wsAcceptOne(ctx, server, c):
    discard wsReadHead(c)
    sleep(2000)     # hold past the client's connect timeout

proc serveWsPingCounter(ctx: WsSrv) {.thread.} =
  ## Pong every ping, and after the second ping send a text message -- so a client
  ## with keepalive stays alive across pings and finally receives it.
  wsAcceptOne(ctx, server, c):
    if wsHandshake(c):
      var dec: WsDecoder
      var pings = 0
      var running = true
      while running:
        var f: Frame
        while not dec.next(f):
          let chunk = c.recv(4096)
          if chunk.len == 0: running = false; break
          dec.feed(chunk)
        if not running: break
        case f.opcode
        of opPing:
          c.send(encodeFrame(opPong, f.payload, masked = false))
          inc pings
          if pings == 2: c.send(encodeFrame(opText, "alive", masked = false))
        of opClose: running = false
        else: discard

proc serveWsStreamEcho(ctx: WsSrv) {.thread.} =
  ## Reassemble messages and, on the text trigger "fragment", reply with a
  ## 3-fragment message; otherwise echo the whole message as one frame.
  wsAcceptOne(ctx, server, c):
    if wsHandshake(c):
      var dec: WsDecoder
      var asmb: WsAssembler
      var running = true
      while running:
        var f: Frame
        while not dec.next(f):
          let chunk = c.recv(4096)
          if chunk.len == 0: running = false; break
          dec.feed(chunk)
        if not running: break
        let o = asmb.offer(f)
        case o.reply
        of wrPong: c.send(encodeFrame(opPong, o.replyPayload, masked = false))
        of wrCloseEcho: running = false
        of wrNone: discard
        if o.ready:
          case o.message.kind
          of wmText:
            if o.message.data == "fragment":
              c.send(encodeFrame(opText, "one", masked = false, fin = false))
              c.send(encodeFrame(opContinuation, "-two", masked = false, fin = false))
              c.send(encodeFrame(opContinuation, "-three", masked = false, fin = true))
            else:
              c.send(encodeFrame(opText, o.message.data, masked = false))
          of wmBinary: c.send(encodeFrame(opBinary, o.message.data, masked = false))
          of wmClose: running = false

proc startWs(th: var Thread[WsSrv], run: proc(ctx: WsSrv) {.thread.}, port: var int) =
  ## Launch a WS server on an ephemeral port, write it to `port` (a mutable `var`),
  ## and block until it is listening.
  var ready = false
  createThread(th, run, WsSrv(ready: addr ready, portOut: addr port))
  while not ready: sleep(1)

proc startWsEcho*(th: var Thread[WsSrv], port: var int) = startWs(th, serveWsEcho, port)
proc startWsSilent*(th: var Thread[WsSrv], port: var int) = startWs(th, serveWsSilent, port)
proc startWsStall*(th: var Thread[WsSrv], port: var int) = startWs(th, serveWsStall, port)
proc startWsPingCounter*(th: var Thread[WsSrv], port: var int) = startWs(th, serveWsPingCounter, port)
proc startWsStreamEcho*(th: var Thread[WsSrv], port: var int) = startWs(th, serveWsStreamEcho, port)
