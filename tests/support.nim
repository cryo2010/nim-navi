## Shared test helper: a one-shot in-process HTTP/1.1 server on a thread.
## Filename has no leading 't' so `nimble test` does not treat it as a suite.

import std/[net, strutils]

type ServerCtx* = object
  port: int
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
  var client: Socket
  server.accept(client)
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
  var client: Socket
  server.accept(client)
  var head = ""
  while true:
    let c = client.recv(1)
    if c.len == 0: break
    head.add c
    if head.len >= 4 and head[^4 .. ^1] == "\r\n\r\n": break
  let n = parseInt(headerValue(head, "content-length"))
  var body = ""
  while body.len < n:
    let part = client.recv(n - body.len)
    if part.len == 0: break
    body.add part
  client.send("HTTP/1.1 200 OK\r\n" &
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

proc serveCookies(ctx: ServerCtx) {.thread.} =
  ## First request gets a Set-Cookie; the second echoes back whatever Cookie
  ## header it received in the response body. One kept-alive connection.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client: Socket
  server.accept(client)
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
  var client: Socket
  server.accept(client)
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
  var client: Socket
  server.accept(client)
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
  var client: Socket
  server.accept(client)
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
  port: int
  requests: int
  ready: ptr bool
  accepts: ptr int

proc serveKeepAlive(ctx: KeepAliveCtx) {.thread.} =
  ## Accept exactly one connection and answer `requests` keep-alive responses
  ## on it. If the client reuses its pooled connection, every request lands
  ## here and `accepts` stays 1.
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(ctx.port), "127.0.0.1")
  server.listen()
  ctx.ready[] = true
  var client: Socket
  server.accept(client)
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
  var client: Socket
  server.accept(client)
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

proc startKeepAlive*(th: var Thread[KeepAliveCtx], port, requests: int,
                     accepts: ptr int) =
  ## Launch the keep-alive server and block until it is listening.
  var ready = false
  createThread(th, serveKeepAlive,
    KeepAliveCtx(port: port, requests: requests, ready: addr ready, accepts: accepts))
  while not ready: discard
