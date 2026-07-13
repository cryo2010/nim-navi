## Shared test helper: a one-shot in-process HTTP/1.1 server on a thread.
## Filename has no leading 't' so `nimble test` does not treat it as a suite.

import std/[net, strutils]

type ServerCtx* = object
  port: int
  ready: ptr bool
  ipv6: bool

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
