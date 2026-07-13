## Shared test helper: a one-shot in-process HTTP/1.1 server on a thread.
## Filename has no leading 't' so `nimble test` does not treat it as a suite.

import std/net

type ServerCtx* = object
  port: int
  ready: ptr bool

proc serveOnce(ctx: ServerCtx) {.thread.} =
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
  let body = """{"ok":true}"""
  client.send("HTTP/1.1 200 OK\r\n" &
              "Content-Type: application/json\r\n" &
              "Content-Length: " & $body.len & "\r\n\r\n" & body)
  client.close()
  server.close()

proc startServer*(th: var Thread[ServerCtx], port: int) =
  ## Launch the one-shot server and block until it is listening.
  var ready = false
  createThread(th, serveOnce, ServerCtx(port: port, ready: addr ready))
  while not ready: discard
