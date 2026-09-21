## Blocking-socket HTTP/1.1 peers for the keep-alive-race tests, each run on its own
## thread (off navi's event loop, like the servers in `support.nim`). Plain 127.0.0.1
## TCP, no TLS -- macOS cannot dlopen libcrypto in this test harness, so these must run
## unencrypted. Two peers:
##
##  * serveInterimThenClose -- accepts one connection, reads the request, sends a 1xx
##    interim response ("HTTP/1.1 103 Early Hints\r\n\r\n") and then closes WITHOUT any
##    final response. The peer demonstrably began responding, so a drop here must NOT be
##    the keep-alive race: navi surfaces a plain IOError (never auto-replayed), the h1
##    analog of the h2 1xx-then-drop classification.
##
##  * serveReusedDrop -- serves ONE keep-alive request (so the client pools the
##    connection), closes it, then answers the client's retry on a SECOND connection with
##    the echoed body. The reused connection fails BEFORE any response -- at write time
##    (the socket is already gone) or at read time -- and either way navi classifies it as
##    the keep-alive race, so an idempotent method (or one with an Idempotency-Key) is
##    replayed onto the fresh connection. Proves finding 6's write-time classification
##    restores the pre-response replay.
import std/net
import std/strutils
import std/os   # sleep

type
  H1RaceCtx* = object
    portOut*: ptr int
    ready*: ptr bool
    closed1*: ptr bool     ## set once the first (pooled) connection is closed
    accepts*: ptr int      ## how many connections were accepted (1 = never replayed)

proc acceptClient(server: Socket): Socket =
  new(result)
  server.accept(result)

proc recvHead(c: Socket): string =
  ## Read up to and including the blank line that ends the request head.
  while not result.contains("\r\n\r\n"):
    let part = c.recv(1)
    if part.len == 0: break
    result.add part

proc contentLength(head: string): int =
  for line in head.splitLines():
    let i = line.find(':')
    if i > 0 and cmpIgnoreCase(line[0 ..< i].strip(), "content-length") == 0:
      return parseInt(line[i + 1 .. ^1].strip())
  0

proc readBody(c: Socket, n: int): string =
  while result.len < n:
    let part = c.recv(n - result.len)
    if part.len == 0: break
    result.add part

proc serveInterimThenClose(ctx: H1RaceCtx) {.thread.} =
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0), "127.0.0.1")
  server.listen()
  ctx.portOut[] = server.getLocalAddr()[1].int
  ctx.ready[] = true
  var c = acceptClient(server)
  let head = recvHead(c)
  discard readBody(c, contentLength(head))
  ctx.accepts[] = 1
  c.send("HTTP/1.1 103 Early Hints\r\nLink: </style.css>; rel=preload\r\n\r\n")
  c.close()                              # dropped after the interim, before the final
  server.close()

proc startInterimThenClose*(th: var Thread[H1RaceCtx], port: var int,
                            accepts: ptr int) =
  ## Serve a single connection that sends a 103 interim then closes. Ephemeral port.
  var ready = false
  var closed1 = false
  createThread(th, serveInterimThenClose,
    H1RaceCtx(portOut: addr port, ready: addr ready, closed1: addr closed1,
              accepts: accepts))
  while not ready: sleep(1)

proc serveReusedDrop(ctx: H1RaceCtx) {.thread.} =
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0), "127.0.0.1")
  server.listen()
  ctx.portOut[] = server.getLocalAddr()[1].int
  ctx.ready[] = true
  # Connection 1: serve one keep-alive request so the client pools the connection.
  var c1 = acceptClient(server)
  let head1 = recvHead(c1)
  discard readBody(c1, contentLength(head1))
  c1.send("HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: keep-alive\r\n\r\nfirst")
  ctx.accepts[] = 1
  c1.close()                             # close it while the client thinks it is pooled
  ctx.closed1[] = true
  # Connection 2: the client's retry after the reused connection failed pre-response.
  var c2 = acceptClient(server)
  let head2 = recvHead(c2)
  let body = readBody(c2, contentLength(head2))
  ctx.accepts[] = 2
  let respBody = "replayed:" & body
  c2.send("HTTP/1.1 200 OK\r\nContent-Length: " & $respBody.len &
          "\r\nConnection: close\r\n\r\n" & respBody)
  c2.close()
  server.close()

proc startReusedDrop*(th: var Thread[H1RaceCtx], port: var int, closed1: ptr bool,
                      accepts: ptr int) =
  ## Serve one keep-alive request, close the pooled connection, then answer the retry on
  ## a second connection with the echoed body. Ephemeral port.
  var ready = false
  createThread(th, serveReusedDrop,
    H1RaceCtx(portOut: addr port, ready: addr ready, closed1: closed1, accepts: accepts))
  while not ready: sleep(1)

proc waitFlag*(flag: ptr bool) =
  while not flag[]: sleep(1)
