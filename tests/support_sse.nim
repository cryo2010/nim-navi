## Shared SSE test server: a loopback `text/event-stream` endpoint that flaps.
## Filename has no leading 't' so the unit runner does not treat it as a suite.
##
## Every connection is answered with a valid `200 text/event-stream` head plus
## `preamble`, and is then closed by the server. The first `emptyConns` of them
## carry no event at all (the shape that used to spin navi's reconnect loop: a
## clean 200 that closes with nothing in it); the ones after that also carry
## `event`, so a stream can be driven through a run of empty reconnects and then
## be handed a real event.

import std/[net, os]
import ./support       # acceptClient (Windows-safe accept)

type SseFlapSrv* = object
  portOut*: ptr int      ## ephemeral port, written before `ready`
  ready*: ptr bool       ## set once the socket is listening
  conns*: ptr int        ## connections served so far
  emptyConns*: int       ## leading connections that close with zero events
  total*: int            ## stop accepting (and exit the thread) after this many
  preamble*: string      ## sent on every connection (e.g. "retry: 0\n\n")
  event*: string         ## sent once the empty phase is over

proc serveSseFlap(ctx: SseFlapSrv) {.thread.} =
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0), "127.0.0.1")   # ephemeral: no cross-run collision
  server.listen()
  ctx.portOut[] = server.getLocalAddr()[1].int
  ctx.ready[] = true
  var served = 0
  while served < ctx.total:
    var client = acceptClient(server)
    var req = ""
    while true:                            # drain the request head
      let c = client.recv(1)
      if c.len == 0: break
      req.add c
      if req.len >= 4 and req[^4 .. ^1] == "\r\n\r\n": break
    inc served
    if ctx.conns != nil: ctx.conns[] = served
    var body = ctx.preamble
    if served > ctx.emptyConns: body.add ctx.event
    try:
      # `flags = {}`: std/net's default SafeDisconn swallows a peer reset without
      # advancing the write offset, which spins the send loop on Windows.
      client.send("HTTP/1.1 200 OK\r\n" &
                  "Content-Type: text/event-stream\r\n" &
                  "Connection: close\r\n\r\n" & body, flags = {})
    except CatchableError:
      discard                              # the client went away first: fine
    try: client.close() except CatchableError: discard
  try: server.close() except CatchableError: discard

proc startSseFlap*(th: var Thread[SseFlapSrv], port: var int, c: var SseFlapSrv) =
  ## Launch the flapping SSE server on an ephemeral port (written to `port`) and
  ## block until it is listening.
  var ready = false
  c.portOut = addr port
  c.ready = addr ready
  createThread(th, serveSseFlap, c)
  while not ready: os.sleep(1)

proc drainSseFlap*(port: int, want: int, served: ptr int) =
  ## Unblock the server's parked `accept` so the thread can be joined even when the
  ## client made fewer connections than the test expected (an assertion failed
  ## early). Dials the port until every expected connection has been served.
  var guard = 0
  while served[] < want and guard < 200:
    inc guard
    try:
      let c = newSocket()
      c.connect("127.0.0.1", Port(port))
      c.close()
    except CatchableError:
      discard
    os.sleep(5)
