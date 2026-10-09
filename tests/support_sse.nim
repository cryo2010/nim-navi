## Shared SSE test server: a loopback `text/event-stream` endpoint that flaps.
## Filename has no leading 't' so the unit runner does not treat it as a suite.
##
## Every connection is answered with a valid `200 text/event-stream` head plus
## `preamble`, and is then closed by the server. The first `emptyConns` of them
## carry no event at all (the shape that used to spin navi's reconnect loop: a
## clean 200 that closes with nothing in it); the ones after that also carry
## `event`, so a stream can be driven through a run of empty reconnects and then
## be handed a real event.
##
## `SseKeepAliveSrv` (below) is the opposite shape: it keeps the connection open
## between responses, so a test can count how many connections a client actually
## needed (issue #466).

import std/[net, os, strutils]
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

type SseKeepAliveSrv* = object
  ## A keep-alive SSE origin: every response carries a Content-Length and
  ## `Connection: keep-alive`, so a client that reuses connections answers every
  ## request of a run on ONE socket and `accepts` stays 1. That is what makes the
  ## #466 reuse claim measurable: `sse()` runs on the caller's client, so two
  ## consecutive streams (and a plain request after them) land on one connection
  ## instead of paying a connect each.
  ##
  ## `/events` answers `200 text/event-stream` with `events` (plus `events2` after
  ## `splitMs` milliseconds, to leave a stream legitimately mid-body), a
  ## `Set-Cookie` the caller's jar must pick up, and `altSvc` when set. Any other
  ## path answers `200 text/plain` with `cookie=<what the request sent>`, so a test
  ## can see the jar is shared.
  portOut*: ptr int      ## ephemeral port, written before `ready`
  ready*: ptr bool       ## set once the socket is listening
  accepts*: ptr int      ## connections accepted so far
  requests*: ptr int     ## requests answered so far, across every connection
  maxConns*: int         ## stop accepting (and exit the thread) after this many
  events*: string        ## the /events body (or its first half when splitMs > 0)
  events2*: string       ## the rest of the /events body, sent after splitMs
  splitMs*: int          ## delay between the two halves; 0 sends them as one write
  altSvc*: string        ## when set, an Alt-Svc header on the /events response

proc serveSseKeepAlive(ctx: SseKeepAliveSrv) {.thread.} =
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0), "127.0.0.1")   # ephemeral: no cross-run collision
  server.listen()
  ctx.portOut[] = server.getLocalAddr()[1].int
  ctx.ready[] = true
  var accepted = 0
  var served = 0
  while accepted < ctx.maxConns:
    var client = acceptClient(server)
    inc accepted
    if ctx.accepts != nil: ctx.accepts[] = accepted
    while true:
      var req = ""
      while true:                          # drain one request head
        let c = client.recv(1)
        if c.len == 0: break
        req.add c
        if req.len >= 4 and req[^4 .. ^1] == "\r\n\r\n": break
      if req.len == 0: break               # the peer closed: take the next connection
      let line = req.splitLines()[0]
      let sentCookie = req.contains("sid=sse")
      var head = "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\n"
      var body = ""
      var tail = ""
      if line.contains("/events"):
        body = ctx.events
        tail = ctx.events2
        head.add "Content-Type: text/event-stream\r\n"
        head.add "Set-Cookie: sid=sse; Path=/\r\n"
        if ctx.altSvc.len > 0: head.add "Alt-Svc: " & ctx.altSvc & "\r\n"
      else:
        body = "cookie=" & (if sentCookie: "sid=sse" else: "none")
        head.add "Content-Type: text/plain\r\n"
      head.add "Content-Length: " & $(body.len + tail.len) & "\r\n\r\n"
      try:
        # `flags = {}`: std/net's default SafeDisconn swallows a peer reset without
        # advancing the write offset, which spins the send loop on Windows.
        client.send(head & body, flags = {})
      except CatchableError:
        break                              # the client went away first: fine
      # Count the request once its head and body are out, BEFORE the split tail: a
      # mid-body close by the client (what the "close only that stream's connection"
      # test does) makes the tail send fail on Windows, and counting after it made
      # `requests` depend on whether the peer reset landed before or after that send.
      inc served
      if ctx.requests != nil: ctx.requests[] = served
      if tail.len > 0:
        try:
          os.sleep(ctx.splitMs)            # the stream is legitimately mid-body here
          client.send(tail, flags = {})
        except CatchableError:
          break                            # the client closed mid-body: fine
    try: client.close() except CatchableError: discard
  try: server.close() except CatchableError: discard

proc startSseKeepAlive*(th: var Thread[SseKeepAliveSrv], port: var int,
                        c: var SseKeepAliveSrv) =
  ## Launch the keep-alive SSE origin on an ephemeral port (written to `port`) and
  ## block until it is listening.
  var ready = false
  c.portOut = addr port
  c.ready = addr ready
  createThread(th, serveSseKeepAlive, c)
  while not ready: os.sleep(1)

proc drainSseKeepAlive*(port: int, want: int, accepts: ptr int) =
  ## Unblock the server's parked `accept` so its thread can be joined when the client
  ## opened fewer connections than `maxConns` -- which, for these tests, is the point.
  ## Call it only after the client has been closed, or the dials just queue in the
  ## listen backlog behind the connection the server is still reading.
  var guard = 0
  while accepts[] < want and guard < 200:
    inc guard
    try:
      let c = newSocket()
      c.connect("127.0.0.1", Port(port))
      c.close()
    except CatchableError:
      discard
    os.sleep(5)
