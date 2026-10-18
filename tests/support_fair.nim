## Test helper for the event-loop fairness specs (#462): a keep-alive HTTP/1.1
## server on a thread that answers `/big` with a large Content-Length body written
## as fast as the socket takes it, drains `POST /sink` until the client closes,
## echoes `POST /mirror` back byte for byte until the client closes, and answers
## anything else with a 2-byte `ok`. One handler thread per accepted
## connection, so a download in flight never blocks the small requests'
## connections. Filename has no leading 't' so the runner skips it.

import std/[net, nativesockets, strutils]
import ./support       # acceptClient (Windows-safe accept), waitFlag

const
  fairBigBytes* = 32 * 1024 * 1024
  fairMaxConns = 64

type
  FairServer* = object
    port*: int
    ready: bool
    stop: bool
    accepted: int
    listener: Thread[ptr FairServer]
    handlers: array[fairMaxConns, Thread[SocketHandle]]

proc readHead(c: Socket): string =
  ## The request head up to the blank line, or "" when the peer closed.
  while not result.endsWith("\r\n\r\n"):
    var ch = ""
    if c.recv(ch, 1) <= 0: return ""
    result.add ch

proc serveConn(fd: SocketHandle) {.thread.} =
  let c = newSocket(fd, AF_INET, SOCK_STREAM, IPPROTO_TCP)
  let blk = repeat('x', 256 * 1024)
  try:
    while true:
      let head = c.readHead()
      if head.len == 0: break
      if head.startsWith("GET /big "):
        c.send("HTTP/1.1 200 OK\r\nContent-Length: " & $fairBigBytes & "\r\n\r\n",
               flags = {})
        var left = fairBigBytes
        while left > 0:
          let n = min(blk.len, left)
          # `flags = {}`: the default SafeDisconn swallows a peer reset without
          # advancing, which spins this loop on Windows.
          c.send(if n == blk.len: blk else: blk[0 ..< n], flags = {})
          left -= n
      elif head.startsWith("POST /sink "):
        # A raw upload sink for the transport-level test: read and drop everything
        # as fast as it comes, so the client's writes keep completing in place.
        var buf = newString(256 * 1024)
        while c.recv(addr buf[0], buf.len) > 0: discard
        break
      elif head.startsWith("POST /mirror "):
        # Echo the raw bytes back in arrival order (the write-order test). One byte
        # per call: this socket is buffered, and a buffered recv blocks until it has
        # filled the whole request.
        var ch = ""
        while c.recv(ch, 1) > 0:
          c.send(ch, flags = {})
        break
      else:
        c.send("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok", flags = {})
  except CatchableError: discard
  c.close()

proc listen(s: ptr FairServer) {.thread.} =
  let server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0), "127.0.0.1")
  server.listen()
  s.port = server.getLocalAddr()[1].int
  s.ready = true
  while true:
    let c = server.acceptClient()
    if s.stop or s.accepted >= fairMaxConns:
      c.close(); break
    let fd = c.getFd()
    createThread(s.handlers[s.accepted], serveConn, fd)
    inc s.accepted
  server.close()

proc startFair*(s: var FairServer) =
  createThread(s.listener, listen, addr s)
  waitFlag(addr s.ready)

proc stopFair*(s: var FairServer) =
  ## Wake the accept loop with one last connection, then join every handler (each
  ## exits once the client side of its connection is closed).
  s.stop = true
  let poke = newSocket()
  try: poke.connect("127.0.0.1", Port(s.port))
  except CatchableError: discard
  poke.close()
  joinThread(s.listener)
  for i in 0 ..< s.accepted: joinThread(s.handlers[i])
