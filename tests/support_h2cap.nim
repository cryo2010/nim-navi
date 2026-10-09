## A blocking-socket HTTP/2 peer that answers each stream on its own schedule, for
## the per-stream size-cap tests (#466). Run on its own thread so it stays off navi's
## event loop, like `support_h2peer` / `support_h2race`.
##
## It sends an initial SETTINGS, drops the client preface, and then answers each
## client stream according to `script` -- ONE LETTER PER CLIENT STREAM, in the order
## their HEADERS arrive (stream 1, 3, 5, ...), so a test picks the behaviour of each
## stream it opens without the peer having to decode HPACK:
##
##   'q'  QUIET: `:status: 200` plus one event immediately, then nothing at all for
##        `quietMs`, then a second event and END_STREAM. A stream that is alive but
##        silent -- what an SSE stream between events looks like on the wire, and
##        what an uncapped stream delivers its body in here.
##   'f'  FAST: the whole response immediately.
##
## Actions are queued with absolute due times and flushed from the same loop that
## polls the socket, so one thread can keep several streams on different schedules.
## A client RST_STREAM drops that stream's queued actions, so a request the client
## has given up on is not answered on a stream it has already closed.
import std/[net, monotimes, times]
from std/nativesockets import selectRead, SocketHandle
from navi/proto/h2/frame import encodeSettings, encodeHeaders, encodeData

type
  CapPeerArg* = object
    port*: int
    script*: string    ## one letter ('q'/'f') per client stream, in arrival order
    quietMs*: int      ## the silent gap in the middle of a 'q' stream
    body*: string      ## the body an 'f' stream answers with
    events*: seq[string]  ## the two chunks a 'q' stream delivers (before/after the gap)
  Action = object
    due: MonoTime
    sid: uint32
    bytes: string

const
  clientPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"   # 24 bytes, precedes the frames
  status200Block = "\x88"                             # HPACK indexed field, index 8
  peerLifeMs = 30_000      ## hard stop, so a wedged test cannot hang the suite thread

proc runCapPeer*(arg: CapPeerArg) {.thread.} =
  let listener = newSocket(buffered = false)
  listener.setSockOpt(OptReuseAddr, true)
  listener.bindAddr(Port(arg.port), "127.0.0.1")
  listener.listen()
  var client: Socket
  listener.accept(client)                        # inherits the unbuffered mode
  client.send(encodeSettings([]))                # a real h2 peer's mandatory first frame
  var
    rest: string                                 # unparsed bytes after the preface
    prefaceDropped = false
    queue: seq[Action]
    streams = 0                                  # client streams seen so far
    eof = false
  let hardStop = getMonoTime() + initDuration(milliseconds = peerLifeMs)
  while not eof and getMonoTime() < hardStop:
    # 1. flush every action whose moment has come.
    let now = getMonoTime()
    var still: seq[Action]
    for a in queue:
      if a.due <= now:
        try: client.send(a.bytes)
        except CatchableError: eof = true        # the client is gone
      else: still.add a
    queue = still
    # 2. poll the socket briefly, then read only when it is actually readable, so the
    #    loop keeps ticking (and flushing the queue) while the client is quiet. It is
    #    `selectRead` + a plain `recv`, NOT `recv(..., timeout = ...)`: on an
    #    unbuffered socket that overload selects on a fd that is never registered and
    #    times out even with bytes waiting, so the peer would never see a request.
    var ready = @[client.getFd()]
    var chunk = ""
    if selectRead(ready, 20) > 0:
      chunk = client.recv(4096)                  # readable: one non-blocking recv
      if chunk.len == 0: eof = true              # the client closed
    if chunk.len == 0: continue
    rest.add chunk
    if not prefaceDropped:
      if rest.len < clientPreface.len: continue
      rest = rest[clientPreface.len .. ^1]
      prefaceDropped = true
    while rest.len >= 9:                          # whole frames only
      let length = (rest[0].int shl 16) or (rest[1].int shl 8) or rest[2].int
      if rest.len < 9 + length: break
      let ftype = rest[3].int
      let sid = uint32(((rest[5].int and 0x7f) shl 24) or (rest[6].int shl 16) or
                       (rest[7].int shl 8) or rest[8].int)
      rest = rest[9 + length .. ^1]
      if ftype == 0x3:                            # RST_STREAM: the client gave up on it
        var kept: seq[Action]
        for a in queue:
          if a.sid != sid: kept.add a
        queue = kept
      elif ftype == 0x1 and sid != 0:             # HEADERS: a new request
        let role = if streams < arg.script.len: arg.script[streams] else: 'f'
        inc streams
        let head = encodeHeaders(sid, status200Block,
                                 endStream = false, endHeaders = true)
        case role
        of 'q':
          let ev = if arg.events.len >= 2: arg.events else: @["a", "b"]
          queue.add Action(due: now, sid: sid, bytes: head & encodeData(sid, ev[0], false))
          queue.add Action(due: now + initDuration(milliseconds = arg.quietMs), sid: sid,
                           bytes: encodeData(sid, ev[1], false) &
                                  encodeData(sid, "", true))
        else:
          queue.add Action(due: now, sid: sid,
                           bytes: head & encodeData(sid, arg.body, true))
  # Drain whatever the client sent before closing: closing with unread bytes in the
  # receive buffer sends an RST rather than a FIN, which on Windows discards frames
  # the client has not read yet (see support_h2race for the same precaution).
  try:
    var ready = @[client.getFd()]
    while selectRead(ready, 200) > 0 and client.recv(4096).len > 0: discard
  except CatchableError: discard
  client.close()
  listener.close()

proc startCapPeer*(t: var Thread[CapPeerArg], arg: CapPeerArg) =
  createThread(t, runCapPeer, arg)
