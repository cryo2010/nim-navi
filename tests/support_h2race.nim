## A blocking-socket HTTP/2 peer for the keep-alive-race tests, run on its own
## thread (off navi's event loop, like `support_h2peer`). It sends an initial
## SETTINGS, reads the client preface + frames until the request's HEADERS arrive,
## then either:
##
##  * pmCloseBeforeHeaders -- closes the connection WITHOUT any response. The client
##    stream never receives HEADERS, so a request dropped this way on a REUSED
##    connection is the classic keep-alive race (navi should surface it as
##    KeepAliveRaceError, retryable on a fresh connection).
##
##  * pmCloseAfterHeaders -- sends a response HEADERS frame (`:status: 200`, HPACK
##    static-table index 8 -> the single byte 0x88), NO END_STREAM, then closes. The
##    client saw the response begin, so the same drop must NOT be a race (a plain
##    IOError: the peer processed it, a non-idempotent request must not be replayed).
##
##  * pmResetThenClose -- sends RST_STREAM(CANCEL) then closes. A terminal reset the peer
##    may have processed: NOT a race (a plain reset IOError). Exercises `connDeathError`'s
##    reset consultation when the close races the reader's dispatch of the RST.
##
##  * pmRefusedThenClose -- sends RST_STREAM(REFUSED_STREAM) then closes. The peer proved
##    it did not process the request, so it is provably unprocessed (UnprocessedError,
##    retryable for any method). Exercises `connDeathError`'s unprocessed consultation.
##
## The peer owns its whole listener (create/bind/listen/accept/close), binds to
## 127.0.0.1 explicitly and unbuffered -- same rationale as support_h2peer.
import std/net
from navi/proto/h2/frame import encodeSettings, encodeHeaders, encodeRstStream,
  errCancel, errRefusedStream

type
  PeerMode* = enum
    pmCloseBeforeHeaders
    pmCloseAfterHeaders
    pmInterimThenClose      ## send a 1xx interim response, then close before the final
    pmResetThenClose        ## send RST_STREAM(CANCEL), then close: a terminal reset
    pmRefusedThenClose      ## send RST_STREAM(REFUSED_STREAM), then close: unprocessed
  RacePeerArg* = tuple[port: int, mode: PeerMode]

const clientPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"   # 24 bytes, precedes the frames
const status200Block = "\x88"                             # HPACK indexed field, index 8
const status103Block = "\x08\x03103"                      # HPACK literal :status: 103
                                                          # (0x08 = literal, name idx 8;
                                                          #  0x03 = 3-byte value "103")

proc runRacePeer*(arg: RacePeerArg) {.thread.} =
  let listener = newSocket(buffered = false)
  listener.setSockOpt(OptReuseAddr, true)
  listener.bindAddr(Port(arg.port), "127.0.0.1")
  listener.listen()
  var client: Socket
  listener.accept(client)
  client.send(encodeSettings([]))                # empty SETTINGS: a real h2 peer
  var rest: string
  var prefaceDropped = false
  var reqStream = 0'u32                           # the first client-opened stream id
  block read:
    while true:
      let data = client.recv(4096)
      if data.len == 0: break read                # client closed first: nothing to do
      rest.add data
      if not prefaceDropped:
        if rest.len < clientPreface.len: continue
        rest = rest[clientPreface.len .. ^1]
        prefaceDropped = true
      while rest.len >= 9:                         # parse whole frames off the buffer
        let length = (rest[0].int shl 16) or (rest[1].int shl 8) or rest[2].int
        if rest.len < 9 + length: break
        let ftype = rest[3].int
        let sid = ((rest[5].int and 0x7f) shl 24) or (rest[6].int shl 16) or
                  (rest[7].int shl 8) or rest[8].int
        rest = rest[9 + length .. ^1]
        if ftype == 0x1:                           # a HEADERS frame: the request head
          reqStream = uint32(sid)
          break read                               # request seen: act on the mode
  if reqStream != 0:
    if arg.mode == pmCloseAfterHeaders:
      client.send(encodeHeaders(reqStream, status200Block,
                                endStream = false, endHeaders = true))
    elif arg.mode == pmInterimThenClose:
      client.send(encodeHeaders(reqStream, status103Block,   # 1xx: response began
                                endStream = false, endHeaders = true))
    elif arg.mode == pmResetThenClose:
      client.send(encodeRstStream(reqStream, errCancel))     # terminal reset (not REFUSED)
    elif arg.mode == pmRefusedThenClose:
      client.send(encodeRstStream(reqStream, errRefusedStream))  # provably unprocessed
  client.close()
  listener.close()

proc startRacePeer*(t: var Thread[RacePeerArg], port: int, mode: PeerMode) =
  createThread(t, runRacePeer, (port, mode))
