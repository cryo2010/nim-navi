## A blocking-socket HTTP/2 peer for the mux turn-budget tests (#461), run on its own
## thread so it stays off navi's event loop. One connection, two client streams:
##
##   * the first request (a GET) is answered at once with `downloadBytes` of body in
##     16 KiB DATA frames and END_STREAM, all inside the client's 8 MiB stream
##     window, so the whole body lands in the client's per-stream queue without
##     waiting for a single WINDOW_UPDATE;
##   * the second request (a POST) is an upload against the default 64 KiB windows
##     this peer advertises (an empty SETTINGS), acked like a real server: every DATA
##     frame is answered with a stream and a connection WINDOW_UPDATE for its length,
##     and END_STREAM gets a 200 with END_STREAM.
##
## So the upload can only move one window per round trip through the client's event
## loop, which is exactly what starved next to a busy download.
##
## Binds 127.0.0.1 explicitly (the CI sandbox rejects the wildcard) and unbuffered,
## and owns its whole listener, like support_h2peer.
import std/net
from navi/proto/h2/frame import encodeSettings, encodeSettingsAck, encodeHeaders,
  encodeData, encodeWindowUpdate

type FloodArg* = tuple[port: int, downloadBytes: int]

const
  clientPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
  status200 = "\x88"                  # HPACK: indexed static entry 8, ":status: 200"
  frameData = 0x0
  frameHeaders = 0x1
  frameSettings = 0x4
  flagEndStream = 0x1
  flagAck = 0x1

proc readU31(s: string, i: int): uint32 =
  (uint32(s[i].uint8 and 0x7f) shl 24) or (uint32(s[i+1].uint8) shl 16) or
    (uint32(s[i+2].uint8) shl 8) or uint32(s[i+3].uint8)

proc runFlood*(arg: FloodArg) {.thread.} =
  let listener = newSocket(buffered = false)
  listener.setSockOpt(OptReuseAddr, true)
  listener.bindAddr(Port(arg.port), "127.0.0.1")
  listener.listen()
  var client: Socket
  listener.accept(client)
  client.send(encodeSettings([]))     # defaults: 64 KiB windows for the upload
  var rest: string
  var prefaceDropped = false
  var downloadSid = 0'u32
  var chunk = newString(16384)
  for i in 0 ..< chunk.len: chunk[i] = char(i mod 251)
  block serve:
    while true:
      let data = client.recv(65536)
      if data.len == 0: break serve
      rest.add data
      if not prefaceDropped:
        if rest.len < clientPreface.len: continue
        rest = rest[clientPreface.len .. ^1]
        prefaceDropped = true
      while rest.len >= 9:
        let length = (rest[0].int shl 16) or (rest[1].int shl 8) or rest[2].int
        if rest.len < 9 + length: break
        let ftype = rest[3].int
        let flags = rest[4].int
        let sid = readU31(rest, 5)
        rest = rest[9 + length .. ^1]
        if ftype == frameSettings and (flags and flagAck) == 0:
          client.send(encodeSettingsAck())
        elif ftype == frameHeaders and downloadSid == 0:
          # The download: answer the whole body now, END_STREAM on the last frame.
          downloadSid = sid
          client.send(encodeHeaders(sid, status200, endStream = false, endHeaders = true))
          var left = arg.downloadBytes
          while left > 0:
            let n = min(left, chunk.len)
            left -= n
            client.send(encodeData(sid, chunk[0 ..< n], endStream = left == 0))
        elif ftype == frameData and sid != downloadSid:
          # Upload DATA: hand the window straight back, then answer at END_STREAM.
          if length > 0:
            client.send(encodeWindowUpdate(sid, uint32(length)) &
                        encodeWindowUpdate(0, uint32(length)))
          if (flags and flagEndStream) != 0:
            client.send(encodeHeaders(sid, status200, endStream = true, endHeaders = true))
  client.close()
  listener.close()

proc startFlood*(t: var Thread[FloodArg], port: int, downloadBytes: int) =
  createThread(t, runFlood, (port, downloadBytes))
