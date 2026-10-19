## A blocking-socket HTTP/2 peer for the mux turn-budget tests (#461), run on its own
## thread so it stays off navi's event loop. One connection, two client streams:
##
##   * the first request (a GET) is answered at once with `downloadBytes` of body in
##     16 KiB DATA frames and END_STREAM, all inside the client's 8 MiB stream
##     window, so the whole body lands in the client's per-stream queue without
##     waiting for a single WINDOW_UPDATE;
##   * the second request (a POST) is an upload against the default 64 KiB windows
##     this peer advertises (an empty SETTINGS). The peer acks none of it until the
##     test says so: once a whole first window has arrived it raises
##     `floodUploadBlocked` and waits for `floodRelease`, then grants one stream and
##     one connection WINDOW_UPDATE big enough for the rest of the upload, raises
##     `floodGranted`, and answers END_STREAM with a 200.
##
## The handshake lets the test put the grant in the client's socket buffer while
## its own thread is blocked, so nothing on the client has read it when the drain
## starts: the upload then moves past its first window exactly when the client's
## event loop gives the mux reader a turn, with no dependence on how fast this
## thread answers (under ASan it answered too late for the drain to see it).
##
## Binds 127.0.0.1 explicitly (the CI sandbox rejects the wildcard) and unbuffered,
## and owns its whole listener, like support_h2peer.
import std/[net, os, atomics, monotimes, times]
from navi/proto/h2/frame import encodeSettings, encodeSettingsAck, encodeHeaders,
  encodeData, encodeWindowUpdate

type FloodArg* = tuple[port: int, downloadBytes: int]

var
  floodUploadBlocked*: Atomic[bool]   ## a whole first upload window has arrived
  floodRelease*: Atomic[bool]         ## set by the test: send the grant now
  floodGranted*: Atomic[bool]         ## the grant has been written to the socket

const
  clientPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
  status200 = "\x88"                  # HPACK: indexed static entry 8, ":status: 200"
  frameData = 0x0
  frameHeaders = 0x1
  frameSettings = 0x4
  flagEndStream = 0x1
  flagAck = 0x1
  firstWindow = 65_535                # the default window this peer advertises
  uploadGrant = 4 * 1024 * 1024       # covers the rest of any upload the tests send
  handshakeMs = 10_000                # give up waiting for the test after this

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
  client.send(encodeSettings([]), flags = {})   # defaults: 64 KiB windows for the upload
  var rest: string
  var prefaceDropped = false
  var downloadSid = 0'u32
  var uploadSeen = 0
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
          client.send(encodeSettingsAck(), flags = {})
        elif ftype == frameHeaders and downloadSid == 0:
          # The download: answer the whole body now, END_STREAM on the last frame.
          downloadSid = sid
          client.send(encodeHeaders(sid, status200, endStream = false, endHeaders = true),
                      flags = {})
          var left = arg.downloadBytes
          while left > 0:
            let n = min(left, chunk.len)
            left -= n
            client.send(encodeData(sid, chunk[0 ..< n], endStream = left == 0), flags = {})
        elif ftype == frameData and sid != downloadSid:
          # Upload DATA: hold the first window until the test releases it, then
          # grant the rest in one go; answer at END_STREAM.
          let before = uploadSeen
          uploadSeen += length
          if before < firstWindow and uploadSeen >= firstWindow:
            floodUploadBlocked.store(true)
            let giveUp = getMonoTime() + initDuration(milliseconds = handshakeMs)
            while not floodRelease.load and getMonoTime() < giveUp:
              sleep(1)
            client.send(encodeWindowUpdate(sid, uint32(uploadGrant)) &
                        encodeWindowUpdate(0, uint32(uploadGrant)), flags = {})
            floodGranted.store(true)
          if (flags and flagEndStream) != 0:
            client.send(encodeHeaders(sid, status200, endStream = true, endHeaders = true),
                        flags = {})
  client.close()
  listener.close()

proc startFlood*(t: var Thread[FloodArg], port: int, downloadBytes: int) =
  floodUploadBlocked.store(false)
  floodRelease.store(false)
  floodGranted.store(false)
  createThread(t, runFlood, (port, downloadBytes))
