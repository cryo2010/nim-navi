## A blocking-socket HTTP/2 peer for the never-written-stream reset tests (#462
## review), run on its own thread so it stays off navi's event loop. One connection.
## It answers every request whose HEADERS carry END_STREAM with a bodiless 200 and
## leaves one with a body to come unanswered, so the client can cancel it with the
## stream live on both ends. A request body's DATA is counted (`dataSeen`) and its
## END_STREAM answered with a bodiless 200; the peer never sends WINDOW_UPDATE, so
## the client can upload exactly the 64 KiB default connection window, no more.
##
## Like a real server it enforces RFC 9113 6.4 and 5.1.1: a RST_STREAM on an idle
## stream (one whose HEADERS it never received) and a new stream id that does not
## rise are connection errors, so it records them, sends GOAWAY(PROTOCOL_ERROR) and
## closes. Every sibling stream on the client's mux dies
## with it, which is what the test guards against.
import std/[net, atomics, sets]
from navi/proto/h2/frame import encodeSettings, encodeSettingsAck, encodeHeaders,
  encodeGoAway

type RstArg* = tuple[port: int]

var rstSeen*: Atomic[int]        ## RST_STREAMs for streams whose HEADERS arrived
var idleRstSeen*: Atomic[int]    ## RST_STREAMs for streams the peer never saw
var headersSeen*: Atomic[int]    ## request HEADERS received
var orderErrors*: Atomic[int]    ## HEADERS whose stream id did not rise (5.1.1)
var dataSeen*: Atomic[int]       ## request-body DATA payload bytes received

const
  clientPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
  status200 = "\x88"              # HPACK: indexed static entry 8, ":status: 200"
  frameData = 0x0
  frameHeaders = 0x1
  frameRst = 0x3
  frameSettings = 0x4
  flagEndStream = 0x1
  flagAck = 0x1
  errProtocol = 0x1'u32

proc readU31(s: string, i: int): uint32 =
  (uint32(s[i].uint8 and 0x7f) shl 24) or (uint32(s[i+1].uint8) shl 16) or
    (uint32(s[i+2].uint8) shl 8) or uint32(s[i+3].uint8)

proc runRstPeer*(arg: RstArg) {.thread.} =
  let listener = newSocket(buffered = false)
  listener.setSockOpt(OptReuseAddr, true)
  listener.bindAddr(Port(arg.port), "127.0.0.1")
  listener.listen()
  var client: Socket
  listener.accept(client)
  client.send(encodeSettings([]))
  var rest: string
  var prefaceDropped = false
  var opened = initHashSet[uint32]()
  var maxSid = 0'u32
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
        elif ftype == frameHeaders:
          if sid <= maxSid:
            # A new stream id below one already used is a connection error.
            orderErrors.atomicInc
            client.send(encodeGoAway(maxSid, errProtocol))
            break serve
          maxSid = sid
          opened.incl sid
          headersSeen.atomicInc
          if (flags and flagEndStream) != 0:
            client.send(encodeHeaders(sid, status200, endStream = true, endHeaders = true))
        elif ftype == frameData:
          dataSeen.atomicInc(length)
          if (flags and flagEndStream) != 0:
            client.send(encodeHeaders(sid, status200, endStream = true, endHeaders = true))
        elif ftype == frameRst:
          if sid in opened:
            rstSeen.atomicInc
          else:
            idleRstSeen.atomicInc
            client.send(encodeGoAway(0, errProtocol))
            break serve
  client.close()
  listener.close()

proc startRstPeer*(t: var Thread[RstArg], port: int) =
  rstSeen.store(0); idleRstSeen.store(0); headersSeen.store(0); orderErrors.store(0)
  dataSeen.store(0)
  createThread(t, runRstPeer, (port: port))
