## Sans-io HTTP/2 client connection tests, driven by simulated server frames.

import unittest
import std/strutils
import navi/proto/h2/[conn, frame, hpack]

proc serverResponse(streamId: uint32, status: string, headers: seq[HeaderPair],
                    body: string): string =
  ## Build a server-side HEADERS + DATA frame sequence for one stream.
  let enc = HpackEncoder()
  result = encodeHeaders(streamId,
    enc.encode(@[(":status", status)] & headers),
    endStream = false, endHeaders = true)
  result.add encodeData(streamId, body, endStream = true)

suite "h2 client connection":
  test "sends preface and SETTINGS":
    let c = initH2Conn()
    let pre = c.preamble()
    check pre.startsWith(connectionPreface)
    var d: FrameDecoder
    d.feed(pre[connectionPreface.len ..< pre.len])
    var f: Frame
    check d.next(f)
    check f.typ == uint8(ftSettings)
    check d.next(f)
    check f.typ == uint8(ftWindowUpdate)

  test "encodes a request as a HEADERS frame with pseudo-headers":
    let c = initH2Conn()
    let id = c.openStream()
    check id == 1'u32
    let wire = c.encodeRequest(id, @[
      (":method", "GET"), (":scheme", "https"), (":path", "/"),
      (":authority", "example.com")], body = "")
    var d: FrameDecoder
    d.feed(wire)
    var f: Frame
    check d.next(f)
    check f.typ == uint8(ftHeaders)
    check (f.flags and flagEndStream) != 0
    check (f.flags and flagEndHeaders) != 0
    var dec = initHpackDecoder()
    check dec.decode(f.payload) == @[
      (":method", "GET"), (":scheme", "https"), (":path", "/"),
      (":authority", "example.com")]

  test "assembles a response and acks control frames":
    let c = initH2Conn()
    let id = c.openStream()
    var server = encodeSettings([])
    server.add serverResponse(id, "200", @[("content-type", "text/plain")], "hello")

    let toSend = c.feed(server)
    check c.streamDone(id)
    let resp = c.takeResponse(id)
    check resp.status == 200
    check resp.body == "hello"
    check resp.headers == @[("content-type", "text/plain")]

    var d: FrameDecoder
    d.feed(toSend)
    var f: Frame
    check d.next(f)
    check f.typ == uint8(ftSettings)
    check (f.flags and flagAck) != 0

  test "assigns odd, incrementing stream ids and reuses one connection":
    let c = initH2Conn()
    let a = c.openStream()
    let b = c.openStream()
    check a == 1'u32
    check b == 3'u32
    # Two independent responses on the same connection.
    discard c.feed(serverResponse(a, "200", @[], "first"))
    discard c.feed(serverResponse(b, "201", @[], "second"))
    check c.streamDone(a)
    check c.streamDone(b)
    check c.takeResponse(a).body == "first"
    check c.takeResponse(b).body == "second"

  test "reports a stream reset":
    let c = initH2Conn()
    let id = c.openStream()
    discard c.feed(encodeRstStream(id, 1))
    check c.streamDone(id)
    check c.streamReset(id)

  test "GOAWAY marks the connection unreusable":
    let c = initH2Conn()
    check c.canReuse()
    discard c.feed(encodeFrame(ftGoAway, 0, 0, "\x00\x00\x00\x00\x00\x00\x00\x00"))
    check c.goneAway
    check not c.canReuse()

  test "responds to a server PING with an ACK":
    let c = initH2Conn()
    let toSend = c.feed(encodePing("01234567"))
    var d: FrameDecoder
    d.feed(toSend)
    var f: Frame
    check d.next(f)
    check f.typ == uint8(ftPing)
    check (f.flags and flagAck) != 0
    check f.payload == "01234567"

proc dataBytes(s: string): int =
  ## Total DATA-frame payload bytes in a serialized frame sequence.
  var d: FrameDecoder
  d.feed(s)
  var f: Frame
  while d.next(f):
    if f.typ == uint8(ftData): result += f.payload.len

suite "h2 send-side flow control":
  test "caps the initial body to the window and releases on WINDOW_UPDATE":
    let c = initH2Conn()
    let sid = c.openStream()
    let body = repeat("x", 200_000)          # > the 65535 default send window
    let head = @[(":method", "POST"), (":scheme", "https"),
                 (":path", "/"), (":authority", "x")]
    let out1 = c.encodeRequest(sid, head, body)
    check dataBytes(out1) == 65535           # only the initial window goes out now

    # grant 100k on the stream and the connection
    let out2 = c.feed(encodeWindowUpdate(sid, 100_000) & encodeWindowUpdate(0, 100_000))
    check dataBytes(out2) == 100_000

    # grant the rest; the whole body must be sent, and no more
    let out3 = c.feed(encodeWindowUpdate(sid, 200_000) & encodeWindowUpdate(0, 200_000))
    check dataBytes(out1) + dataBytes(out2) + dataBytes(out3) == 200_000

suite "h2 CONTINUATION on send":
  test "splits a large header block across HEADERS and CONTINUATION frames":
    let c = initH2Conn()
    let sid = c.openStream()
    let big = repeat("v", 20_000)          # forces the block over the 16384 frame size
    let want = @[(":method", "GET"), (":scheme", "https"), (":path", "/"),
                 (":authority", "x"), ("x-big", big)]
    let wire = c.encodeRequest(sid, want, body = "")

    var d: FrameDecoder
    d.feed(wire)
    var frames: seq[Frame]
    var f: Frame
    while d.next(f): frames.add f

    check frames.len >= 2
    check frames[0].typ == uint8(ftHeaders)
    check (frames[0].flags and flagEndHeaders) == 0        # more headers follow
    check frames[^1].typ == uint8(ftContinuation)
    check (frames[^1].flags and flagEndHeaders) != 0       # last one ends the block
    for fr in frames:
      check fr.payload.len <= defaultMaxFrameSize          # every frame within the limit

    var hb = ""
    for fr in frames: hb.add fr.payload
    var dec = initHpackDecoder()
    check dec.decode(hb) == want                           # reassembles losslessly
