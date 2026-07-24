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

proc newServerConn(maxBody = 0): H2Conn =
  ## A connection that has already consumed the server's opening SETTINGS preface,
  ## so a test can feed response / control frames without re-sending it.
  result = initH2Conn(maxBody)
  discard result.feed(encodeSettings([]))

suite "h2 client connection":
  test "sends preface and SETTINGS":
    let c = newServerConn()
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
    let c = newServerConn()
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
    let c = newServerConn()
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
    let c = newServerConn()
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
    let c = newServerConn()
    let id = c.openStream()
    discard c.feed(encodeRstStream(id, 1))
    check c.streamDone(id)
    check c.streamReset(id)

  test "GOAWAY marks the connection unreusable":
    let c = newServerConn()
    check c.canReuse()
    discard c.feed(encodeFrame(ftGoAway, 0, 0, "\x00\x00\x00\x00\x00\x00\x00\x00"))
    check c.goneAway
    check not c.canReuse()

  test "responds to a server PING with an ACK":
    let c = newServerConn()
    let toSend = c.feed(encodePing("01234567"))
    var d: FrameDecoder
    d.feed(toSend)
    var f: Frame
    check d.next(f)
    check f.typ == uint8(ftPing)
    check (f.flags and flagAck) != 0
    check f.payload == "01234567"

proc firstFrameOfType(s: string, typ: FrameType): bool =
  ## True if `s` contains a frame of the given type.
  var d: FrameDecoder
  d.feed(s)
  var f: Frame
  while d.next(f):
    if f.typ == uint8(typ): return true

suite "h2 client DoS limits":
  test "RSTs a CONTINUATION flood instead of buffering unbounded headers":
    let c = newServerConn()
    let id = c.openStream()
    # A HEADERS frame with END_STREAM but never END_HEADERS, then a flood of
    # CONTINUATION frames -- the shape of the HTTP/2 CONTINUATION flood.
    var flood = encodeHeaders(id, repeat("A", 16000), endStream = false, endHeaders = false)
    var control = ""
    for _ in 0 ..< 16:                     # 16 x 16000 = 256000 > 128 KiB cap
      control.add c.feed(encodeContinuation(id, repeat("B", 16000), endHeaders = false))
    let toSend = c.feed(flood) & control
    check c.streamReset(id)                # bounded and reset, not OOM
    check firstFrameOfType(toSend, ftRstStream)

  test "RSTs a response body that exceeds maxResponseBytes":
    let c = newServerConn(maxBody = 10)       # 10-byte cap
    let id = c.openStream()
    var server = encodeHeaders(id,
      (HpackEncoder()).encode(@[(":status", "200")]), endStream = false, endHeaders = true)
    server.add encodeData(id, repeat("x", 50), endStream = true)   # 50 > 10
    let toSend = c.feed(server)
    check c.streamReset(id)
    check c.streamTooLarge(id)
    check firstFrameOfType(toSend, ftRstStream)

  test "a body within maxResponseBytes is delivered normally":
    let c = newServerConn(maxBody = 100)
    let id = c.openStream()
    discard c.feed(serverResponse(id, "200", @[], "short body"))
    check c.streamDone(id)
    check not c.streamTooLarge(id)
    check c.takeResponse(id).body == "short body"

suite "h2 retry classification":
  test "REFUSED_STREAM marks the stream unprocessed (safe to retry)":
    let c = newServerConn()
    let id = c.openStream()
    discard c.feed(encodeRstStream(id, errRefusedStream))
    check c.streamReset(id)
    check c.streamUnprocessed(id)

  test "a non-refused RST_STREAM is not unprocessed":
    let c = newServerConn()
    let id = c.openStream()
    discard c.feed(encodeRstStream(id, errProtocolError))   # processed then failed
    check c.streamReset(id)
    check not c.streamUnprocessed(id)

  test "streams above GOAWAY's last-processed id are unprocessed":
    let c = newServerConn()
    let a = c.openStream()   # id 1
    let b = c.openStream()   # id 3
    let d = c.openStream()   # id 5
    discard c.feed(encodeFrame(ftGoAway, 0, 0, "\x00\x00\x00\x01\x00\x00\x00\x00"))
    check not c.streamUnprocessed(a)    # id 1 == last-processed: may have run
    check c.streamUnprocessed(b)        # id 3 > 1: not processed
    check c.streamUnprocessed(d)        # id 5 > 1: not processed

proc headForStream(id: uint32): string =
  encodeHeaders(id, (HpackEncoder()).encode(@[(":status", "200")]),
                endStream = false, endHeaders = true)

suite "h2 receive-side flow control":
  test "advertises a larger initial window in SETTINGS":
    let c = newServerConn()
    var d: FrameDecoder
    d.feed(c.preamble()[connectionPreface.len .. ^1])
    var f: Frame
    check d.next(f)
    check f.typ == uint8(ftSettings)
    var initWin = 0'u32
    for (id, v) in parseSettings(f.payload):
      if id == settingsInitialWindowSize: initWin = v
    check initWin > 65535'u32                    # bigger than the 64 KiB default

  test "batches WINDOW_UPDATEs (no ack for a small body)":
    let c = newServerConn()
    let id = c.openStream()
    var server = headForStream(id)
    server.add encodeData(id, repeat("x", 1000), endStream = false)   # well under threshold
    let toSend = c.feed(server)
    check not firstFrameOfType(toSend, ftWindowUpdate)

  test "replenishes once consumed crosses the threshold":
    let c = newServerConn()
    let id = c.openStream()
    var server = headForStream(id)
    for _ in 0 ..< 270:                          # 270 x 16000 = 4.32 MB > 4 MiB
      server.add encodeData(id, repeat("x", 16000), endStream = false)
    let toSend = c.feed(server)
    check firstFrameOfType(toSend, ftWindowUpdate)

proc dataBytes(s: string): int =
  ## Total DATA-frame payload bytes in a serialized frame sequence.
  var d: FrameDecoder
  d.feed(s)
  var f: Frame
  while d.next(f):
    if f.typ == uint8(ftData): result += f.payload.len

suite "h2 send-side flow control":
  test "caps the initial body to the window and releases on WINDOW_UPDATE":
    let c = newServerConn()
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

suite "h2 max concurrent streams":
  test "reads the peer's MAX_CONCURRENT_STREAMS from SETTINGS":
    let c = newServerConn()
    check c.maxConcurrentStreams == int.high        # unlimited until advertised
    discard c.feed(encodeSettings({settingsMaxConcurrentStreams: 3'u32}))
    check c.maxConcurrentStreams == 3

suite "h2 CONTINUATION on send":
  test "splits a large header block across HEADERS and CONTINUATION frames":
    let c = newServerConn()
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

suite "h2 connection errors":
  test "rejects a server whose first frame is not SETTINGS":
    let c = initH2Conn()                       # deliberately NOT pre-fed a preface
    let toSend = c.feed(encodePing("01234567"))  # first frame is PING, not SETTINGS
    check c.connError.len > 0
    check not c.canReuse()
    check firstFrameOfType(toSend, ftGoAway)

  test "rejects a frame larger than the max frame size":
    let c = newServerConn()
    let oversized = encodeFrame(ftData, 0, 1'u32, repeat("x", 20_000))  # > 16384
    let toSend = c.feed(oversized)
    check c.connError.len > 0
    check firstFrameOfType(toSend, ftGoAway)

  test "rejects an unexpected PUSH_PROMISE (push disabled)":
    let c = newServerConn()
    let toSend = c.feed(encodeFrame(ftPushPromise, 0, 1'u32, "\x00\x00\x00\x02hdr"))
    check c.connError.len > 0
    check firstFrameOfType(toSend, ftGoAway)
