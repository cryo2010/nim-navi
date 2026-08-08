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
  test "the h2 client should send the preface and SETTINGS":
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

  test "the h2 client should encode a request as a HEADERS frame with pseudo-headers":
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

  test "the h2 client should assemble a response and ACK control frames":
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

  test "the h2 client should assign odd, incrementing stream ids and reuse one connection":
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

  test "the h2 client should report a stream reset when the server sends RST_STREAM":
    let c = newServerConn()
    let id = c.openStream()
    discard c.feed(encodeRstStream(id, 1))
    check c.streamDone(id)
    check c.streamReset(id)

  test "the connection should become unreusable after GOAWAY":
    let c = newServerConn()
    check c.canReuse()
    discard c.feed(encodeFrame(ftGoAway, 0, 0, "\x00\x00\x00\x00\x00\x00\x00\x00"))
    check c.goneAway
    check not c.canReuse()

  test "the h2 client should respond to a server PING with an ACK":
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
  test "the h2 client should RST a CONTINUATION flood instead of buffering unbounded headers":
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

  test "the h2 client should RST a response body when it exceeds maxResponseBytes":
    let c = newServerConn(maxBody = 10)       # 10-byte cap
    let id = c.openStream()
    var server = encodeHeaders(id,
      (HpackEncoder()).encode(@[(":status", "200")]), endStream = false, endHeaders = true)
    server.add encodeData(id, repeat("x", 50), endStream = true)   # 50 > 10
    let toSend = c.feed(server)
    check c.streamReset(id)
    check c.streamTooLarge(id)
    check firstFrameOfType(toSend, ftRstStream)

  test "the h2 client should deliver a body normally when it is within maxResponseBytes":
    let c = newServerConn(maxBody = 100)
    let id = c.openStream()
    discard c.feed(serverResponse(id, "200", @[], "short body"))
    check c.streamDone(id)
    check not c.streamTooLarge(id)
    check c.takeResponse(id).body == "short body"

suite "h2 retry classification":
  test "the stream should be marked unprocessed when reset with REFUSED_STREAM (safe to retry)":
    let c = newServerConn()
    let id = c.openStream()
    discard c.feed(encodeRstStream(id, errRefusedStream))
    check c.streamReset(id)
    check c.streamUnprocessed(id)

  test "the stream should not be marked unprocessed when reset with a non-refused RST_STREAM":
    let c = newServerConn()
    let id = c.openStream()
    discard c.feed(encodeRstStream(id, errProtocolError))   # processed then failed
    check c.streamReset(id)
    check not c.streamUnprocessed(id)

  test "streams should be marked unprocessed when above GOAWAY's last-processed id":
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
  test "the receive window should advertise a larger initial window in SETTINGS":
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

  test "the receive window should batch WINDOW_UPDATEs when the body is small":
    let c = newServerConn()
    let id = c.openStream()
    var server = headForStream(id)
    server.add encodeData(id, repeat("x", 1000), endStream = false)   # well under threshold
    let toSend = c.feed(server)
    check not firstFrameOfType(toSend, ftWindowUpdate)

  test "the receive window should replenish with a WINDOW_UPDATE when consumed crosses the threshold":
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
  test "the send window should cap the initial body to the window and release the rest on WINDOW_UPDATE":
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
  test "the h2 client should read the peer's MAX_CONCURRENT_STREAMS from SETTINGS":
    let c = newServerConn()
    check c.maxConcurrentStreams == int.high        # unlimited until advertised
    discard c.feed(encodeSettings({settingsMaxConcurrentStreams: 3'u32}))
    check c.maxConcurrentStreams == 3

suite "h2 CONTINUATION on send":
  test "the h2 client should split a large header block across HEADERS and CONTINUATION frames":
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
  test "the h2 client should reject a server when its first frame is not SETTINGS (GOAWAY)":
    let c = initH2Conn()                       # deliberately NOT pre-fed a preface
    let toSend = c.feed(encodePing("01234567"))  # first frame is PING, not SETTINGS
    check c.connError.len > 0
    check not c.canReuse()
    check firstFrameOfType(toSend, ftGoAway)

  test "the h2 client should reject a frame when it is larger than the max frame size (GOAWAY)":
    let c = newServerConn()
    let oversized = encodeFrame(ftData, 0, 1'u32, repeat("x", 20_000))  # > 16384
    let toSend = c.feed(oversized)
    check c.connError.len > 0
    check firstFrameOfType(toSend, ftGoAway)

  test "the h2 client should reject an unexpected PUSH_PROMISE when push is disabled (GOAWAY)":
    let c = newServerConn()
    let toSend = c.feed(encodeFrame(ftPushPromise, 0, 1'u32, "\x00\x00\x00\x02hdr"))
    check c.connError.len > 0
    check firstFrameOfType(toSend, ftGoAway)

proc paddedData(id: uint32, data: string, padLen: int, endStream: bool): string =
  ## A DATA frame with `padLen` bytes of padding: [pad length][data][zero padding].
  let flags = flagPadded or (if endStream: flagEndStream else: 0'u8)
  encodeFrame(ftData, flags, id, char(padLen) & data & repeat('\0', padLen))

suite "h2 frame padding":
  test "the h2 client should strip DATA frame padding from the body":
    let c = newServerConn()
    let id = c.openStream()
    var server = headForStream(id)
    server.add paddedData(id, "hello", padLen = 7, endStream = true)
    discard c.feed(server)
    check c.streamDone(id)
    check c.takeResponse(id).body == "hello"     # pad byte + 7 padding bytes gone

  test "the h2 client should strip HEADERS frame padding before HPACK decode":
    let c = newServerConn()
    let id = c.openStream()
    let block0 = (HpackEncoder()).encode(@[(":status", "200"), ("x-k", "v")])
    let payload = char(4) & block0 & repeat('\0', 4)
    var server = encodeFrame(ftHeaders, flagPadded or flagEndHeaders, id, payload)
    server.add encodeData(id, "body", endStream = true)
    discard c.feed(server)
    check c.streamDone(id)
    let resp = c.takeResponse(id)
    check resp.status == 200
    check resp.headers == @[("x-k", "v")]        # padding did not corrupt HPACK
    check resp.body == "body"

  test "a zero-data padded DATA frame should be delivered as empty":
    let c = newServerConn()
    let id = c.openStream()
    var server = headForStream(id)
    server.add paddedData(id, "", padLen = 5, endStream = true)
    discard c.feed(server)
    check c.streamDone(id)
    check c.takeResponse(id).body == ""

  test "the h2 client should reject DATA padding that meets or exceeds the payload (PROTOCOL_ERROR)":
    let c = newServerConn()
    let id = c.openStream()
    var server = headForStream(id)
    # Pad length 10, but only 5 bytes follow the pad-length byte.
    server.add encodeFrame(ftData, flagPadded or flagEndStream, id, char(10) & "short")
    let toSend = c.feed(server)
    check c.connError.len > 0
    check firstFrameOfType(toSend, ftGoAway)

proc hasConnWindowUpdate(s: string): bool =
  ## True if `s` contains a connection-level (stream 0) WINDOW_UPDATE.
  var d: FrameDecoder
  d.feed(s)
  var f: Frame
  while d.next(f):
    if f.typ == uint8(ftWindowUpdate) and f.streamId == 0'u32: return true

suite "h2 interim responses and trailers":
  test "the h2 client should ignore a 1xx interim HEADERS block before the final response":
    let c = newServerConn()
    let id = c.openStream()
    let enc = HpackEncoder()                       # one server-side HPACK stream
    var server = encodeHeaders(id,
      enc.encode(@[(":status", "103"), ("link", "</s.css>; rel=preload")]),
      endStream = false, endHeaders = true)
    server.add encodeHeaders(id,
      enc.encode(@[(":status", "200"), ("content-type", "text/html")]),
      endStream = false, endHeaders = true)
    server.add encodeData(id, "body", endStream = true)
    discard c.feed(server)
    check c.streamDone(id)
    let resp = c.takeResponse(id)
    check resp.status == 200                        # final status, not 103
    check resp.headers == @[("content-type", "text/html")]  # 103 "link" did not leak
    check resp.body == "body"

  test "the h2 client should not surface trailers as response headers":
    let c = newServerConn()
    let id = c.openStream()
    let enc = HpackEncoder()
    var server = encodeHeaders(id,
      enc.encode(@[(":status", "200"), ("content-type", "text/plain")]),
      endStream = false, endHeaders = true)
    server.add encodeData(id, "hello", endStream = false)
    server.add encodeHeaders(id, enc.encode(@[("grpc-status", "0")]),
                             endStream = true, endHeaders = true)   # trailers
    discard c.feed(server)
    check c.streamDone(id)
    let resp = c.takeResponse(id)
    check resp.status == 200
    check resp.body == "hello"
    check resp.headers == @[("content-type", "text/plain")]  # grpc-status trailer dropped

suite "h2 flow control on reset streams":
  test "the connection window should count DATA on a reset stream toward the connection window":
    # A tiny body cap makes the first DATA reset the stream; the server then keeps
    # sending DATA in-flight on that stream. It must still replenish the connection
    # flow-control window (RFC 9113 6.9.1), or a pooled connection slowly stalls.
    let c = newServerConn(maxBody = 10)
    let id = c.openStream()
    var server = headForStream(id)
    server.add encodeData(id, repeat("x", 50), endStream = false)   # 50 > 10 -> RST
    discard c.feed(server)
    check c.streamReset(id)
    var more = ""
    for _ in 0 ..< 270: more.add encodeData(id, repeat("x", 16000), endStream = false)
    let toSend = c.feed(more)                       # 270 x 16000 = 4.32 MB > connReplenish
    check hasConnWindowUpdate(toSend)               # connection window given back

proc allFrames(s: string): seq[Frame] =
  var d: FrameDecoder
  d.feed(s)
  var f: Frame
  while d.next(f): result.add f

proc endStreamCount(s: string): int =
  for f in allFrames(s):
    if (f.flags and flagEndStream) != 0: inc result

proc lastFrameEndsStream(s: string): bool =
  let fs = allFrames(s)
  fs.len > 0 and (fs[^1].flags and flagEndStream) != 0

suite "h2 streaming request body":
  # Pull-based upload: HEADERS without END_STREAM, then DATA frames from the
  # producer, END_STREAM on the final frame. Exercises the sans-io send API
  # (encodeRequestHead / queueSend / finishSend) the engine drives.
  let head = @[(":method", "POST"), (":scheme", "https"),
               (":path", "/"), (":authority", "x")]

  test "a streamed body should send HEADERS without END_STREAM, DATA per chunk, END_STREAM last":
    let c = newServerConn()
    let sid = c.openStream()
    var wire = c.encodeRequestHead(sid, head)
    let hs = allFrames(wire)
    check hs.len == 1 and hs[0].typ == uint8(ftHeaders)
    check (hs[0].flags and flagEndStream) == 0     # body follows, so not ended here
    wire.add c.queueSend(sid, "hello ")
    wire.add c.queueSend(sid, "world")
    wire.add c.finishSend(sid)
    check dataBytes(wire) == len("hello world")     # whole body on the wire
    check endStreamCount(wire) == 1                  # END_STREAM exactly once...
    check lastFrameEndsStream(wire)                  # ...on the final frame

  test "an empty streamed body should end the stream with an empty DATA frame":
    let c = newServerConn()
    let sid = c.openStream()
    var wire = c.encodeRequestHead(sid, head)
    wire.add c.finishSend(sid)                       # no chunks produced
    let fs = allFrames(wire)
    check fs.len == 2
    check fs[0].typ == uint8(ftHeaders) and (fs[0].flags and flagEndStream) == 0
    check fs[1].typ == uint8(ftData) and fs[1].payload.len == 0
    check (fs[1].flags and flagEndStream) != 0

  test "a streamed chunk over the window should be released by WINDOW_UPDATE with END_STREAM on the tail":
    let c = newServerConn()
    let sid = c.openStream()
    discard c.encodeRequestHead(sid, head)
    let out1 = c.queueSend(sid, repeat("x", 100_000))  # > 65535 default send window
    check dataBytes(out1) == 65535                   # only the window goes out now
    check not c.sendDrained(sid)                     # tail still queued
    let out2 = c.finishSend(sid)                     # windowed out: nothing to emit yet
    check dataBytes(out2) == 0
    let out3 = c.feed(encodeWindowUpdate(sid, 100_000) & encodeWindowUpdate(0, 100_000))
    check dataBytes(out1) + dataBytes(out3) == 100_000  # the whole body, no more
    check c.sendDrained(sid)
    check lastFrameEndsStream(out3)                  # END_STREAM rides the tail

suite "h2 incremental response body":
  # The engine drains the body per feed via takeBody (bounded memory) instead of
  # buffering the whole response, decoding content-encoding as chunks arrive.
  let head = @[(":method", "GET"), (":scheme", "https"),
               (":path", "/"), (":authority", "x")]
  proc serverHeaders(sid: uint32, extra: seq[HeaderPair] = @[]): string =
    let enc = HpackEncoder()
    encodeHeaders(sid, enc.encode(@[(":status", "200")] & extra),
                  endStream = false, endHeaders = true)

  test "takeBody drains the body incrementally and respHeader reads a header":
    let c = newServerConn()
    let sid = c.openStream()
    discard c.encodeRequest(sid, head, "")
    discard c.feed(serverHeaders(sid, @[("content-encoding", "identity")]))
    check c.respHeader(sid, "content-encoding") == "identity"
    discard c.feed(encodeData(sid, "hello ", endStream = false))
    check c.takeBody(sid) == "hello "
    check c.takeBody(sid) == ""                      # nothing left until more arrives
    discard c.feed(encodeData(sid, "world", endStream = true))
    check c.takeBody(sid) == "world"
    check c.streamEnded(sid)
    check c.takeResponse(sid).body == ""            # drained, never buffered whole

  test "the size cap counts total body bytes even when drained incrementally":
    let c = newServerConn(maxBody = 10)
    let sid = c.openStream()
    discard c.encodeRequest(sid, head, "")
    discard c.feed(serverHeaders(sid))
    discard c.feed(encodeData(sid, repeat("x", 8), endStream = false))
    check c.takeBody(sid).len == 8                  # drained; resp.body is now empty
    discard c.feed(encodeData(sid, repeat("x", 8), endStream = false))  # total 16 > 10
    check c.streamTooLarge(sid)                     # cap tracks the running total, not the buffer

proc hasStreamWindowUpdate(s: string, sid: uint32): bool =
  for f in allFrames(s):
    if f.typ == uint8(ftWindowUpdate) and f.streamId == sid: return true

proc feedBody(c: H2Conn, sid: uint32, total: int): string =
  ## Feed `total` body bytes as a run of DATA frames (each within the max frame
  ## size); return the control bytes the connection emits back.
  var srv = ""
  var left = total
  while left > 0:
    let n = min(16000, left)
    srv.add encodeData(sid, repeat("x", n), endStream = false)
    left -= n
  c.feed(srv)

suite "h2 gated receive window (sink backpressure)":
  # A sinkMode stream holds its receive-window credit until the sink acks
  # consumption, so a slow consumer stalls the peer instead of buffering without
  # bound. > streamReplenish (recvWindowSize div 2 = 4 MiB) so the eager path
  # would otherwise emit a stream WINDOW_UPDATE.
  const overThreshold = 4 * 1024 * 1024 + 16000
  let head = @[(":method", "GET"), (":scheme", "https"),
               (":path", "/"), (":authority", "x")]

  proc respond200(c: H2Conn, sid: uint32) =
    let enc = HpackEncoder()
    discard c.feed(encodeHeaders(sid, enc.encode(@[(":status", "200")]),
                                 endStream = false, endHeaders = true))

  test "a sinkMode stream holds its stream WINDOW_UPDATE until ackRecv":
    let c = newServerConn()
    let sid = c.openStream()
    discard c.encodeRequest(sid, head, "")
    c.respond200(sid)
    c.setSinkMode(sid)
    let out1 = c.feedBody(sid, overThreshold)
    check not hasStreamWindowUpdate(out1, sid)      # gated: stream window held despite > threshold
    discard c.takeBody(sid)                         # deliver to the (slow) sink...
    let ack = c.ackRecv(sid, overThreshold)         # ...then ack once it has consumed
    check hasStreamWindowUpdate(ack, sid)           # now the stream window is released

  test "a non-sinkMode stream replenishes its window eagerly (control)":
    let c = newServerConn()
    let sid = c.openStream()
    discard c.encodeRequest(sid, head, "")
    c.respond200(sid)
    let out1 = c.feedBody(sid, overThreshold)
    check hasStreamWindowUpdate(out1, sid)          # eager: emitted during feed, no ack needed
