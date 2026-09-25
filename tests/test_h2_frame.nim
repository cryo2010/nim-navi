## Sans-io HTTP/2 frame encode/decode tests.

import unittest
import navi/proto/h2/frame

suite "h2 frame encode/decode":
  test "the frame codec should round-trip a frame header and payload":
    let wire = encodeFrame(ftHeaders, flagEndHeaders, 1'u32, "abc")
    check wire.len == 9 + 3
    var d: FrameDecoder
    d.feed(wire)
    var f: Frame
    check d.next(f)
    check f.typ == uint8(ftHeaders)
    check f.flags == flagEndHeaders
    check f.streamId == 1'u32
    check f.payload == "abc"
    check not d.next(f)  # nothing left

  test "the frame codec should encode the length prefix as 24-bit big-endian":
    let wire = encodeFrame(ftData, 0, 3'u32, "hello")
    check int(uint8(wire[0])) == 0
    check int(uint8(wire[1])) == 0
    check int(uint8(wire[2])) == 5   # payload length

  test "the frame codec should clear the reserved bit of the stream id":
    let wire = encodeFrame(ftData, 0, 0x80000001'u32, "")
    var d: FrameDecoder
    d.feed(wire)
    var f: Frame
    check d.next(f)
    check f.streamId == 1'u32

  test "the frame codec should decode frames arriving in split chunks":
    let wire = encodeFrame(ftData, flagEndStream, 5'u32, "payload")
    var d: FrameDecoder
    var f: Frame
    d.feed(wire[0 ..< 4])
    check not d.next(f)          # header incomplete
    d.feed(wire[4 ..< wire.len])
    check d.next(f)
    check f.payload == "payload"
    check (f.flags and flagEndStream) != 0

  test "the frame codec should decode two frames from one buffer":
    var buf = encodeFrame(ftPing, 0, 0, "01234567")
    buf.add encodeWindowUpdate(0, 65535)
    var d: FrameDecoder
    d.feed(buf)
    var f: Frame
    check d.next(f)
    check f.typ == uint8(ftPing)
    check f.payload == "01234567"
    check d.next(f)
    check f.typ == uint8(ftWindowUpdate)

suite "h2 encodeDataInto (the single-copy DATA encoder)":
  # flushSend frames every streamed-upload chunk through `encodeDataInto`, which
  # writes the 9-byte header and copyMems the payload straight into the output
  # buffer. It must stay byte-for-byte identical to the `encodeData` it replaced,
  # since only `encodeData` is covered by the round-trip tests above.
  const cases = [(1'u32, "hello", false), (5'u32, "", true),
                 (0x7fffffff'u32, "x", true), (3'u32, "payload", false)]

  test "it should produce exactly what encodeData does":
    for (sid, payload, endStream) in cases:
      var outbuf = ""
      encodeDataInto(outbuf, sid, payload, 0, payload.len, endStream)
      check outbuf == encodeData(sid, payload, endStream)

  test "it should frame a slice of the source without copying it out first":
    # The upload path never slices: it passes the whole buffer with an offset and a
    # length, which is the case a naive implementation gets wrong.
    let src = "0123456789"
    var outbuf = ""
    encodeDataInto(outbuf, 7'u32, src, 3, 4, endStream = false)
    check outbuf == encodeData(7'u32, "3456", endStream = false)
    var d: FrameDecoder
    d.feed(outbuf)
    var f: Frame
    check d.next(f)
    check f.typ == uint8(ftData)
    check f.streamId == 7'u32
    check f.payload == "3456"
    check (f.flags and flagEndStream) == 0

  test "it should append to a buffer that already holds frames":
    # flushSend accumulates several DATA frames (and trailers) in one output buffer,
    # so the encoder must append at `outbuf.len`, never overwrite from the start.
    var outbuf = encodeSettingsAck()
    let head = outbuf
    encodeDataInto(outbuf, 1'u32, "aa", 0, 2, endStream = false)
    encodeDataInto(outbuf, 1'u32, "bb", 0, 2, endStream = true)
    check outbuf == head & encodeData(1'u32, "aa", false) & encodeData(1'u32, "bb", true)
    var d: FrameDecoder
    d.feed(outbuf)
    var f: Frame
    check d.next(f) and f.typ == uint8(ftSettings)
    check d.next(f) and f.payload == "aa" and (f.flags and flagEndStream) == 0
    check d.next(f) and f.payload == "bb" and (f.flags and flagEndStream) != 0
    check not d.next(f)

  test "it should clear the reserved bit of the stream id":
    var outbuf = ""
    encodeDataInto(outbuf, 0x80000001'u32, "z", 0, 1, endStream = false)
    var d: FrameDecoder
    d.feed(outbuf)
    var f: Frame
    check d.next(f)
    check f.streamId == 1'u32

  test "it should encode a length that needs all 24 bits of the prefix":
    let big = newString(70000)                 # > 0xffff: exercises the high byte
    var outbuf = ""
    encodeDataInto(outbuf, 1'u32, big, 0, big.len, endStream = false)
    check outbuf.len == 9 + big.len
    check int(uint8(outbuf[0])) == ((big.len shr 16) and 0xff)
    check outbuf == encodeData(1'u32, big, false)

suite "h2 settings":
  test "the settings codec should encode and parse settings pairs":
    let wire = encodeSettings({settingsInitialWindowSize: 65535'u32,
                               settingsMaxConcurrentStreams: 100'u32})
    var d: FrameDecoder
    d.feed(wire)
    var f: Frame
    check d.next(f)
    check f.typ == uint8(ftSettings)
    let params = parseSettings(f.payload)
    check params.len == 2
    check params[0] == (settingsInitialWindowSize, 65535'u32)
    check params[1] == (settingsMaxConcurrentStreams, 100'u32)

  test "the settings codec should set the ack flag with an empty payload for a settings ack":
    var d: FrameDecoder
    d.feed(encodeSettingsAck())
    var f: Frame
    check d.next(f)
    check (f.flags and flagAck) != 0
    check f.payload.len == 0
