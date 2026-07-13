## Sans-io HTTP/2 frame encode/decode tests.

import unittest
import navi/proto/h2/frame

suite "h2 frame encode/decode":
  test "round-trips a frame header and payload":
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

  test "length prefix is 24-bit big-endian":
    let wire = encodeFrame(ftData, 0, 3'u32, "hello")
    check int(uint8(wire[0])) == 0
    check int(uint8(wire[1])) == 0
    check int(uint8(wire[2])) == 5   # payload length

  test "clears the reserved bit of the stream id":
    let wire = encodeFrame(ftData, 0, 0x80000001'u32, "")
    var d: FrameDecoder
    d.feed(wire)
    var f: Frame
    check d.next(f)
    check f.streamId == 1'u32

  test "decodes frames arriving in split chunks":
    let wire = encodeFrame(ftData, flagEndStream, 5'u32, "payload")
    var d: FrameDecoder
    var f: Frame
    d.feed(wire[0 ..< 4])
    check not d.next(f)          # header incomplete
    d.feed(wire[4 ..< wire.len])
    check d.next(f)
    check f.payload == "payload"
    check (f.flags and flagEndStream) != 0

  test "decodes two frames from one buffer":
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

suite "h2 settings":
  test "encodes and parses settings pairs":
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

  test "settings ack sets the ack flag with an empty payload":
    var d: FrameDecoder
    d.feed(encodeSettingsAck())
    var f: Frame
    check d.next(f)
    check (f.flags and flagAck) != 0
    check f.payload.len == 0
