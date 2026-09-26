## Sans-io HTTP/2 frame encode/decode tests.

import unittest
import std/strutils
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

suite "h2 frame peek/consume (the zero-copy DATA path)":
  # `next` materializes the payload as its own string; the connection layer's DATA
  # path instead peeks the header and copies the payload out of the decoder buffer
  # straight into the response body (issue #400). Both must see the same frames.
  test "peek should report the header without consuming the frame":
    var d: FrameDecoder
    d.feed(encodeData(3'u32, "hello", endStream = true))
    var h: FrameHeader
    check d.peek(h)
    check h.typ == uint8(ftData)
    check h.flags == flagEndStream
    check h.streamId == 3'u32
    check h.length == 5
    var h2: FrameHeader
    check d.peek(h2)                   # peeking again reports the same frame
    check h2.length == 5
    var f: Frame
    check d.next(f)                    # and `next` still gets it
    check f.payload == "hello"

  test "peek should clear the reserved bit of the stream id":
    var d: FrameDecoder
    d.feed(encodeFrame(ftData, 0, 0x80000001'u32, "x"))
    var h: FrameHeader
    check d.peek(h)
    check h.streamId == 1'u32

  test "consume should advance past the peeked frame":
    var buf = encodeData(1'u32, "first", endStream = false)
    buf.add encodeData(1'u32, "second", endStream = true)
    var d: FrameDecoder
    d.feed(buf)
    var h: FrameHeader
    check d.peek(h)
    check h.length == 5
    d.consume()
    var f: Frame
    check d.next(f)                    # the next frame, not the consumed one
    check f.payload == "second"
    check not d.next(f)

  test "peek should report false until the whole frame is buffered":
    let wire = encodeData(1'u32, "payload", endStream = false)
    var d: FrameDecoder
    var h: FrameHeader
    d.feed(wire[0 ..< 4])
    check not d.peek(h)                # header incomplete
    d.feed(wire[4 ..< wire.len - 1])
    check not d.peek(h)                # payload incomplete
    d.feed(wire[^1 .. ^1])
    check d.peek(h)
    check h.length == 7

  test "appendPayload should copy the whole payload and a sub-range byte-exactly":
    var d: FrameDecoder
    d.feed(encodeData(1'u32, "0123456789", endStream = false))
    var h: FrameHeader
    check d.peek(h)
    var whole = ""
    d.appendPayload(whole, 0, h.length)
    check whole == "0123456789"
    var part = ""
    d.appendPayload(part, 3, 4)
    check part == "3456"

  test "appendPayload should append to a destination that already holds bytes":
    var d: FrameDecoder
    d.feed(encodeData(1'u32, "world", endStream = false))
    var h: FrameHeader
    check d.peek(h)
    var dst = "hello "
    d.appendPayload(dst, 0, h.length)
    check dst == "hello world"

  test "appendPayload should be a no-op for a zero or negative length":
    var d: FrameDecoder
    d.feed(encodeData(1'u32, "abc", endStream = false))
    var h: FrameHeader
    check d.peek(h)
    var dst = "keep"
    d.appendPayload(dst, 0, 0)
    d.appendPayload(dst, 1, -1)
    check dst == "keep"

  test "payloadByte should read the pad-length octet of a padded frame":
    # A padded DATA payload is: pad length octet, content, padding (RFC 9113 6.1).
    let payload = "\x02" & "body" & "\x00\x00"
    var d: FrameDecoder
    d.feed(encodeFrame(ftData, flagPadded, 1'u32, payload))
    var h: FrameHeader
    check d.peek(h)
    check d.payloadByte(0) == 2'u8
    var content = ""
    d.appendPayload(content, 1, h.length - 1 - int(d.payloadByte(0)))
    check content == "body"

  test "remaining should report the bytes buffered but not consumed":
    var buf = encodeData(1'u32, "aaaa", endStream = false)
    buf.add encodeData(1'u32, "bb", endStream = false)
    var d: FrameDecoder
    d.feed(buf)
    check d.remaining == buf.len
    var h: FrameHeader
    check d.peek(h)
    check d.remaining == buf.len       # peek does not consume
    d.consume()
    check d.remaining == 9 + 2

  test "peek should flag a frame larger than the max frame size":
    var wire = encodeFrame(ftData, 0, 1'u32, "")
    wire[0] = char(0)                  # length = 16385, one over the max
    wire[1] = char(0x40)
    wire[2] = char(0x01)
    var d: FrameDecoder
    d.feed(wire)
    var h: FrameHeader
    check not d.peek(h)
    check d.frameSizeError

  test "a frame split at every byte offset should still decode":
    # The compaction path in `feed` runs whenever a read ends mid-frame, which is
    # the normal case; every split point must preserve the unconsumed bytes.
    var buf = encodeData(1'u32, "0123456789abcdef", endStream = false)
    buf.add encodeFrame(ftPing, 0, 0, "01234567")
    for split in 1 ..< buf.len:
      var d: FrameDecoder
      d.feed(buf[0 ..< split])
      var got: seq[string] = @[]
      var h: FrameHeader
      while d.peek(h):
        var payload = ""
        d.appendPayload(payload, 0, h.length)
        got.add payload
        d.consume()
      d.feed(buf[split ..< buf.len])
      while d.peek(h):
        var payload = ""
        d.appendPayload(payload, 0, h.length)
        got.add payload
        d.consume()
      check got == @["0123456789abcdef", "01234567"]

  test "a feed after a partial consumption should keep the unconsumed bytes intact":
    var buf = encodeData(1'u32, "first", endStream = false)
    let second = encodeData(1'u32, "second", endStream = false)
    buf.add second[0 ..< 5]            # the next frame's header, truncated
    var d: FrameDecoder
    d.feed(buf)
    var h: FrameHeader
    check d.peek(h)
    d.consume()                        # compaction on the NEXT feed has 5 bytes to keep
    check not d.peek(h)
    d.feed("")                         # an empty read still compacts
    check d.remaining == 5
    check not d.peek(h)
    for i in 5 ..< second.len:         # then a byte at a time
      d.feed(second[i .. i])
    check d.peek(h)
    check h.length == 6
    var payload = ""
    d.appendPayload(payload, 0, h.length)
    check payload == "second"
    d.consume()
    check d.remaining == 0

  test "next and peek should agree on a long run of frames fed in ragged chunks":
    var wire = ""
    var expected: seq[string] = @[]
    for i in 0 ..< 40:
      let payload = repeat(char(ord('a') + i mod 26), i * 7)
      expected.add payload
      wire.add encodeData(1'u32, payload, endStream = false)
    var d: FrameDecoder
    var got: seq[string] = @[]
    var off = 0
    var take = 1
    while off < wire.len:
      let n = min(take, wire.len - off)
      d.feed(wire[off ..< off + n])
      off += n
      take = take * 3 + 1              # ragged, never aligned with frame boundaries
      var h: FrameHeader
      while d.peek(h):
        var payload = ""
        d.appendPayload(payload, 0, h.length)
        got.add payload
        d.consume()
    check got == expected
