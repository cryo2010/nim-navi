## HPACK tests, using RFC 7541 worked examples (Appendix C).

import unittest
import std/strutils
import navi/proto/h2/hpack

proc hex(s: string): string =
  for i in countup(0, s.len - 2, 2):
    result.add char(parseHexInt(s[i .. i + 1]))

suite "hpack decode (RFC 7541 Appendix C.3, without Huffman)":
  test "C.3.1 first request builds the expected headers":
    var dec = initHpackDecoder()
    let headers = dec.decode(hex("828684410f7777772e6578616d706c652e636f6d"))
    check headers == @[
      (":method", "GET"), (":scheme", "http"), (":path", "/"),
      (":authority", "www.example.com")]

  test "C.4.1 decodes a request with Huffman-coded values":
    var dec = initHpackDecoder()
    let headers = dec.decode(hex("828684418cf1e3c2e5f23a6ba0ab90f4ff"))
    check headers == @[
      (":method", "GET"), (":scheme", "http"), (":path", "/"),
      (":authority", "www.example.com")]

  test "C.3.2 second request resolves a dynamic-table reference":
    var dec = initHpackDecoder()
    discard dec.decode(hex("828684410f7777772e6578616d706c652e636f6d"))
    # :method GET, :scheme http, :path /, :authority (dyn idx 62), cache-control no-cache
    let headers = dec.decode(hex("828684be58086e6f2d6361636865"))
    check headers == @[
      (":method", "GET"), (":scheme", "http"), (":path", "/"),
      (":authority", "www.example.com"), ("cache-control", "no-cache")]

suite "hpack encode":
  test "indexes an exact static-table entry":
    let enc = HpackEncoder()
    # :method GET is static index 2 -> single indexed byte 0x82
    let encoded = enc.encode(@[(":method", "GET")])
    check encoded == "\x82"

  test "encoder output round-trips through the decoder":
    let enc = HpackEncoder()
    var dec = initHpackDecoder()
    let headers = @[
      (":method", "POST"), (":path", "/submit"), (":authority", "api.test"),
      ("content-type", "application/json"), ("x-custom", "hello world")]
    check dec.decode(enc.encode(headers)) == headers

  test "lowercases header names":
    let enc = HpackEncoder()
    var dec = initHpackDecoder()
    let decoded = dec.decode(enc.encode(@[("Content-Type", "text/plain")]))
    check decoded == @[("content-type", "text/plain")]

suite "hpack decode rejects malformed input without crashing":
  # A peer controls the header block, so truncated/oversized fields must raise a
  # catchable error, never an IndexDefect/OverflowDefect (found by tests/fuzz).
  test "a string length past the end of the block raises":
    var dec = initHpackDecoder()
    # literal-with-indexing whose value length runs past the buffer (fuzz-found)
    expect ValueError:
      discard dec.decode(hex("a20f0b0d04c36ed80e71e0fd77"))

  test "a truncated integer continuation raises":
    var dec = initHpackDecoder()
    expect ValueError:
      discard dec.decode("\xff")  # indexed field, all-ones prefix, no continuation

  test "an oversized integer raises instead of overflowing":
    var dec = initHpackDecoder()
    expect ValueError:
      discard dec.decode("\x3f" & "\xff".repeat(8))  # table-size update, huge int

suite "hpack decode enforces DoS bounds":
  # Encode an HPACK integer with `prefixBits` and the given top-bit flag, so the
  # tests can build size-update / indexed-reference instructions precisely.
  proc hpackInt(value, prefixBits: int, flag: uint8): string =
    let maxPrefix = (1 shl prefixBits) - 1
    if value < maxPrefix:
      result.add char(uint8(value) or flag)
    else:
      result.add char(uint8(maxPrefix) or flag)
      var v = value - maxPrefix
      while v >= 128:
        result.add char(uint8((v and 0x7f) or 0x80))
        v = v shr 7
      result.add char(uint8(v))

  test "rejects a dynamic table size update above the advertised maximum":
    var dec = initHpackDecoder(maxSize = 4096)     # advertised table max
    expect ValueError:                              # RFC 7541 6.3 -> COMPRESSION_ERROR
      discard dec.decode(hpackInt(100_000, 5, 0x20))

  test "accepts a table size update at or below the maximum":
    var dec = initHpackDecoder(maxSize = 4096)
    check dec.decode(hpackInt(2048, 5, 0x20) & "\x82") == @[(":method", "GET")]

  test "rejects an over-budget decoded header list (indexed-reference bomb)":
    var dec = initHpackDecoder(maxList = 4096)      # small decoded-list budget
    # Many 1-byte indexed references to static entry 2 (":method", "GET"): each
    # accounts for ~35 octets, so this blows the 4 KiB budget and must raise
    # rather than expand the seq without limit.
    expect ValueError:
      discard dec.decode(hpackInt(2, 7, 0x80).repeat(1000))

  test "a normal header list stays well under the default budget":
    var dec = initHpackDecoder()
    check dec.decode("\x82\x86\x84").len == 3      # :method GET, :scheme http, :path /
