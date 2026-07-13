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
