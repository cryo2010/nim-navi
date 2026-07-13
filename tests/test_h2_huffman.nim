## HPACK Huffman tests, validated against RFC 7541 Appendix C.4/C.6 vectors.

import unittest
import std/strutils
import navi/proto/h2/huffman

proc hex(s: string): string =
  for i in countup(0, s.len - 2, 2):
    result.add char(parseHexInt(s[i .. i + 1]))

suite "huffman decode (RFC 7541 vectors)":
  test "C.4.1 decodes www.example.com":
    check huffmanDecode(hex("f1e3c2e5f23a6ba0ab90f4ff")) == "www.example.com"

  test "C.4.2 decodes no-cache":
    check huffmanDecode(hex("a8eb10649cbf")) == "no-cache"

  test "C.4.3 decodes custom-key and custom-value":
    check huffmanDecode(hex("25a849e95ba97d7f")) == "custom-key"
    check huffmanDecode(hex("25a849e95bb8e8b4bf")) == "custom-value"

  test "C.6.1 decodes a date header value":
    check huffmanDecode(hex("d07abe941054d444a8200595040b8166e082a62d1bff")) ==
      "Mon, 21 Oct 2013 20:13:21 GMT"

suite "huffman encode":
  test "C.4.1 encodes www.example.com":
    check huffmanEncode("www.example.com") == hex("f1e3c2e5f23a6ba0ab90f4ff")

  test "round-trips every byte value":
    for b in 0 .. 255:
      let s = $char(b)
      check huffmanDecode(huffmanEncode(s)) == s

  test "round-trips mixed ASCII":
    let s = "GET /path?x=1&y=2 HTTP/2 Bearer.Token_09"
    check huffmanDecode(huffmanEncode(s)) == s
