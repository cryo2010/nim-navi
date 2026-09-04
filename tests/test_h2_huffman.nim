## HPACK Huffman tests, validated against RFC 7541 Appendix C.4/C.6 vectors.

import unittest
import std/strutils
import navi/proto/h2/huffman

proc hex(s: string): string =
  for i in countup(0, s.len - 2, 2):
    result.add char(parseHexInt(s[i .. i + 1]))

suite "huffman decode (RFC 7541 vectors)":
  test "the Huffman decoder should decode www.example.com (C.4.1)":
    check huffmanDecode(hex("f1e3c2e5f23a6ba0ab90f4ff")) == "www.example.com"

  test "the Huffman decoder should decode no-cache (C.4.2)":
    check huffmanDecode(hex("a8eb10649cbf")) == "no-cache"

  test "the Huffman decoder should decode custom-key and custom-value (C.4.3)":
    check huffmanDecode(hex("25a849e95ba97d7f")) == "custom-key"
    check huffmanDecode(hex("25a849e95bb8e8b4bf")) == "custom-value"

  test "the Huffman decoder should decode a date header value (C.6.1)":
    check huffmanDecode(hex("d07abe941054d444a8200595040b8166e082a62d1bff")) ==
      "Mon, 21 Oct 2013 20:13:21 GMT"

suite "huffman encode":
  test "the Huffman encoder should encode www.example.com (C.4.1)":
    check huffmanEncode("www.example.com") == hex("f1e3c2e5f23a6ba0ab90f4ff")

  test "the Huffman codec should round-trip every byte value":
    for b in 0 .. 255:
      let s = $char(b)
      check huffmanDecode(huffmanEncode(s)) == s

  test "the Huffman codec should round-trip mixed ASCII":
    let s = "GET /path?x=1&y=2 HTTP/2 Bearer.Token_09"
    check huffmanDecode(huffmanEncode(s)) == s

suite "huffman decode validation (RFC 7541 5.2)":
  test "over-long padding (a whole extra byte) is rejected":
    # A complete value plus a full extra 0xFF byte is > 7 bits of padding.
    expect ValueError:
      discard huffmanDecode(hex("f1e3c2e5f23a6ba0ab90f4ff") & "\xff")

  test "an all-ones stream (EOS symbol / invalid padding) is rejected":
    expect ValueError:
      discard huffmanDecode("\xff\xff\xff\xff")
