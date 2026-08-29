## Response.text charset decoding.
import unittest
import navi/core/[headers, response]

proc resp(body, contentType: string): Response =
  var h = initHeaders()
  if contentType.len > 0: h.add("content-type", contentType)
  initResponse(200, "OK", "HTTP/1.1", h, body)

suite "Response.text charset decoding":
  test "a UTF-8 body should pass through unchanged":
    check resp("caf\xC3\xA9", "text/plain; charset=utf-8").text == "caf\xC3\xA9"

  test "a body with no charset should default to UTF-8":
    check resp("hello", "text/plain").text == "hello"

  test "an ISO-8859-1 body should be decoded to UTF-8":
    # 0xE9 is 'é' in Latin-1 -> U+00E9 -> UTF-8 0xC3 0xA9
    check resp("caf\xE9", "text/plain; charset=iso-8859-1").text == "caf\xC3\xA9"

  test "a Windows-1252 body should decode the 0x80-0x9F range":
    # 0x80 is the euro sign in cp1252 -> U+20AC -> UTF-8 0xE2 0x82 0xAC
    check resp("\x80", "text/html; charset=windows-1252").text == "\xE2\x82\xAC"

  test "a quoted charset label should be honored":
    check resp("caf\xE9", "text/plain; charset=\"latin-1\"").text == "caf\xC3\xA9"

  test "a UTF-8 BOM should be stripped":
    check resp("\xEF\xBB\xBFhi", "text/plain").text == "hi"

  test "a UTF-16LE body with BOM should be decoded":
    check resp("\xFF\xFEh\x00i\x00", "text/plain").text == "hi"

  test "a UTF-16BE body with BOM should be decoded":
    check resp("\xFE\xFF\x00h\x00i", "text/plain").text == "hi"

  test "a UTF-16 label without BOM should be decoded as little-endian":
    check resp("h\x00i\x00", "text/plain; charset=utf-16").text == "hi"

  test "a UTF-16 surrogate pair should decode to an astral code point":
    # U+1F600 (grinning face) as UTF-16LE: D83D DE00
    check resp("\x3D\xD8\x00\xDE", "text/plain; charset=utf-16le").text == "\xF0\x9F\x98\x80"

  test "an unknown charset should fall back to the raw bytes":
    check resp("caf\xE9", "text/plain; charset=shift_jis").text == "caf\xE9"

  test "a BOM should win over a conflicting charset label":
    check resp("\xFF\xFEh\x00i\x00", "text/plain; charset=iso-8859-1").text == "hi"
