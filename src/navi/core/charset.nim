## Decode a response body to a UTF-8 string using its declared charset.
##
## Pure Nim (no iconv/FFI) so it runs on every backend, `nim js` included. Covers
## the charsets that actually appear on the wire: UTF-8, US-ASCII, ISO-8859-1 /
## Latin-1, Windows-1252, and UTF-16 (LE/BE, by BOM or label). A byte-order mark
## wins over the declared label (WHATWG); an unknown label falls back to the raw
## bytes, so `text` is always best-effort and never raises.

import std/strutils

proc utf8Encode(cp: int; dst: var string) =
  ## Append code point `cp` as UTF-8.
  if cp <= 0x7F:
    dst.add chr(cp)
  elif cp <= 0x7FF:
    dst.add chr(0xC0 or (cp shr 6))
    dst.add chr(0x80 or (cp and 0x3F))
  elif cp <= 0xFFFF:
    dst.add chr(0xE0 or (cp shr 12))
    dst.add chr(0x80 or ((cp shr 6) and 0x3F))
    dst.add chr(0x80 or (cp and 0x3F))
  else:
    dst.add chr(0xF0 or (cp shr 18))
    dst.add chr(0x80 or ((cp shr 12) and 0x3F))
    dst.add chr(0x80 or ((cp shr 6) and 0x3F))
    dst.add chr(0x80 or (cp and 0x3F))

# Windows-1252 differs from Latin-1 only in 0x80-0x9F. Undefined slots (0x81,
# 0x8D, 0x8F, 0x90, 0x9D) map to the matching C1 control, i.e. the byte value.
const cp1252High: array[0x80..0x9F, int] = [
  0x20AC, 0x81, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021,
  0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0x8D, 0x017D, 0x8F,
  0x90, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
  0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x9D, 0x017E, 0x0178]

proc decodeSingleByte(body: string; cp1252: bool): string =
  result = newStringOfCap(body.len)
  for ch in body:
    let b = ord(ch)
    let cp = if cp1252 and b in 0x80..0x9F: cp1252High[b] else: b
    utf8Encode(cp, result)

proc decodeUtf16(body: string; le: bool; start: int): string =
  result = newStringOfCap(body.len)
  var i = start
  while i + 1 < body.len:
    let unit =
      if le: ord(body[i]) or (ord(body[i+1]) shl 8)
      else: (ord(body[i]) shl 8) or ord(body[i+1])
    i += 2
    if unit in 0xD800..0xDBFF and i + 1 < body.len:      # high surrogate
      let lo =
        if le: ord(body[i]) or (ord(body[i+1]) shl 8)
        else: (ord(body[i]) shl 8) or ord(body[i+1])
      if lo in 0xDC00..0xDFFF:
        i += 2
        utf8Encode(0x10000 + ((unit - 0xD800) shl 10) + (lo - 0xDC00), result)
        continue
    utf8Encode(unit, result)

proc charsetOf(contentType: string): string =
  ## The lowercased `charset` parameter of a Content-Type, or "" if absent.
  let lower = contentType.toLowerAscii
  let idx = lower.find("charset=")
  if idx < 0: return ""
  var v = contentType[idx + len("charset=") .. ^1].strip()
  var e = 0
  while e < v.len and v[e] notin {';', ' ', '\t'}: inc e
  v[0 ..< e].strip(chars = {'"', '\'', ' '}).toLowerAscii

proc hasBom(body, bom: string): bool =
  body.len >= bom.len and body[0 ..< bom.len] == bom

proc decodeText*(body, contentType: string): string =
  ## Decode `body` to UTF-8 using a leading BOM if present, else the Content-Type
  ## charset, else UTF-8. Unknown labels return `body` unchanged (best-effort).
  if hasBom(body, "\xEF\xBB\xBF"): return body[3 .. ^1]     # UTF-8 BOM
  if hasBom(body, "\xFF\xFE"): return decodeUtf16(body, le = true, start = 2)
  if hasBom(body, "\xFE\xFF"): return decodeUtf16(body, le = false, start = 2)
  case charsetOf(contentType).replace("_", "-")
  of "", "utf-8", "utf8", "us-ascii", "ascii": body
  of "iso-8859-1", "latin1", "latin-1", "l1", "iso8859-1", "cp819": decodeSingleByte(body, cp1252 = false)
  of "windows-1252", "cp1252", "windows1252": decodeSingleByte(body, cp1252 = true)
  of "utf-16", "utf16", "utf-16le", "utf16le": decodeUtf16(body, le = true, start = 0)
  of "utf-16be", "utf16be": decodeUtf16(body, le = false, start = 0)
  else: body
