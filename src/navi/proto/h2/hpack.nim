## HPACK header compression (RFC 7541), sans-io.
##
## The encoder is stateless: it indexes the static table where it can and
## otherwise emits literals without indexing (no Huffman on the way out). The
## decoder is stateful, maintaining the dynamic table, and handles every
## representation a server may send. Huffman-coded strings are handled by
## `huffman.nim`.

import std/strutils
import ./huffman

type
  HeaderPair* = (string, string)

  DynamicTable = object
    entries: seq[HeaderPair]  ## most-recent first
    size: int                 ## current size in HPACK octets
    maxSize: int

  HpackDecoder* = object
    dyn: DynamicTable

  HpackEncoder* = object

const staticTable: array[1 .. 61, HeaderPair] = [
  (":authority", ""), (":method", "GET"), (":method", "POST"),
  (":path", "/"), (":path", "/index.html"), (":scheme", "http"),
  (":scheme", "https"), (":status", "200"), (":status", "204"),
  (":status", "206"), (":status", "304"), (":status", "400"),
  (":status", "404"), (":status", "500"), ("accept-charset", ""),
  ("accept-encoding", "gzip, deflate"), ("accept-language", ""),
  ("accept-ranges", ""), ("accept", ""), ("access-control-allow-origin", ""),
  ("age", ""), ("allow", ""), ("authorization", ""), ("cache-control", ""),
  ("content-disposition", ""), ("content-encoding", ""),
  ("content-language", ""), ("content-length", ""), ("content-location", ""),
  ("content-range", ""), ("content-type", ""), ("cookie", ""), ("date", ""),
  ("etag", ""), ("expect", ""), ("expires", ""), ("from", ""), ("host", ""),
  ("if-match", ""), ("if-modified-since", ""), ("if-none-match", ""),
  ("if-range", ""), ("if-unmodified-since", ""), ("last-modified", ""),
  ("link", ""), ("location", ""), ("max-forwards", ""),
  ("proxy-authenticate", ""), ("proxy-authorization", ""), ("range", ""),
  ("referer", ""), ("refresh", ""), ("retry-after", ""), ("server", ""),
  ("set-cookie", ""), ("strict-transport-security", ""),
  ("transfer-encoding", ""), ("user-agent", ""), ("vary", ""), ("via", ""),
  ("www-authenticate", "")]

const entryOverhead = 32  # RFC 7541 section 4.1

proc initHpackDecoder*(maxSize = 4096): HpackDecoder =
  result.dyn.maxSize = maxSize

# --- Integer and string primitives (RFC 7541 sections 5.1-5.2) ---

proc encodeInteger(value, prefixBits: int): string =
  ## Encode `value` in a `prefixBits`-wide prefix; the caller ORs flag bits into
  ## the first byte.
  let maxPrefix = (1 shl prefixBits) - 1
  if value < maxPrefix:
    result.add char(value)
  else:
    result.add char(maxPrefix)
    var v = value - maxPrefix
    while v >= 128:
      result.add char((v and 0x7f) or 0x80)
      v = v shr 7
    result.add char(v)

proc decodeInteger(data: string, i: var int, prefixBits: int): int =
  let maxPrefix = (1 shl prefixBits) - 1
  result = int(uint8(data[i])) and maxPrefix
  inc i
  if result == maxPrefix:
    var shift = 0
    while true:
      let b = int(uint8(data[i])); inc i
      result += (b and 0x7f) shl shift
      shift += 7
      if (b and 0x80) == 0: break

proc encodeString(s: string): string =
  result = encodeInteger(s.len, 7) # H bit (0x80) left 0: not Huffman-coded
  result.add s

proc decodeString(data: string, i: var int): string =
  let huffman = (uint8(data[i]) and 0x80) != 0
  let length = decodeInteger(data, i, 7)
  let raw = data[i ..< i + length]
  i += length
  if huffman: huffmanDecode(raw) else: raw

# --- Dynamic table ---

proc evict(dt: var DynamicTable) =
  while dt.size > dt.maxSize and dt.entries.len > 0:
    let last = dt.entries[^1]
    dt.size -= last[0].len + last[1].len + entryOverhead
    dt.entries.setLen(dt.entries.len - 1)

proc add(dt: var DynamicTable, name, value: string) =
  let entrySize = name.len + value.len + entryOverhead
  if entrySize > dt.maxSize:
    dt.entries.setLen(0)
    dt.size = 0
    return
  dt.entries.insert((name, value), 0)
  dt.size += entrySize
  dt.evict()

proc resize(dt: var DynamicTable, newMax: int) =
  dt.maxSize = newMax
  dt.evict()

proc lookup(dec: HpackDecoder, index: int): HeaderPair =
  if index >= 1 and index <= 61:
    staticTable[index]
  elif index >= 62 and index - 62 < dec.dyn.entries.len:
    dec.dyn.entries[index - 62]
  else:
    raise newException(ValueError, "hpack: invalid table index " & $index)

# --- Decoder (RFC 7541 section 6) ---

proc decodeLiteral(dec: var HpackDecoder, data: string, i: var int,
                   prefixBits: int): HeaderPair =
  let nameIndex = decodeInteger(data, i, prefixBits)
  let name = if nameIndex == 0: decodeString(data, i) else: dec.lookup(nameIndex)[0]
  result = (name, decodeString(data, i))

proc decode*(dec: var HpackDecoder, headerBlock: string): seq[HeaderPair] =
  var i = 0
  while i < headerBlock.len:
    let b = uint8(headerBlock[i])
    if (b and 0x80) != 0:                      # indexed header field
      result.add dec.lookup(decodeInteger(headerBlock, i, 7))
    elif (b and 0x40) != 0:                    # literal, incremental indexing
      let pair = dec.decodeLiteral(headerBlock, i, 6)
      dec.dyn.add(pair[0], pair[1])
      result.add pair
    elif (b and 0x20) != 0:                    # dynamic table size update
      dec.dyn.resize(decodeInteger(headerBlock, i, 5))
    else:                                       # literal, without/never indexed
      result.add dec.decodeLiteral(headerBlock, i, 4)

# --- Encoder (RFC 7541 section 6, static-indexed + literals) ---

proc staticFind(name, value: string): (int, bool) =
  ## Returns (index, exactMatch). index 0 means no name match.
  var nameOnly = 0
  for idx in 1 .. 61:
    if staticTable[idx][0] == name:
      if staticTable[idx][1] == value: return (idx, true)
      if nameOnly == 0: nameOnly = idx
  (nameOnly, false)

proc encode*(enc: HpackEncoder, headers: openArray[HeaderPair]): string =
  for (name, value) in headers:
    let lower = name.toLowerAscii
    let (idx, exact) = staticFind(lower, value)
    if exact:
      var b = encodeInteger(idx, 7)
      b[0] = char(uint8(b[0]) or 0x80)         # indexed header field
      result.add b
    else:
      # literal without indexing (0x00 prefix), name indexed if known
      result.add encodeInteger(idx, 4)          # top 4 bits already 0
      if idx == 0: result.add encodeString(lower)
      result.add encodeString(value)
