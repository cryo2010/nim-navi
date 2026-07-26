## Fuzz target: the HTTP/2 connection state machine (H2Conn), structure-aware.
##
## fuzz_frame checks the byte-level frame decoder survives arbitrary bytes. But
## the padding / interim-1xx / trailers / flow-control bugs lived in H2Conn's
## *semantics*, not the decoder -- it parsed the frames fine and then mishandled
## them. This target catches that class: it reads the fuzz input as a script to
## build a VALID server response on one stream, randomizing
##
##   - DATA padding and HEADERS padding (RFC 9113 6.1/6.2)
##   - a HEADERS PRIORITY prefix
##   - 0..2 interim 1xx HEADERS blocks before the final response
##   - trailers (a HEADERS block after DATA)
##   - how the body is chunked into DATA frames
##   - how the serialized bytes are split across feed() calls
##
## then asserts H2Conn reassembles the exact status, headers, and body. Any
## mismatch (padding leaking in, an interim block polluting the final headers, a
## trailer surfacing) is a finding. All generated frames are well-formed, so
## feeding must not raise and the stream must complete. Stream resets are out of
## scope here (they abort rather than reassemble; covered by unit tests).

import std/strutils
import navi/proto/h2/[conn, frame, hpack]
include ./fuzzlib

type Cursor = object
  data: string
  pos: int

proc nextByte(c: var Cursor): int =
  ## Next input byte, or 0 once exhausted, so even a short input builds a
  ## complete, valid sequence (libFuzzer grows it to reach new structure).
  if c.pos < c.data.len:
    result = int(uint8(c.data[c.pos])); inc c.pos

proc chance(c: var Cursor, oneIn: int): bool = (c.nextByte mod oneIn) == 0

proc padded(typ: FrameType, flags: uint8, id: uint32, payload: string,
            pad: int): string =
  ## A frame with `pad` bytes of padding: [pad length][payload][zero padding].
  encodeFrame(typ, flags or flagPadded, id, char(pad) & payload & repeat('\0', pad))

const priorityPrefix = "\x00\x00\x00\x00\x00"   # stream dependency (4) + weight (1)

fuzzMain:
  var cur = Cursor(data: input)
  let c = initH2Conn()
  discard c.feed(encodeSettings([]))            # consume the server preface
  let id = c.openStream()
  let enc = HpackEncoder()                       # one coherent server HPACK stream
  var wire = ""

  # 0..2 interim 1xx blocks (each optionally padded); must be dropped.
  for _ in 0 ..< (cur.nextByte mod 3):
    let blk = enc.encode(@[(":status", "103"), ("x-hint", "/a.css")])
    if cur.chance(2):
      wire.add padded(ftHeaders, flagEndHeaders, id, blk, 1 + cur.nextByte mod 32)
    else:
      wire.add encodeHeaders(id, blk, endStream = false, endHeaders = true)

  # Final response: a >= 200 status and 0..3 regular headers.
  const statuses = ["200", "201", "204", "404", "500"]
  let status = statuses[cur.nextByte mod statuses.len]
  var finalHeaders: seq[(string, string)]
  for i in 0 ..< (cur.nextByte mod 4):
    finalHeaders.add(("x-h" & $i, "v" & $cur.nextByte))
  let headerBlock = enc.encode(@[(":status", status)] & finalHeaders)

  # Body: up to ~2 KiB of deterministic bytes.
  let bodyLen = (cur.nextByte shl 3) or (cur.nextByte and 7)
  var body = newString(bodyLen)
  for i in 0 ..< bodyLen: body[i] = char((i * 31 + 7) and 0xff)

  let hasTrailers = bodyLen > 0 and cur.chance(3)
  let endOnHeaders = bodyLen == 0 and not hasTrailers

  # Final HEADERS frame, optionally with a PRIORITY prefix and/or padding.
  var hflags = flagEndHeaders or (if endOnHeaders: flagEndStream else: 0'u8)
  var hpayload = headerBlock
  if cur.chance(2):
    hpayload = priorityPrefix & headerBlock
    hflags = hflags or flagPriority
  if cur.chance(2):
    wire.add padded(ftHeaders, hflags, id, hpayload, 1 + cur.nextByte mod 64)
  else:
    wire.add encodeFrame(ftHeaders, hflags, id, hpayload)

  # Body split into random DATA frames, each optionally padded.
  var off = 0
  while off < bodyLen:
    let take = min(bodyLen - off, 1 + (cur.nextByte * 7) mod 700)
    let chunk = body[off ..< off + take]
    off += take
    let dflags = if off >= bodyLen and not hasTrailers: flagEndStream else: 0'u8
    if cur.chance(2):
      wire.add padded(ftData, dflags, id, chunk, cur.nextByte mod 32)
    else:
      wire.add encodeFrame(ftData, dflags, id, chunk)

  # Optional trailers: a final HEADERS block after DATA (END_STREAM). Dropped.
  if hasTrailers:
    wire.add encodeFrame(ftHeaders, flagEndHeaders or flagEndStream, id,
                         enc.encode(@[("x-trailer", "t")]))

  # The sequence above is valid, so feeding must not raise and the reassembly must
  # match. Any CatchableError here is a finding -- but under libFuzzer an escaping
  # CatchableError does NOT abort the process (it unwinds silently across the C
  # callback), so convert it to a fatal assertion. A failed reassembly doAssert
  # raises a Defect, which aborts on its own and is not caught here.
  try:
    var i = 0
    while i < wire.len:
      let n = min(wire.len - i, 1 + (cur.nextByte * 5) mod 4096)
      discard c.feed(wire[i ..< i + n])   # input-driven slices: split-across-feeds
      i += n
    doAssert c.streamDone(id), "stream did not complete"
    let resp = c.takeResponse(id)
    doAssert resp.status == parseInt(status),
      "status " & $resp.status & " != " & status
    doAssert resp.body == body,
      "body mismatch: got " & $resp.body.len & " bytes, want " & $body.len
    doAssert resp.headers == finalHeaders, "final headers mismatch"
  except CatchableError as e:
    doAssert false, "valid frames raised " & $e.name & ": " & e.msg
