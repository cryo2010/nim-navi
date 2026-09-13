## A thin, allocation-light builder for wire-format byte strings.
##
## Wraps a `string` (the byte buffer every native backend already sends), with
## helpers for the big-endian integer widths and length-prefixed fields that the
## SOCKS5 and HTTP/2 framers hand-assemble. Each `add*` is just `string.add`
## calls, so a ByteWriter costs exactly what the open-coded `.add char(...)`
## sequences it replaces cost -- no seq<->string conversions, no per-byte heap
## churn beyond a growable string's own amortized growth. No navi imports, so
## both `core/socks` and `proto/h2/frame` can use it without an import cycle.

type ByteWriter* = object
  buf*: string

proc initByteWriter*(cap = 0): ByteWriter =
  ## A writer with `cap` bytes reserved (pass the known frame size to avoid regrowth).
  ByteWriter(buf: newStringOfCap(cap))

proc len*(w: ByteWriter): int {.inline.} = w.buf.len

proc addByte*(w: var ByteWriter, b: uint8) {.inline.} =
  w.buf.add char(b)

proc addU16BE*(w: var ByteWriter, n: uint16) {.inline.} =
  w.buf.add char((n shr 8) and 0xff)
  w.buf.add char(n and 0xff)

proc addU24BE*(w: var ByteWriter, n: int) {.inline.} =
  w.buf.add char((n shr 16) and 0xff)
  w.buf.add char((n shr 8) and 0xff)
  w.buf.add char(n and 0xff)

proc addU32BE*(w: var ByteWriter, n: uint32) {.inline.} =
  w.buf.add char((n shr 24) and 0xff)
  w.buf.add char((n shr 16) and 0xff)
  w.buf.add char((n shr 8) and 0xff)
  w.buf.add char(n and 0xff)

proc u32BE*(n: uint32): string =
  ## The 4-byte big-endian encoding of `n` as a standalone string, for the
  ## single-integer HTTP/2 payloads (WINDOW_UPDATE, RST_STREAM, GOAWAY).
  var w = initByteWriter(4)
  w.addU32BE(n)
  w.buf

proc add*(w: var ByteWriter, s: string) {.inline.} =
  ## Append raw bytes (payload, host name, credentials) unchanged.
  w.buf.add s

proc addLenPrefixed*(w: var ByteWriter, s: string) {.inline.} =
  ## A single-byte length followed by `s` (SOCKS5 domain name and credentials).
  ## The caller must have already bounded `s.len` to 255.
  w.buf.add char(s.len and 0xff)
  w.buf.add s
