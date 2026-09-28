## navi/proto/ws under the JavaScript backend (the name does not match
## checkmate's `t*.nim`, so the native unit runner skips it; CI compiles this
## with `nim js` and runs it under Node).
##
## navi/js does not use proto/ws at runtime -- the browser or Node does the
## handshake and framing inside its own `WebSocket` -- but the codec is portable
## Nim and js is navi's only 32-bit-`int` target, so the frame-length guards get
## checked here on a target where they can actually overflow. The unit suite runs
## on 64-bit hosts, where the #285 case cannot truncate at all.
import std/[base64, strutils]
import navi/proto/ws

doAssert int.high == 2147483647, "js `int` should be 32 bits; the length guards need that"

# --- opening handshake: the pure-Nim SHA-1 and the Web Crypto nonce ---
doAssert acceptFor("dGhlIHNhbXBsZSBub25jZQ==") == "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
# Two more accepts, computed by the native build (OpenSSL EVP / checksums): an
# empty key, and one long enough that the hash spans two SHA-1 blocks.
doAssert acceptFor("") == "Kfh9QIsMVZcl6xEPYxPHzW8SZ8w="
doAssert acceptFor(repeat("A", 100)) == "WXqNWiGhe0+utGv47+HlLjR7KVw="
doAssert base64.decode(genKey()).len == 16
doAssert genKey() != genKey()

# --- frame codec ---
proc roundtrip(opcode: Opcode, payload: string, masked: bool): Frame =
  var d: WsDecoder
  d.feed(encodeFrame(opcode, payload, masked = masked))
  doAssert d.next(result), "a complete frame should decode"

# Masked: 8 aligned bytes plus a 3-byte tail, so both halves of applyMask run.
let long = "the quick brown fox jumps over the lazy dog"
doAssert long.len mod 8 == 3
let m = roundtrip(opText, long, true)
doAssert m.opcode == opText and m.fin and m.masked and m.payload == long

# Unmasked (a server frame): the byte-copy path js takes instead of copyMem.
let u = roundtrip(opBinary, long, false)
doAssert u.opcode == opBinary and not u.masked and u.payload == long

# The 126 extended-length path, masked and not.
let big = repeat("x", 4096)
doAssert roundtrip(opBinary, big, true).payload == big
doAssert roundtrip(opBinary, big, false).payload == big

# A frame arriving in pieces stays buffered until the payload is complete.
block:
  let wire = encodeFrame(opText, "hello", masked = true)
  var d: WsDecoder
  var f: Frame
  d.feed(wire[0 ..< wire.len - 2])
  doAssert not d.next(f)
  d.feed(wire[wire.len - 2 .. ^1])
  doAssert d.next(f) and f.payload == "hello"

# A burst of small frames out of one feed, then a split feed: the read cursor and
# `feed`'s compaction (#409) have their own scalar path on js (no moveMem), so the
# order, the mask handling and the retained-buffer bound get checked here too.
block:
  var wire = ""
  for i in 0 ..< 500:
    wire.add encodeFrame(opText, "m" & $i & repeat(".", 30), masked = (i mod 2 == 0))
  var d: WsDecoder
  var f: Frame
  # Hold back the last three bytes so a partial frame is carried across the feed
  # and the in-place compaction branch (prefix over 8 KiB) runs.
  d.feed(wire[0 ..< wire.len - 3])
  var got = 0
  while d.next(f):
    doAssert f.payload == "m" & $got & repeat(".", 30), "frame " & $got & " out of order"
    inc got
  doAssert got == 499, "499 whole frames should decode, got " & $got
  d.feed(wire[wire.len - 3 .. ^1])
  doAssert d.next(f) and f.payload == "m499" & repeat(".", 30)
  doAssert not d.next(f)
  doAssert d.buffered == 0
  doAssert d.bufferLen <= 2 * wire.len, "the buffer should be compacted, not grown"

# Every split point of a small two-frame wire still decodes both frames.
block:
  let wire = encodeFrame(opText, "hello", masked = true) &
             encodeFrame(opBinary, "world!", masked = false)
  for cut in 0 .. wire.len:
    var d: WsDecoder
    var f: Frame
    var got: seq[string] = @[]
    d.feed(wire[0 ..< cut])
    while d.next(f): got.add f.payload
    d.feed(wire[cut .. ^1])
    while d.next(f): got.add f.payload
    doAssert got == @["hello", "world!"], "split at " & $cut & " decoded " & $got

proc decodeError(wire: string): string =
  var d: WsDecoder
  var f: Frame
  d.feed(wire)
  try:
    discard d.next(f)
  except ValueError as e:
    return e.msg
  ""

# #285 on a real 32-bit target: 0x0000_0001_0000_0005 truncates to 5 when the
# eight length bytes are shifted into a 32-bit `int`, which would let a 4 GiB
# frame through as a 5-byte one. The uint64 accumulator must reject it.
doAssert "invalid or exceeds" in
  decodeError("\x82\x7f\x00\x00\x00\x01\x00\x00\x00\x05" & "hello")
# A 64-bit length with the high bit set (RFC 6455 5.2 forbids it).
doAssert "invalid or exceeds" in
  decodeError("\x82\x7f\x80\x00\x00\x00\x00\x00\x00\x00")
# RSV bits and reserved opcodes still fail the connection.
doAssert "reserved bit" in decodeError("\xc2\x00")
doAssert "reserved WebSocket opcode" in decodeError("\x83\x00")

# --- message assembly ---
block:
  var a: WsAssembler
  doAssert not a.offer(Frame(fin: false, opcode: opText, payload: "he")).ready
  let o = a.offer(Frame(fin: true, opcode: opContinuation, payload: "llo"))
  doAssert o.ready and o.message.kind == wmText and o.message.data == "hello"
  let p = a.offer(Frame(fin: true, opcode: opPing, payload: "hi"))
  doAssert p.reply == wrPong and p.replyPayload == "hi"
  let c = a.offer(Frame(fin: true, opcode: opClose,
                        payload: closePayload(closeNormal, "bye")))
  doAssert c.ready and c.message.closeCode == closeNormal and c.message.data == "bye"

echo "navi/proto/ws js codec: ok"
