## Sans-io WebSocket (RFC 6455): the frame codec, the opening-handshake key
## helpers, and an incremental frame decoder. No sockets here -- a backend does
## the HTTP/1.1 Upgrade over its transport, then pumps bytes through this codec.
##
## Client frames MUST be masked (RFC 6455 5.3); server frames MUST NOT be. The
## decoder handles both directions, so the same core drives a client and (in
## tests) a server.

import std/[strutils, base64, random, times]
import checksums/sha1

type
  Opcode* = enum
    opContinuation = 0x0
    opText = 0x1
    opBinary = 0x2
    opClose = 0x8
    opPing = 0x9
    opPong = 0xA

  Frame* = object
    fin*: bool
    opcode*: Opcode
    payload*: string

  WsDecoder* = object
    buf: string

const
  wsGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"  ## RFC 6455 handshake magic
  wsVersion* = "13"
  # Close codes (RFC 6455 7.4.1); the common ones.
  closeNormal* = 1000'u16
  closeGoingAway* = 1001'u16
  closeProtocolError* = 1002'u16

var rng = initRand(getTime().toUnix xor getTime().nanosecond)

# --- opening handshake ---

proc genKey*(): string =
  ## A fresh random 16-byte Sec-WebSocket-Key, base64-encoded (RFC 6455 4.1).
  var raw = newString(16)
  for i in 0 ..< 16: raw[i] = char(rng.rand(255))
  base64.encode(raw)

proc acceptFor*(key: string): string =
  ## The Sec-WebSocket-Accept value for `key`: base64(SHA1(key + GUID)). Used by
  ## a server to answer and by the client to validate the 101 response.
  let digest = Sha1Digest(secureHash(key & wsGuid))
  var raw = newString(digest.len)
  for i in 0 ..< digest.len: raw[i] = char(digest[i])
  base64.encode(raw)

# --- frame codec ---

proc encodeFrame*(opcode: Opcode, payload: string, masked = true,
                  maskKey = "", fin = true): string =
  ## Serialize one frame. Client callers keep `masked` true; `maskKey` (4 bytes)
  ## is generated when empty -- pass a fixed one only for deterministic tests.
  ## `fin = false` marks a non-final fragment (continued by more frames).
  result = newStringOfCap(payload.len + 14)
  result.add char((if fin: 0x80 else: 0) or ord(opcode))   # FIN + opcode; RSV clear
  let n = payload.len
  let maskBit = if masked: 0x80 else: 0
  if n < 126:
    result.add char(maskBit or n)
  elif n <= 0xFFFF:
    result.add char(maskBit or 126)
    result.add char((n shr 8) and 0xFF)
    result.add char(n and 0xFF)
  else:
    result.add char(maskBit or 127)
    for shift in countdown(56, 0, 8):
      result.add char((n shr shift) and 0xFF)
  if masked:
    var key = maskKey
    if key.len != 4:
      key = newString(4)
      for i in 0 ..< 4: key[i] = char(rng.rand(255))
    result.add key
    for i in 0 ..< n:
      result.add char(ord(payload[i]) xor ord(key[i mod 4]))
  else:
    result.add payload

proc feed*(d: var WsDecoder, data: string) =
  d.buf.add data

proc next*(d: var WsDecoder, f: var Frame): bool =
  ## Pop one complete frame from the buffer, unmasking if needed. Returns false
  ## when more bytes are required.
  if d.buf.len < 2: return false
  let b0 = ord(d.buf[0])
  let b1 = ord(d.buf[1])
  let masked = (b1 and 0x80) != 0
  var length = b1 and 0x7F
  var pos = 2
  if length == 126:
    if d.buf.len < 4: return false
    length = (ord(d.buf[2]) shl 8) or ord(d.buf[3])
    pos = 4
  elif length == 127:
    if d.buf.len < 10: return false
    length = 0
    for i in 2 ..< 10: length = (length shl 8) or ord(d.buf[i])
    pos = 10
  var key: array[4, int]
  if masked:
    if d.buf.len < pos + 4: return false
    for i in 0 ..< 4: key[i] = ord(d.buf[pos + i])
    pos += 4
  if d.buf.len < pos + length: return false     # payload not fully arrived
  f.fin = (b0 and 0x80) != 0
  # Map the 4-bit opcode explicitly: Opcode has holes (0x3-0x7 and 0xB-0xF are
  # reserved), so a blind int-to-enum conversion is unsafe. A reserved opcode
  # must fail the connection (RFC 6455 5.2).
  f.opcode = case b0 and 0x0F
    of 0x0: opContinuation
    of 0x1: opText
    of 0x2: opBinary
    of 0x8: opClose
    of 0x9: opPing
    of 0xA: opPong
    else: raise newException(ValueError,
      "navi: reserved WebSocket opcode 0x" & toHex(b0 and 0x0F, 1))
  f.payload = newString(length)
  for i in 0 ..< length:
    let b = ord(d.buf[pos + i])
    f.payload[i] = char(if masked: b xor key[i mod 4] else: b)
  d.buf.delete(0 ..< pos + length)
  true

proc closePayload*(code: uint16, reason = ""): string =
  ## The 2-byte big-endian code followed by an optional UTF-8 reason.
  result = newString(2)
  result[0] = char((code shr 8) and 0xFF)
  result[1] = char(code and 0xFF)
  result.add reason
