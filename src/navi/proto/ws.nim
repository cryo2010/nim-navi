## Sans-io WebSocket (RFC 6455): the frame codec, the opening-handshake key
## helpers, and an incremental frame decoder. No sockets here -- a backend does
## the HTTP/1.1 Upgrade over its transport, then pumps bytes through this codec.
##
## Client frames MUST be masked (RFC 6455 5.3); server frames MUST NOT be. The
## decoder handles both directions, so the same core drives a client and (in
## tests) a server.

import std/[strutils, base64]
import checksums/sha1
import ../core/[url, headers]

# The masking key (RFC 6455 5.3) and the handshake nonce (4.1) must come from a
# strong entropy source, so draw them from the OS CSPRNG via std/sysrand. sysrand
# has no JavaScript target, but this module is native-only (the js backend uses the
# runtime's WebSocket); the `js` branch is a dead-path fallback that only keeps a
# `nim js` build compiling.
when defined(js):
  import std/random
  var jsRng = initRand(0x6a09e667)
  proc randomBytes(n: int): string =
    result = newString(n)
    for i in 0 ..< n: result[i] = char(jsRng.rand(255))
else:
  import std/sysrand
  proc randomBytes(n: int): string =
    ## `n` cryptographically-random bytes from the OS CSPRNG.
    result = newString(n)
    if n > 0 and not urandom(result.toOpenArrayByte(0, n - 1)):
      raise newException(IOError, "navi: failed to read OS entropy for WebSocket")

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
  closeMessageTooBig* = 1009'u16   ## RFC 6455 7.4.1: a message exceeded a size limit
  maxFramePayload* = 64 * 1024 * 1024
    ## Reject a single incoming frame larger than this (64 MiB). A 64-bit length
    ## with its high bit set (RFC 6455 5.2 forbids it) would otherwise become a
    ## negative `int` that slips past the bounds check and crashes `newString`.

# --- opening handshake ---

proc genKey*(): string =
  ## A fresh random 16-byte Sec-WebSocket-Key, base64-encoded (RFC 6455 4.1).
  base64.encode(randomBytes(16))

proc acceptFor*(key: string): string =
  ## The Sec-WebSocket-Accept value for `key`: base64(SHA1(key + GUID)). Used by
  ## a server to answer and by the client to validate the 101 response.
  let digest = Sha1Digest(secureHash(key & wsGuid))
  var raw = newString(digest.len)
  for i in 0 ..< digest.len: raw[i] = char(digest[i])
  base64.encode(raw)

# --- frame codec ---

proc applyMask(dst: var string, dstStart: int, src: string, srcStart, n: int,
               key: array[4, byte]) =
  ## dst[dstStart+i] = src[srcStart+i] xor key[i mod 4] for i in 0..<n, eight bytes
  ## at a time (RFC 6455 masking is per-byte; masking every payload byte of every
  ## client frame is the hot path). The 4-byte key is repeated into a uint64 and
  ## XORed word-wise; because each chunk starts at a multiple of 8 (a multiple of the
  ## 4-byte key period) the alignment holds, and copyMem keeps byte order so the
  ## result is endianness-independent. A scalar tail finishes the last <8 bytes.
  if n <= 0: return
  var mask8: array[8, byte]
  for i in 0 ..< 8: mask8[i] = key[i and 3]
  var m64: uint64
  copyMem(addr m64, addr mask8[0], 8)
  var i = 0
  while i + 8 <= n:
    var w: uint64
    copyMem(addr w, unsafeAddr src[srcStart + i], 8)
    w = w xor m64
    copyMem(addr dst[dstStart + i], addr w, 8)
    i += 8
  while i < n:
    dst[dstStart + i] = char(byte(src[srcStart + i]) xor key[i and 3])
    inc i

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
    var keyStr = maskKey
    if keyStr.len != 4:
      keyStr = randomBytes(4)
    result.add keyStr
    var key: array[4, byte]
    for i in 0 ..< 4: key[i] = byte(keyStr[i])
    let off = result.len
    result.setLen(off + n)
    applyMask(result, off, payload, 0, n, key)
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
  # A negative length (64-bit high bit set) or an oversized one must fail the
  # connection, not reach `newString(length)` (a RangeDefect / huge allocation).
  if length < 0 or length > maxFramePayload:
    raise newException(ValueError,
      "navi: WebSocket frame length is invalid or exceeds the " &
      $maxFramePayload & "-byte limit")
  var key: array[4, byte]
  if masked:
    if d.buf.len < pos + 4: return false
    for i in 0 ..< 4: key[i] = byte(d.buf[pos + i])
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
  if masked:
    applyMask(f.payload, 0, d.buf, pos, length, key)
  elif length > 0:
    copyMem(addr f.payload[0], unsafeAddr d.buf[pos], length)
  d.buf.delete(0 ..< pos + length)
  true

proc closePayload*(code: uint16, reason = ""): string =
  ## The 2-byte big-endian code followed by an optional UTF-8 reason.
  result = newString(2)
  result[0] = char((code shr 8) and 0xFF)
  result[1] = char(code and 0xFF)
  result.add reason

# --- message assembly + handshake helpers (pure; shared by every backend) ---

type
  WsMessageKind* = enum wmText, wmBinary, wmClose
  WsMessage* = object
    ## A received WebSocket message. `data` is the payload for text/binary and
    ## the (optional) reason for a close; `closeCode` is set for `wmClose`.
    kind*: WsMessageKind
    data*: string
    closeCode*: uint16

  WsReply* = enum wrNone, wrPong, wrCloseEcho   ## control frame the caller must send
  WsOutcome* = object
    ready*: bool             ## `message` is a complete message (or close)
    message*: WsMessage
    reply*: WsReply          ## a control frame to send back, with `replyPayload`
    replyPayload*: string

  WsAssembler* = object      ## reassembles fragmented messages across frames
    kind: WsMessageKind
    buf: string

  WsMessageTooLarge* = object of CatchableError
    ## Raised by `offer` when a reassembled message would exceed the caller's
    ## `maxMessageBytes`. The caller should close with `closeMessageTooBig` (1009).

proc offer*(a: var WsAssembler, f: Frame, maxMessageBytes = 0): WsOutcome =
  ## Feed one decoded frame. Handles fragmentation (text/binary + continuation)
  ## and the control frames: a ping asks for a pong, a close both yields a
  ## `wmClose` message and asks for a close echo. No I/O -- the caller sends any
  ## `reply` and surfaces `message` when `ready`.
  ##
  ## `maxMessageBytes` (0 = unlimited) bounds a *reassembled* message across its
  ## fragments: the per-frame length cap does not, so without it a peer can grow the
  ## buffer without limit via a stream of continuation frames. When the next frame
  ## would push the message past the cap, `offer` raises `WsMessageTooLarge` before
  ## buffering it.
  template guardSize(add: int) =
    if maxMessageBytes > 0 and a.buf.len + add > maxMessageBytes:
      raise newException(WsMessageTooLarge,
        "navi: WebSocket message exceeds maxMessageBytes (" & $maxMessageBytes & ")")
  case f.opcode
  of opPing:
    result.reply = wrPong
    result.replyPayload = f.payload
  of opPong:
    discard
  of opClose:
    var code = closeNormal
    if f.payload.len >= 2:
      code = uint16((ord(f.payload[0]) shl 8) or ord(f.payload[1]))
    result.reply = wrCloseEcho
    result.replyPayload = f.payload
    result.ready = true
    result.message = WsMessage(kind: wmClose, closeCode: code,
      data: if f.payload.len > 2: f.payload[2 .. ^1] else: "")
  of opText, opBinary:
    a.buf.setLen(0)                 # a new message starts here; drop any stale partial
    guardSize(f.payload.len)
    a.kind = if f.opcode == opText: wmText else: wmBinary
    a.buf = f.payload
    if f.fin:
      result.ready = true
      result.message = WsMessage(kind: a.kind, data: a.buf)
  of opContinuation:
    guardSize(f.payload.len)
    a.buf.add f.payload
    if f.fin:
      result.ready = true
      result.message = WsMessage(kind: a.kind, data: a.buf)

proc hostHeader(u: Url): string =
  result = u.host
  let p = u.port
  if not ((u.isTls and p == 443) or (not u.isTls and p == 80)):
    result.add(":" & $p)

proc upgradeRequest*(u: Url, key: string, extra: Headers): string =
  ## The client's HTTP/1.1 Upgrade request for `u` with Sec-WebSocket-Key `key`.
  result = "GET " & u.requestTarget & " HTTP/1.1\r\n" &
           "Host: " & hostHeader(u) & "\r\n" &
           "Upgrade: websocket\r\nConnection: Upgrade\r\n" &
           "Sec-WebSocket-Key: " & key & "\r\n" &
           "Sec-WebSocket-Version: " & wsVersion & "\r\n"
  for (k, v) in extra.pairs: result.add(k & ": " & v & "\r\n")
  result.add("\r\n")

proc validate101*(responseHead, key: string): bool =
  ## True when `responseHead` (the status line + headers) is a 101 whose
  ## Sec-WebSocket-Accept matches `key`.
  let lines = responseHead.splitLines
  if lines.len == 0 or not lines[0].startsWith("HTTP/1.1 101"): return false
  for line in lines[1 .. ^1]:
    let c = line.find(':')
    if c > 0 and cmpIgnoreCase(line[0 ..< c].strip, "sec-websocket-accept") == 0:
      return line[c + 1 .. ^1].strip == acceptFor(key)
  false
