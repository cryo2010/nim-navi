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

# SHA-1 for the accept hash: prefer OpenSSL EVP (hardware SHA) when TLS is linked
# and libcrypto loads (Linux), else the pure-Nim `checksums` above (also the `nim
# js` path). One hash per handshake, so this is for consistency, not throughput.
when defined(ssl) and not defined(js):
  import ../backend/evpdigest

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
    masked*: bool      ## whether the received frame was masked (a server frame must not be)

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

proc sha1Bytes(s: string): string =
  ## Raw 20-byte SHA-1 of `s`, from EVP (hardware) or the checksums fallback.
  when defined(ssl) and not defined(js):
    if evpAvailable(): return evpSha1Raw(s)
  let digest = Sha1Digest(secureHash(s))
  result = newString(digest.len)
  for i in 0 ..< digest.len: result[i] = char(digest[i])

proc acceptFor*(key: string): string =
  ## The Sec-WebSocket-Accept value for `key`: base64(SHA1(key + GUID)). Used by
  ## a server to answer and by the client to validate the 101 response.
  base64.encode(sha1Bytes(key & wsGuid))

proc wsExtraFields*(headers: Headers): seq[(string, string)] =
  ## The user headers to carry on an Extended CONNECT (h2 RFC 8441 / h3 RFC 9220):
  ## connection-specific / hop-by-hop fields dropped (the pseudo-headers are added by
  ## the backend), plus sec-websocket-version. Shared by all backends so the policy
  ## stays in one place.
  for (k, v) in headers.pairs:
    let lk = k.toLowerAscii
    if lk in ["host", "connection", "keep-alive", "proxy-connection",
              "transfer-encoding", "upgrade"]: continue
    result.add((lk, v))
  result.add(("sec-websocket-version", wsVersion))

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
  if (b0 and 0x70) != 0:        # RFC 6455 5.2: RSV1/2/3 must be 0 (no extension negotiated)
    raise newException(ValueError, "navi: WebSocket reserved bit (RSV) set")
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
  f.masked = masked
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
  if f.opcode in {opClose, opPing, opPong}:     # RFC 6455 5.5: control frames must be
    if length > 125:                            # <= 125 bytes and never fragmented
      raise newException(ValueError, "navi: WebSocket control frame over 125 bytes")
    if not f.fin:
      raise newException(ValueError, "navi: fragmented WebSocket control frame")
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
    fragmented: bool         ## a data message is open (a fin=false frame started it)

  WsMessageTooLarge* = object of CatchableError
    ## Raised by `offer` when a reassembled message would exceed the caller's
    ## `maxMessageBytes`. The caller should close with `closeMessageTooBig` (1009).

proc isValidUtf8(s: string): bool =
  ## Strict UTF-8 well-formedness (RFC 3629): rejects overlong encodings, surrogate
  ## code points (U+D800..U+DFFF), values above U+10FFFF, and bad continuation bytes.
  ## Used to fail a text message or close reason that is not valid UTF-8 (RFC 6455 8.1).
  var i = 0
  while i < s.len:
    let b = uint8(s[i])
    template cont(k: int): bool =
      i + k < s.len and (uint8(s[i + k]) and 0xC0'u8) == 0x80'u8
    if b < 0x80'u8:
      inc i
    elif b >= 0xC2'u8 and b <= 0xDF'u8:                       # 2-byte
      if not cont(1): return false
      i += 2
    elif b == 0xE0'u8:                                        # 3-byte, no overlong
      if i + 2 >= s.len or uint8(s[i+1]) < 0xA0'u8 or uint8(s[i+1]) > 0xBF'u8 or not cont(2):
        return false
      i += 3
    elif b >= 0xE1'u8 and b <= 0xEC'u8:
      if not cont(1) or not cont(2): return false
      i += 3
    elif b == 0xED'u8:                                        # 3-byte, exclude surrogates
      if i + 2 >= s.len or uint8(s[i+1]) < 0x80'u8 or uint8(s[i+1]) > 0x9F'u8 or not cont(2):
        return false
      i += 3
    elif b >= 0xEE'u8 and b <= 0xEF'u8:
      if not cont(1) or not cont(2): return false
      i += 3
    elif b == 0xF0'u8:                                        # 4-byte, no overlong
      if i + 3 >= s.len or uint8(s[i+1]) < 0x90'u8 or uint8(s[i+1]) > 0xBF'u8 or
         not cont(2) or not cont(3): return false
      i += 4
    elif b >= 0xF1'u8 and b <= 0xF3'u8:
      if not cont(1) or not cont(2) or not cont(3): return false
      i += 4
    elif b == 0xF4'u8:                                        # 4-byte, up to U+10FFFF
      if i + 3 >= s.len or uint8(s[i+1]) < 0x80'u8 or uint8(s[i+1]) > 0x8F'u8 or
         not cont(2) or not cont(3): return false
      i += 4
    else:                                                     # 0x80-0xC1, 0xF5-0xFF
      return false
  true

proc validCloseCode(code: uint16): bool =
  ## RFC 6455 7.4: close codes valid on the wire. 1004/1005/1006 and 1015 are
  ## reserved (never sent); 1000-1014 otherwise are registered, and 3000-4999 are
  ## for registered/private use. Matches Node `ws`'s isValidStatusCode.
  (code >= 1000'u16 and code <= 1014'u16 and code notin [1004'u16, 1005'u16, 1006'u16]) or
  (code >= 3000'u16 and code <= 4999'u16)

proc offer*(a: var WsAssembler, f: Frame, maxMessageBytes = 0,
            rejectMasked = false): WsOutcome =
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
  template finishMessage() =
    ## Complete the reassembled data message. A text message must be valid UTF-8
    ## (RFC 6455 8.1); a binary message is delivered as-is.
    if a.kind == wmText and not isValidUtf8(a.buf):
      raise newException(ValueError, "navi: invalid UTF-8 in a WebSocket text message")
    result.ready = true
    result.message = WsMessage(kind: a.kind, data: a.buf)
  if rejectMasked and f.masked:     # RFC 6455 5.1: a server->client frame must not be masked
    raise newException(ValueError, "navi: masked WebSocket frame received from server")
  case f.opcode
  of opPing:
    result.reply = wrPong
    result.replyPayload = f.payload
  of opPong:
    discard
  of opClose:
    if f.payload.len == 1:          # RFC 6455 5.5.1: a close body is empty or >= 2 bytes
      raise newException(ValueError, "navi: WebSocket close frame with a 1-byte payload")
    var code = closeNormal
    if f.payload.len >= 2:
      code = uint16((ord(f.payload[0]) shl 8) or ord(f.payload[1]))
      if not validCloseCode(code):
        raise newException(ValueError, "navi: invalid WebSocket close code " & $code)
    let reason = if f.payload.len > 2: f.payload[2 .. ^1] else: ""
    if not isValidUtf8(reason):     # RFC 6455 8.1: the close reason must be valid UTF-8
      raise newException(ValueError, "navi: invalid UTF-8 in a WebSocket close reason")
    result.reply = wrCloseEcho
    result.replyPayload = f.payload
    result.ready = true
    result.message = WsMessage(kind: wmClose, closeCode: code, data: reason)
  of opText, opBinary:
    if a.fragmented:                # RFC 6455 5.4: a new data frame mid-fragmentation
      raise newException(ValueError, "navi: new WebSocket data frame during a fragmented message")
    a.buf.setLen(0)                 # a new message starts; the prior one's bytes must not
    guardSize(f.payload.len)        # count against maxMessageBytes here
    a.kind = if f.opcode == opText: wmText else: wmBinary
    a.buf = f.payload
    if f.fin:
      finishMessage()
    else:
      a.fragmented = true
  of opContinuation:
    if not a.fragmented:            # RFC 6455 5.4: continuation with no message in progress
      raise newException(ValueError, "navi: WebSocket continuation with no open message")
    guardSize(f.payload.len)
    a.buf.add f.payload
    if f.fin:
      a.fragmented = false
      finishMessage()

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
