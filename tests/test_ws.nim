## Sans-io WebSocket core: RFC 6455 handshake + frame codec vectors.

import unittest
import std/[base64, strutils]
import navi/proto/ws
import navi/private/sha1js       # the pure-Nim SHA-1 the js target hashes accepts with
import checksums/sha1            # ... cross-checked against the native implementation
import navi/core/[url, headers]   # parseUrl / initHeaders for the upgrade-request tests
import ./support      # hexToBytes
import ./support_ws   # shared WebSocket test servers (WsSrv / startWs*)

suite "websocket handshake":
  test "the handshake should compute the accept key from the client key (RFC 6455 1.3)":
    check acceptFor("dGhlIHNhbXBsZSBub25jZQ==") == "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="

  test "the handshake should generate a fresh 16-byte base64 nonce key":
    check base64.decode(genKey()).len == 16
    check genKey() != genKey()

  test "the js SHA-1 should match checksums across the padding block boundaries (#394)":
    # `checksums/sha1` reaches std/endians, which has no js target, so acceptFor
    # hashes with navi's own SHA-1 under `nim js`. The js build cannot run in the
    # unit suite, but the implementation is plain uint32 Nim, so check it here:
    # the lengths around 56 and 64 are where FIPS 180-4 padding spills into an
    # extra block, and 0 is the empty-message case.
    proc hex(s: string): string =
      for c in s: result.add toHex(ord(c), 2)
    for n in [0, 1, 3, 55, 56, 57, 63, 64, 65, 119, 120, 121, 1000]:
      let msg = repeat("a", n)
      check hex(sha1Raw(msg)) == $secureHash(msg)
    check hex(sha1Raw("abc")) == "A9993E364706816ABA3E25717850C26C9CD0D89D"

  test "acceptFor should agree with the js SHA-1 on the RFC 6455 vector (#394)":
    const guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    check acceptFor("dGhlIHNhbXBsZSBub25jZQ==") ==
      base64.encode(sha1Raw("dGhlIHNhbXBsZSBub25jZQ==" & guid))

suite "websocket handshake response validation (RFC 6455 4.1)":
  const clientKey = "dGhlIHNhbXBsZSBub25jZQ=="

  proc head101(fields: string): string =
    ## A 101 head whose fields are exactly `fields` (each line CRLF-terminated).
    "HTTP/1.1 101 Switching Protocols\r\n" & fields & "\r\n"

  test "validate101 should accept a 101 with the accept, Upgrade and Connection fields":
    check validate101(head101(
      "Upgrade: websocket\r\nConnection: Upgrade\r\n" &
      "Sec-WebSocket-Accept: " & acceptFor(clientKey) & "\r\n"), clientKey)

  test "validate101 should reject a 101 with a correct accept but no Upgrade (#286)":
    check not validate101(head101(
      "Connection: Upgrade\r\n" &
      "Sec-WebSocket-Accept: " & acceptFor(clientKey) & "\r\n"), clientKey)

  test "validate101 should reject a 101 with a correct accept but no Connection (#286)":
    check not validate101(head101(
      "Upgrade: websocket\r\n" &
      "Sec-WebSocket-Accept: " & acceptFor(clientKey) & "\r\n"), clientKey)

  test "validate101 should accept Connection: keep-alive, Upgrade (#286)":
    check validate101(head101(
      "Upgrade: WebSocket\r\nConnection: keep-alive, Upgrade\r\n" &
      "Sec-WebSocket-Accept: " & acceptFor(clientKey) & "\r\n"), clientKey)

  test "validate101 should reject an Upgrade naming another protocol (#286)":
    check not validate101(head101(
      "Upgrade: h2c\r\nConnection: Upgrade\r\n" &
      "Sec-WebSocket-Accept: " & acceptFor(clientKey) & "\r\n"), clientKey)

  test "validate101 should reject a mismatched Sec-WebSocket-Accept":
    check not validate101(head101(
      "Upgrade: websocket\r\nConnection: Upgrade\r\n" &
      "Sec-WebSocket-Accept: " & acceptFor("other") & "\r\n"), clientKey)

  test "validate101 should reject two Sec-WebSocket-Accept fields (#289)":
    # One copy matches, so an accept-only check would let a split/injected
    # response through.
    check not validate101(head101(
      "Upgrade: websocket\r\nConnection: Upgrade\r\n" &
      "Sec-WebSocket-Accept: " & acceptFor(clientKey) & "\r\n" &
      "Sec-WebSocket-Accept: " & acceptFor("other") & "\r\n"), clientKey)

  test "validate101 should reject two identical Sec-WebSocket-Accept fields (#289)":
    check not validate101(head101(
      "Upgrade: websocket\r\nConnection: Upgrade\r\n" &
      "Sec-WebSocket-Accept: " & acceptFor(clientKey) & "\r\n" &
      "sec-websocket-accept: " & acceptFor(clientKey) & "\r\n"), clientKey)

  test "validate101 should reject a non-101 status":
    check not validate101("HTTP/1.1 200 OK\r\nUpgrade: websocket\r\n" &
      "Connection: Upgrade\r\nSec-WebSocket-Accept: " & acceptFor(clientKey) &
      "\r\n\r\n", clientKey)

suite "WebSocket target URL (parseWsUrl)":
  # The openers map the ws schemes onto http/https, which is what decides TLS
  # (`isTls` compares against "https"), the default port and the pool key.

  test "ws:// should dial http and wss:// should dial https":
    let plain = parseWsUrl("ws://x.test/chat")
    check not plain.isTls
    check plain.port == 80
    check $plain == "http://x.test/chat"
    let secure = parseWsUrl("wss://x.test/chat")
    check secure.isTls
    check secure.port == 443
    check $secure == "https://x.test/chat"

  test "the scheme should be matched case-insensitively, so WSS:// still dials TLS":
    # RFC 3986 3.1: the scheme is case-insensitive. A case-sensitive prefix match
    # left "WSS://" in place, and the handshake then ran in cleartext on port 80.
    for target in ["WSS://x.test/chat", "Wss://x.test/chat", "wSS://x.test/chat"]:
      let u = parseWsUrl(target)
      check u.isTls
      check u.port == 443
      check u.host == "x.test"
    for target in ["WS://x.test/chat", "Ws://x.test/chat"]:
      let u = parseWsUrl(target)
      check not u.isTls
      check u.port == 80
      check u.host == "x.test"

  test "an explicit port, userinfo and query should survive the scheme swap":
    let u = parseWsUrl("WSS://x.test:8443/chat?room=1")
    check u.isTls
    check u.port == 8443
    check $u == "https://x.test:8443/chat?room=1"

  test "an http/https target should pass through unchanged":
    check parseWsUrl("https://x.test/chat").isTls
    check not parseWsUrl("http://x.test/chat").isTls

  test "a target with no host should be rejected whatever the scheme case (#435)":
    for target in ["ws:///chat", "wss:///chat", "WSS:///chat", "Ws:///chat",
                   "https:///chat"]:
      expect ValueError:
        discard parseWsUrl(target)

suite "websocket upgrade request (h1)":
  const nonce = "dGhlIHNhbXBsZSBub25jZQ=="
  let u = parseUrl("http://example.test/chat")

  test "upgradeRequest should emit the mandatory handshake fields":
    let req = upgradeRequest(u, nonce, initHeaders())
    check req.startsWith("GET /chat HTTP/1.1\r\n")
    check "Host: example.test\r\n" in req
    check "Upgrade: websocket\r\n" in req
    check "Connection: Upgrade\r\n" in req
    check "Sec-WebSocket-Key: " & nonce & "\r\n" in req
    check "Sec-WebSocket-Version: 13\r\n" in req
    check req.endsWith("\r\n\r\n")

  test "upgradeRequest should carry an unrelated caller header through":
    let req = upgradeRequest(u, nonce, initHeaders({"X-Trace": "abc"}))
    check "X-Trace: abc\r\n" in req

  test "upgradeRequest should reject CRLF in a caller header value (#288)":
    expect ValueError:
      discard upgradeRequest(u, nonce,
        initHeaders({"X-Evil": "a\r\nX-Injected: 1"}))

  test "upgradeRequest should reject CRLF in a caller header name (#288)":
    expect ValueError:
      discard upgradeRequest(u, nonce,
        initHeaders({"X-Evil\r\nX-Injected": "1"}))

  test "upgradeRequest should reject a bare LF in a caller header value (#288)":
    expect ValueError:
      discard upgradeRequest(u, nonce, initHeaders({"X-Evil": "a\nX-Injected: 1"}))

  test "upgradeRequest should reject NUL in a caller header value (#288)":
    expect ValueError:
      discard upgradeRequest(u, nonce, initHeaders({"X-Evil": "a\x00b"}))

  test "upgradeRequest should reject CRLF in the request target (#288)":
    # std/uri passes control characters through verbatim, so a crafted ws:// URL
    # reaches the request line intact.
    var bad = parseUrl("http://example.test/chat")
    bad.raw.path = "/chat\r\nX-Injected: 1"
    expect ValueError:
      discard upgradeRequest(bad, nonce, initHeaders())

  test "upgradeRequest should reject CRLF in the Host (#288)":
    var bad = parseUrl("http://example.test/chat")
    bad.raw.hostname = "example.test\r\nX-Injected: 1"
    expect ValueError:
      discard upgradeRequest(bad, nonce, initHeaders())

  test "upgradeRequest should drop caller fields that collide with the handshake (#288)":
    let req = upgradeRequest(u, nonce, initHeaders({
      "Host": "evil.test",
      "Connection": "close",
      "Upgrade": "h2c",
      "Sec-WebSocket-Key": "AAAAAAAAAAAAAAAAAAAAAA==",
      "Sec-WebSocket-Version": "8",
      "Sec-WebSocket-Accept": "spoofed",
      "X-Keep": "yes"}))
    check req.count("Host: ") == 1
    check "Host: example.test\r\n" in req
    check req.count("Connection: ") == 1
    check "Connection: Upgrade\r\n" in req
    check req.count("Upgrade: ") == 1
    check req.count("Sec-WebSocket-Key: ") == 1
    check "Sec-WebSocket-Key: " & nonce & "\r\n" in req
    check req.count("Sec-WebSocket-Version: ") == 1
    check "Sec-WebSocket-Version: 13\r\n" in req
    check "spoofed" notin req
    check "X-Keep: yes\r\n" in req

  test "upgradeRequest should match the collision list case-insensitively (#288)":
    let req = upgradeRequest(u, nonce, initHeaders({"CONNECTION": "close"}))
    check req.count("Connection: ") == 1
    check "close" notin req

suite "Extended CONNECT handshake fields (RFC 8441 / 9220)":
  test "wsExtraFields should lowercase names and keep unrelated caller headers":
    let f = wsExtraFields(initHeaders({"X-Trace": "abc"}))
    check ("x-trace", "abc") in f

  test "wsExtraFields should drop hop-by-hop and h1 upgrade fields":
    let f = wsExtraFields(initHeaders({
      "Host": "evil.test", "Connection": "Upgrade", "Upgrade": "websocket",
      "Keep-Alive": "timeout=5", "Proxy-Connection": "keep-alive",
      "Transfer-Encoding": "chunked"}))
    for (k, _) in f:
      check k notin ["host", "connection", "upgrade", "keep-alive",
                     "proxy-connection", "transfer-encoding"]

  test "wsExtraFields should drop a stray key, accept and http2-settings (#289)":
    let f = wsExtraFields(initHeaders({
      "Sec-WebSocket-Key": "AAAAAAAAAAAAAAAAAAAAAA==",
      "Sec-WebSocket-Accept": "spoofed",
      "HTTP2-Settings": "AAMAAABkAARAAAAAAAIAAAAA"}))
    for (k, _) in f:
      check k notin ["sec-websocket-key", "sec-websocket-accept", "http2-settings"]

  test "wsExtraFields should emit exactly one sec-websocket-version (#289)":
    var seen = 0
    for (k, v) in wsExtraFields(initHeaders({"Sec-WebSocket-Version": "8"})):
      if k == "sec-websocket-version":
        inc seen
        check v == "13"
    check seen == 1

suite "websocket frame codec":
  test "the frame codec should encode the masked Hello example (RFC 6455 5.7)":
    let wire = encodeFrame(opText, "Hello", masked = true,
                           maskKey = hexToBytes("37fa213d"))
    check wire == hexToBytes("818537fa213d7f9f4d5158")

  test "the frame codec should encode an unmasked server frame":
    check encodeFrame(opText, "Hello", masked = false) == hexToBytes("810548656c6c6f")

  test "the frame codec should decode a masked frame and unmask the payload":
    var d: WsDecoder
    d.feed(hexToBytes("818537fa213d7f9f4d5158"))
    var f: Frame
    check d.next(f)
    check f.fin
    check f.opcode == opText
    check f.payload == "Hello"

  test "the frame codec should round-trip binary data through a random mask":
    let payload = "raw \x00\x01\x02\xff bytes"
    var d: WsDecoder
    d.feed(encodeFrame(opBinary, payload))     # random mask key
    var f: Frame
    check d.next(f)
    check f.opcode == opBinary
    check f.payload == payload

  test "the frame codec should decode a frame with a 16-bit extended length":
    let big = repeat("x", 1000)
    var d: WsDecoder
    d.feed(encodeFrame(opText, big))
    var f: Frame
    check d.next(f)
    check f.payload == big

  test "the frame codec should wait for more bytes when a frame is split":
    let wire = encodeFrame(opText, "hello world")
    var d: WsDecoder
    d.feed(wire[0 ..< 4])
    var f: Frame
    check not d.next(f)
    d.feed(wire[4 ..< wire.len])
    check d.next(f)
    check f.payload == "hello world"

  test "the frame codec should decode two frames from one buffer":
    var d: WsDecoder
    d.feed(encodeFrame(opPing, "") & encodeFrame(opText, "hi"))
    var f: Frame
    check d.next(f) and f.opcode == opPing
    check d.next(f) and f.opcode == opText and f.payload == "hi"

  test "the frame codec should decode 1000 small frames from one feed, in order (#409)":
    # One 64 KiB-ish transport read carrying a burst of tiny messages. The decoder
    # used to front-delete the buffer per frame, memmoving the whole remainder
    # 1000 times (O(N x buffered)); with a read cursor this is one pass.
    var wire = ""
    for i in 0 ..< 1000:
      wire.add encodeFrame(opText, "msg-" & $i & repeat(".", 30), masked = (i mod 2 == 0))
    var d: WsDecoder
    d.feed(wire)
    var f: Frame
    for i in 0 ..< 1000:
      check d.next(f)
      check f.opcode == opText
      check f.payload == "msg-" & $i & repeat(".", 30)
    check not d.next(f)
    check d.buffered == 0

  test "the frame codec should decode a frame split at every byte boundary (#409)":
    # A two-frame wire cut at each possible split point: the cursor must hold a
    # partial header, a partial mask key and a partial payload across feeds, and
    # compaction on the second feed must not lose the retained prefix.
    let wire = encodeFrame(opText, "hello", masked = true,
                           maskKey = hexToBytes("37fa213d")) &
               encodeFrame(opBinary, "world!", masked = false)
    for cut in 0 .. wire.len:
      var d: WsDecoder
      var f: Frame
      d.feed(wire[0 ..< cut])
      var got: seq[(Opcode, string)] = @[]
      while d.next(f): got.add (f.opcode, f.payload)
      d.feed(wire[cut .. ^1])
      while d.next(f): got.add (f.opcode, f.payload)
      check got == @[(opText, "hello"), (opBinary, "world!")]
      check d.buffered == 0

  test "the frame codec should keep control frames interleaved with continuations (#409)":
    # Fragmented message with a ping and a pong spliced between the fragments, all
    # in one feed: the cursor must not reorder, drop or merge anything.
    var wire = encodeFrame(opText, "he", masked = true, fin = false)
    wire.add encodeFrame(opPing, "p", masked = true)
    wire.add encodeFrame(opContinuation, "ll", masked = true, fin = false)
    wire.add encodeFrame(opPong, "q", masked = true)
    wire.add encodeFrame(opContinuation, "o", masked = true, fin = true)
    var d: WsDecoder
    d.feed(wire)
    var f: Frame
    var a: WsAssembler
    var replies: seq[WsReply] = @[]
    var message = ""
    var seen: seq[Opcode] = @[]
    while d.next(f):
      seen.add f.opcode
      let o = a.offer(f)
      if o.reply != wrNone: replies.add o.reply
      if o.ready: message = o.message.data
    check seen == @[opText, opPing, opContinuation, opPong, opContinuation]
    check message == "hello"
    check replies == @[wrPong]
    check d.buffered == 0

  test "the frame codec should compact its buffer across feed/consume cycles (#409)":
    # Feed a burst, drain it, repeat: the retained buffer must stay bounded by the
    # burst rather than growing with the number of frames ever decoded. It also
    # covers the partial-frame case, where a tail is carried into the next feed.
    # 512 x ~46 bytes is over the 8 KiB compaction floor, so the in-place move
    # branch runs and not just the "fully consumed, setLen(0)" one.
    let frame = encodeFrame(opText, repeat("x", 40), masked = true)
    var burst = ""
    for _ in 0 ..< 512: burst.add frame
    var d: WsDecoder
    var f: Frame
    for round in 0 ..< 100:
      d.feed(burst[0 ..< burst.len - 3])      # last frame straddles the feed
      while d.next(f): check f.payload == repeat("x", 40)
      d.feed(burst[burst.len - 3 .. ^1])
      while d.next(f): check f.payload == repeat("x", 40)
      check d.buffered == 0
      check d.bufferLen <= 2 * burst.len      # no unbounded growth

  test "the frame codec should reject a reserved opcode (RFC 6455 5.2)":
    var d: WsDecoder
    d.feed("\x83\x00")            # FIN + opcode 0x3 (reserved), unmasked, len 0
    var f: Frame
    var msg = ""
    try:
      discard d.next(f)
    except ValueError as e: msg = e.msg
    check "reserved WebSocket opcode" in msg

  test "the frame codec should reject a 64-bit length with the high bit set (DoS)":
    # opcode 0x2 (binary), 127 length marker, then an 8-byte length 0x8000...0000.
    # This became a negative int that slipped past the bounds check and crashed
    # newString; it must now raise instead.
    var d: WsDecoder
    d.feed("\x82\x7f\x80\x00\x00\x00\x00\x00\x00\x00")
    var f: Frame
    var msg = ""
    try:
      discard d.next(f)
    except ValueError as e: msg = e.msg
    check "invalid or exceeds" in msg

  test "the frame codec should reject a 64-bit length whose high word is set (#285)":
    # 0x0000_0001_0000_0005: well under the 63-bit limit, so the high-bit guard
    # never sees it, but the low 32 bits are a plausible 5. Accumulating into a
    # 32-bit `int` truncated it to 5 and let a 4 GiB frame through as a 5-byte
    # one; the length must be carried in a uint64 and rejected against the cap.
    var d: WsDecoder
    d.feed("\x82\x7f\x00\x00\x00\x01\x00\x00\x00\x05" & "hello")
    var f: Frame
    var msg = ""
    try:
      discard d.next(f)
    except ValueError as e: msg = e.msg
    check "invalid or exceeds" in msg

suite "websocket close":
  test "the close payload should carry the big-endian code then the reason":
    let p = closePayload(closeNormal, "bye")
    check ord(p[0]) == 0x03 and ord(p[1]) == 0xe8   # 1000
    check p[2 .. ^1] == "bye"

suite "websocket message assembly":
  test "the message assembler should reassemble a fragmented text message":
    var a: WsAssembler
    check not a.offer(Frame(fin: false, opcode: opText, payload: "he")).ready
    let o = a.offer(Frame(fin: true, opcode: opContinuation, payload: "llo"))
    check o.ready
    check o.message.kind == wmText
    check o.message.data == "hello"

  test "the message assembler should answer a ping with a pong carrying the same payload":
    var a: WsAssembler
    let o = a.offer(Frame(fin: true, opcode: opPing, payload: "hi"))
    check o.reply == wrPong
    check o.replyPayload == "hi"
    check not o.ready

  test "the message assembler should yield a close message and ask for a close echo":
    var a: WsAssembler
    let o = a.offer(Frame(fin: true, opcode: opClose,
                          payload: closePayload(closeNormal, "bye")))
    check o.ready
    check o.message.kind == wmClose
    check o.message.closeCode == closeNormal
    check o.message.data == "bye"
    check o.reply == wrCloseEcho

  test "the assembler should reject a single frame over maxMessageBytes":
    var a: WsAssembler
    expect WsMessageTooLarge:
      discard a.offer(Frame(fin: true, opcode: opText, payload: "toolong"),
                      maxMessageBytes = 4)

  test "the assembler should reject a fragmented message that grows past maxMessageBytes":
    var a: WsAssembler
    check not a.offer(Frame(fin: false, opcode: opText, payload: "aaaa"),
                      maxMessageBytes = 6).ready
    expect WsMessageTooLarge:                    # 4 + 3 = 7 > 6, before buffering
      discard a.offer(Frame(fin: true, opcode: opContinuation, payload: "bbb"),
                      maxMessageBytes = 6)

  test "the assembler should accept a message exactly at maxMessageBytes":
    var a: WsAssembler
    let o = a.offer(Frame(fin: true, opcode: opText, payload: "12345"),
                    maxMessageBytes = 5)
    check o.ready and o.message.data == "12345"

  test "maxMessageBytes of 0 should impose no limit":
    var a: WsAssembler
    let o = a.offer(Frame(fin: true, opcode: opText, payload: repeat("x", 100_000)))
    check o.ready and o.message.data.len == 100_000

suite "websocket message copy path (#411)":
  # `offer` takes the frame as a `sink` and moves the payload through the
  # assembler into the delivered message, so a received message is no longer
  # copied into `a.buf` and then out of it again. These pin the behaviour that
  # the moves must not break: the delivered bytes are right, the message stays
  # valid once later frames arrive, and the assembler starts each message clean.
  test "a single-frame message should be delivered whole from a moved-from frame":
    var a: WsAssembler
    let payload = repeat("\xab", 64 * 1024)
    var f = Frame(fin: true, opcode: opBinary, payload: payload)
    let o = a.offer(move(f))                 # the caller hands the frame over
    check o.ready
    check o.message.kind == wmBinary
    check o.message.data.len == payload.len
    check o.message.data == payload

  test "a delivered message should survive the messages that follow it":
    # The assembler hands its buffer to the message; it must not keep an alias of
    # it and overwrite the caller's message with the next one's bytes.
    var a: WsAssembler
    let first = a.offer(Frame(fin: true, opcode: opText, payload: "first"))
    check first.ready and first.message.data == "first"
    let second = a.offer(Frame(fin: true, opcode: opText, payload: "second"))
    check second.ready and second.message.data == "second"
    check first.message.data == "first"      # untouched by the second message

  test "a fragmented message should assemble across frames and not leak into the next":
    var a: WsAssembler
    check not a.offer(Frame(fin: false, opcode: opText, payload: "aa")).ready
    check not a.offer(Frame(fin: false, opcode: opContinuation, payload: "bb")).ready
    let o = a.offer(Frame(fin: true, opcode: opContinuation, payload: "cc"))
    check o.ready and o.message.data == "aabbcc"
    check not a.offer(Frame(fin: false, opcode: opText, payload: "xx")).ready
    let o2 = a.offer(Frame(fin: true, opcode: opContinuation, payload: "yy"))
    check o2.ready and o2.message.data == "xxyy"   # not "aabbccxxyy"
    check o.message.data == "aabbcc"

  test "decoded frames fed through move() should assemble every message":
    # The shape both backends use: one reused `Frame`, refilled by `next` and
    # handed to `offer` with `move`. Covers a single-frame and a fragmented
    # message arriving in one read.
    var wire = encodeFrame(opText, "solo", masked = true)
    wire.add encodeFrame(opBinary, "he", masked = true, fin = false)
    wire.add encodeFrame(opContinuation, "llo", masked = true, fin = true)
    var d: WsDecoder
    d.feed(wire)
    var a: WsAssembler
    var f: Frame
    var msgs: seq[string]
    var kinds: seq[WsMessageKind]
    while d.next(f):
      let o = a.offer(move(f))
      if o.ready:
        msgs.add o.message.data
        kinds.add o.message.kind
    check msgs == @["solo", "hello"]
    check kinds == @[wmText, wmBinary]
    check d.buffered == 0

  test "a close frame should echo its whole payload while the message keeps the reason":
    # `offer` moves the payload into `replyPayload`, so the echo must still be the
    # code plus reason while `message.data` is the reason alone.
    var a: WsAssembler
    var f = Frame(fin: true, opcode: opClose,
                  payload: closePayload(closeNormal, "bye"))
    let o = a.offer(move(f))
    check o.reply == wrCloseEcho
    check o.replyPayload == closePayload(closeNormal, "bye")
    check o.ready
    check o.message.closeCode == closeNormal
    check o.message.data == "bye"

  test "a ping should still be ponged with its payload after the move":
    var a: WsAssembler
    var f = Frame(fin: true, opcode: opPing, payload: "ping-me")
    let o = a.offer(move(f))
    check o.reply == wrPong
    check o.replyPayload == "ping-me"
    check not o.ready

  test "maxMessageBytes and UTF-8 validation should survive the moved payload":
    var a: WsAssembler
    expect WsMessageTooLarge:
      discard a.offer(Frame(fin: true, opcode: opText, payload: "toolong"),
                      maxMessageBytes = 4)
    expect ValueError:                       # a rejected message must not be kept
      discard a.offer(Frame(fin: true, opcode: opText, payload: "\xff\xfe"))
    let o = a.offer(Frame(fin: true, opcode: opText, payload: "caf\xc3\xa9"),
                    maxMessageBytes = 8)
    check o.ready and o.message.data == "caf\xc3\xa9"

# End-to-end: navi's sync WebSocket client against the shared in-process servers
# in support.nim (built from the same sans-io core; server frames unmasked).
import navi
import navi/core/response   # navi's TimeoutError (qualified; std/net has one too)


suite "websocket streaming":
  test "stream() should read a fragmented message chunk by chunk":
    var th: Thread[WsSrv]
    var port: int
    startWsStreamEcho(th, port)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.send("fragment")                        # server replies with 3 fragments
    let reader = ws.stream()
    check reader.kind == wmText
    var chunks: seq[string]
    reader.each(chunk):
      chunks.add chunk
    check chunks == @["one", "-two", "-three"]  # one chunk per frame, not reassembled
    ws.close()
    joinThread(th)

  test "stream() should hand the first frame over and keep reading (#411)":
    # The reader buffers the opening frame; `readChunk` moves it out instead of
    # returning a copy and pinning the original for the reader's lifetime. What
    # is observable: the chunk is right, the stream continues past it, and the
    # connection is still usable for the next message.
    var th: Thread[WsSrv]
    var port: int
    startWsStreamEcho(th, port)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.send("fragment")                        # server replies with 3 fragments
    let reader = ws.stream()
    check reader.kind == wmText
    check reader.readChunk() == "one"          # the buffered first frame
    check reader.readChunk() == "-two"         # the reader is not stuck on it
    check reader.readChunk() == "-three"
    check reader.readChunk() == ""             # fin consumed
    ws.send("after")                           # and the socket still works
    let m = ws.receive()
    check m.kind == wmText
    check m.data == "after"
    ws.close()
    joinThread(th)

  test "stream() should deliver a single-frame message from the buffered frame (#411)":
    var th: Thread[WsSrv]
    var port: int
    startWsStreamEcho(th, port)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.send("solo")                            # echoed back as one frame
    let reader = ws.stream()
    check reader.kind == wmText
    check reader.readChunk() == "solo"
    check reader.readChunk() == ""             # the fin frame was the first one
    ws.close()
    joinThread(th)

  test "stream(writer) should send a message as fragments the peer reassembles":
    var th: Thread[WsSrv]
    var port: int
    startWsStreamEcho(th, port)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.stream(writer):                          # finish() auto-sent at block exit
      for part in @["aa", "bb", "cc"]:
        writer.write(part)
    let m = ws.receive()                        # server reassembled + echoed it whole
    check m.kind == wmText
    check m.data == "aabbcc"
    ws.close()
    joinThread(th)

  test "streamBinary(writer) should send a binary fragmented message":
    var th: Thread[WsSrv]
    var port: int
    startWsStreamEcho(th, port)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.streamBinary(writer):
      writer.write("\x00\x01")
      writer.write("\x02\xff")
    let m = ws.receive()
    check m.kind == wmBinary
    check m.data == "\x00\x01\x02\xff"
    ws.close()
    joinThread(th)

suite "websocket client end to end":
  test "the WebSocket client should handshake, echo text and binary, reassemble fragments, and close":
    var th: Thread[WsSrv]
    var port: int
    startWsEcho(th, port)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")

    ws.send("hello")
    let m1 = ws.receive()
    check m1.kind == wmText
    check m1.data == "hello"

    ws.send("\x00\x01\x02 bytes", binary = true)
    let m2 = ws.receive()
    check m2.kind == wmBinary
    check m2.data == "\x00\x01\x02 bytes"

    ws.send("please fragment")
    let m3 = ws.receive()
    check m3.kind == wmText
    check m3.data == "frag-ment"               # reassembled from two frames

    ws.send("bye")                             # server answers with a close frame
    let m4 = ws.receive()
    check m4.kind == wmClose
    check m4.closeCode == closeNormal
    ws.close()                                 # idempotent: connection already closed
    joinThread(th)

  test "receive should raise WsMessageTooLarge when a message exceeds maxMessageBytes":
    var th: Thread[WsSrv]
    var port: int
    startWsEcho(th, port)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat", maxMessageBytes = 8)
    ws.send(repeat("x", 100))                  # sending is not capped; the echo is
    expect WsMessageTooLarge:
      discard ws.receive()                     # 100-byte echo > 8-byte cap -> raises + 1009
    ws.close()                                 # idempotent no-op: already dropped on 1009
    joinThread(th)

suite "websocket protocol-error teardown (#281)":
  # A protocol error from the peer must fail the connection (RFC 6455 7.1.7), not
  # just raise: the transport has to be torn down, else it leaks and the decoder
  # stays desynced. The server reports whether the client actually dropped it.
  test "receive should fail the connection when the server sends a masked frame":
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    startWsMisbehave(th, port, sawEof)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.send("masked")
    expect ValueError:
      discard ws.receive()
    joinThread(th)
    check sawEof                               # torn down, not leaked
    ws.close()                                 # idempotent no-op

  test "receive should fail the connection on invalid UTF-8 in a text message":
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    startWsMisbehave(th, port, sawEof)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.send("badutf8")
    expect ValueError:
      discard ws.receive()
    joinThread(th)
    check sawEof
    ws.close()

suite "websocket lifecycle guards (#289)":
  test "send and ping should raise on a closed WebSocket":
    var th: Thread[WsSrv]
    var port: int
    startWsEcho(th, port)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.close()
    expect IOError:
      ws.send("too late")
    expect IOError:
      ws.ping()
    joinThread(th)

  test "close should reject the close codes reserved for local use":
    var th: Thread[WsSrv]
    var port: int
    startWsEcho(th, port)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    expect ValueError:
      ws.close(closeNoStatus)                  # 1005: no status received
    expect ValueError:
      ws.close(closeAbnormal)                  # 1006: abnormal closure
    expect ValueError:
      ws.close(1015'u16)                       # 1015: TLS handshake failure
    ws.close()                                 # a valid code still works
    joinThread(th)

  test "receive should report a codeless close as 1005 and echo no code (#244)":
    # RFC 6455 7.1.5: a close frame with no status code must surface as 1005 ("no
    # status received"), never as an explicit 1000, or an application cannot tell a
    # codeless close from a normal one. 7.4.1: 1005 is reserved for local use, so the
    # close echo the client sends back must carry an empty body, not the code 1005.
    var th: Thread[WsSrv]
    var port: int
    var echoed = "<unset>"
    startWsCodelessClose(th, port, echoed)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.send("go")                              # triggers the server's codeless close
    let m = ws.receive()
    check m.kind == wmClose
    check m.closeCode == closeNoStatus         # 1005, not closeNormal
    check m.data == ""
    ws.close(m.closeCode)                      # mirroring it back stays a no-op
    joinThread(th)
    check echoed == ""                         # empty body on the wire: no 1005 sent

  test "close should accept a reserved code once the socket is already closed":
    # `receive` reports 1006 on an abrupt EOF and 1005 for a codeless close, so a
    # caller mirroring `m.closeCode` back on teardown must not blow up: the codes
    # are rejected only while a close frame would actually go out.
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    startWsMisbehave(th, port, sawEof)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.send("eofnow")                          # dropped with no close frame
    let m = ws.receive()
    check m.closeCode == closeAbnormal
    ws.close(m.closeCode)                      # idempotent teardown, not a raise
    joinThread(th)

  test "a transport EOF should surface as 1006 on both read paths":
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    startWsMisbehave(th, port, sawEof)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.send("eofnow")                          # server drops us with no close frame
    let m = ws.receive()
    check m.kind == wmClose
    check m.closeCode == closeAbnormal
    joinThread(th)

    var th2: Thread[WsSrv]
    var port2: int
    var sawEof2 = false
    startWsMisbehave(th2, port2, sawEof2)
    let ws2 = api.websocket("ws://127.0.0.1:" & $port2 & "/chat")
    ws2.send("eofnow")
    let r = ws2.stream()                       # the streaming path reports the same
    check r.kind == wmClose
    check r.closeCode == closeAbnormal
    joinThread(th2)

suite "websocket incremental UTF-8 validation (#282)":
  test "the scanner should accept a code point split across chunks":
    var v: WsUtf8Scanner
    check v.scanUtf8("\xf0\x9f")               # first half of U+1F4A9
    check v.midCodePoint
    check v.scanUtf8("\x92\xa9")
    check not v.midCodePoint

  test "the scanner should reject a surrogate split across chunks":
    var v: WsUtf8Scanner
    check v.scanUtf8("\xed\xa0")               # nothing complete yet
    check not v.scanUtf8("\x80")               # U+D800: a surrogate, not a code point

  test "the scanner should reject an invalid byte as soon as it lands":
    var v: WsUtf8Scanner
    check not v.scanUtf8("abc\xff")

  test "the scanner should report a truncated tail when the message ends":
    var v: WsUtf8Scanner
    check v.scanUtf8("ok \xc3")                # a 2-byte sequence, one byte short
    check v.midCodePoint

suite "websocket streaming text validation (#282)":
  # The streaming read path never buffers the whole message, so it validates each
  # chunk as it arrives instead of relying on the assembler's whole-message check.
  test "a streamed text message should be rejected when a code point is invalid across frames":
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    startWsMisbehave(th, port, sawEof)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.send("splitbad")
    let r = ws.stream()
    expect ValueError:
      while r.readChunk().len > 0: discard
    joinThread(th)
    check sawEof
    ws.close()

  test "a streamed text message should accept a code point split across frames":
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    startWsMisbehave(th, port, sawEof)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.send("splitok")
    let r = ws.stream()
    var msg = ""
    r.each(chunk):
      msg.add chunk
    check msg == "\xf0\x9f\x92\xa9"            # U+1F4A9, whole again
    ws.close()
    joinThread(th)

suite "websocket streaming desync teardown (#284)":
  # The streaming read path desyncs on the same protocol errors, and only `drain`
  # used to tear down: a direct readChunk/stream() left the transport alive.
  test "stream() should fail the connection when a message starts with a continuation":
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    startWsMisbehave(th, port, sawEof)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.send("orphan")
    expect IOError:
      discard ws.stream()
    joinThread(th)
    check sawEof
    ws.close()

  test "readChunk should fail the connection on a data frame where a continuation is due":
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    startWsMisbehave(th, port, sawEof)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.send("datamid")
    let r = ws.stream()
    check r.readChunk() == "aa"                # the opening fragment
    expect IOError:
      discard r.readChunk()                    # a new text frame, not a continuation
    joinThread(th)
    check sawEof
    ws.close()

suite "websocket streaming close validation (RFC 6455 5.5.1 / 7.4)":
  # The streaming reader used to take the peer's close frame on trust: an invalid
  # body surfaced as a clean `wmClose` and was echoed back verbatim. It now runs
  # the same checks `receive` does, and a bad frame fails the connection with a
  # 1002 close of its own instead of mirroring the malformed one back (#283).
  template badCloseAtOpen(trigger: string) =
    ## Drive `stream()` straight onto the bad close `trigger` produces, and assert
    ## the client answered 1002 and tore the transport down.
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    var reply = ""
    startWsMisbehaveClose(th, port, sawEof, reply)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.send(trigger)
    expect ValueError:
      discard ws.stream()
    joinThread(th)
    check reply == closePayload(closeProtocolError)   # 1002, not the frame echoed
    check sawEof                                      # torn down, not leaked
    ws.close()

  template badCloseMidMessage(trigger: string) =
    ## The same, for a bad close that interrupts a message already being streamed.
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    var reply = ""
    startWsMisbehaveClose(th, port, sawEof, reply)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.send(trigger)
    let r = ws.stream()
    check r.readChunk() == "aa"                # the opening fragment
    expect ValueError:
      discard r.readChunk()                    # the bad close
    check r.closeCode == closeProtocolError    # what `receive` would report too
    joinThread(th)
    check reply == closePayload(closeProtocolError)
    check sawEof
    ws.close()

  test "stream() should answer a close code that may not be sent with 1002":
    badCloseAtOpen("closebad")                 # close frame carrying 1006

  test "stream() should answer a 1-byte close payload with 1002":
    badCloseAtOpen("closeshort")

  test "stream() should answer a close with an invalid-UTF-8 reason with 1002":
    badCloseAtOpen("closeutf8")

  test "stream() should answer a masked close frame with 1002":
    badCloseAtOpen("closemasked")              # RFC 6455 5.1: never masked

  test "readChunk should answer an invalid close code mid-message with 1002":
    badCloseMidMessage("midclosebad")

  test "readChunk should answer an invalid-UTF-8 close reason mid-message with 1002":
    badCloseMidMessage("midcloseutf8")

  test "readChunk should answer a masked close mid-message with 1002":
    # Rejected a step earlier than the others (in `readDataFrame`, before the close
    # handling), so this pins that the reader still reports 1002 like the rest.
    badCloseMidMessage("midclosemasked")

  test "a valid close mid-message should still end the stream with its code":
    var th: Thread[WsSrv]
    var port: int
    var sawEof = false
    startWsMisbehave(th, port, sawEof)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.send("midcloseok")
    let r = ws.stream()
    check r.readChunk() == "aa"
    check r.readChunk() == ""                  # the close truncates the message
    check r.closeCode == closeGoingAway
    joinThread(th)
    ws.close()

suite "websocket streamed-write guards":
  # `write`/`finishWrite` used to poke a torn-down transport and fail with whatever
  # the socket layer said; they now raise navi's IOError like `send`/`ping`.
  test "write should raise on a closed WebSocket":
    var th: Thread[WsSrv]
    var port: int
    startWsEcho(th, port)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.close()
    expect IOError:
      ws.stream(writer):
        writer.write("too late")
    joinThread(th)

  test "finishWrite should raise on a closed WebSocket":
    var th: Thread[WsSrv]
    var port: int
    startWsEcho(th, port)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat")
    ws.close()
    expect IOError:
      ws.stream(writer):                       # no writes: the fin frame alone
        if writer == nil: discard
    joinThread(th)

suite "websocket keepalive":
  test "receive should raise TimeoutError when keepalive gets no response":
    var th: Thread[WsSrv]
    var port: int
    startWsSilent(th, port)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat", keepAlive = 40)
    expect response.TimeoutError:
      discard ws.receive()                     # ping at 40ms, dead at 80ms (no pong)
    joinThread(th)

  test "keepalive should keep the connection alive across pings until a message arrives":
    var th: Thread[WsSrv]
    var port: int
    startWsPingCounter(th, port)

    let api = newNavi()
    # A generous interval: the server must pong within it, and CI thread scheduling
    # is jittery, so 40ms could false-trip the liveness check on a loaded runner.
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat", keepAlive = 200)
    let m = ws.receive()                       # pinged twice (each ponged), then "alive"
    check m.kind == wmText
    check m.data == "alive"
    ws.close()
    joinThread(th)

suite "WebSocket transport selection (sync)":
  test "the sync backend should reject an h2 WebSocket without TLS":
    # h2 Extended CONNECT (RFC 8441) is a wss-only tunnel; a plaintext ws:// with
    # H1 excluded leaves no usable transport, so it raises before connecting.
    var cfg = initNaviConfig()
    cfg.http = {H2}
    let api = newNavi(cfg)
    expect ProtocolError:
      discard api.websocket("ws://127.0.0.1:1/never")

  test "the sync backend should reject an h3 WebSocket without TLS":
    # h3 Extended CONNECT (RFC 9220) likewise needs wss; non-TLS has no transport.
    var cfg = initNaviConfig()
    cfg.http = {H3}
    let api = newNavi(cfg)
    expect ProtocolError:
      discard api.websocket("ws://127.0.0.1:1/never")

suite "websocket frame validation (RFC 6455)":
  test "the decoder should reject a frame with a reserved bit set":
    var d: WsDecoder
    var f: Frame
    d.feed("\xC1\x00")                 # FIN + RSV1 + text, unmasked, len 0
    expect ValueError: discard d.next(f)

  test "the decoder should reject a control frame larger than 125 bytes":
    var d: WsDecoder
    var f: Frame
    d.feed("\x89\x7e\x00\xc8" & repeat("\x00", 200))   # PING, 16-bit len 200
    expect ValueError: discard d.next(f)

  test "the decoder should reject a fragmented control frame":
    var d: WsDecoder
    var f: Frame
    d.feed("\x09\x00")                 # PING with FIN clear
    expect ValueError: discard d.next(f)

  test "the assembler should reject a masked server frame when told to":
    var a: WsAssembler
    expect ValueError:
      discard a.offer(Frame(fin: true, opcode: opText, payload: "hi", masked: true),
                      rejectMasked = true)

  test "the assembler should accept a masked frame when not rejecting (server-role use)":
    var a: WsAssembler
    let o = a.offer(Frame(fin: true, opcode: opText, payload: "hi", masked: true))
    check o.ready and o.message.data == "hi"

  test "the assembler should reject a continuation with no message in progress":
    var a: WsAssembler
    expect ValueError:
      discard a.offer(Frame(fin: true, opcode: opContinuation, payload: "x"))

  test "the assembler should reject a new data frame during a fragmented message":
    var a: WsAssembler
    check not a.offer(Frame(fin: false, opcode: opText, payload: "aa")).ready
    expect ValueError:
      discard a.offer(Frame(fin: true, opcode: opText, payload: "bb"))

  test "the assembler should reject a close frame with a 1-byte payload":
    var a: WsAssembler
    expect ValueError:
      discard a.offer(Frame(fin: true, opcode: opClose, payload: "\x03"))

  test "the assembler should reject an invalid close code":
    var a: WsAssembler
    expect ValueError:
      discard a.offer(Frame(fin: true, opcode: opClose, payload: closePayload(1005'u16)))

  test "the assembler should accept a valid close code":
    var a: WsAssembler
    let o = a.offer(Frame(fin: true, opcode: opClose, payload: closePayload(closeNormal)))
    check o.ready and o.message.closeCode == closeNormal

  test "an empty close frame surfaces as 1005, not 1000 (#244)":
    # RFC 6455 7.1.5: an absent status code must be reported as 1005 ("no status
    # received") so an application can distinguish it from an explicit normal
    # closure (1000). 1005 is never sent on the wire, only surfaced here.
    var a: WsAssembler
    let o = a.offer(Frame(fin: true, opcode: opClose, payload: ""))
    check o.ready
    check o.message.kind == wmClose
    check o.message.closeCode == closeNoStatus
    check o.message.closeCode != closeNormal

  test "maxMessageBytes should not carry one message's size into the next":
    var a: WsAssembler                    # regression: a.buf was not cleared between messages
    check a.offer(Frame(fin: true, opcode: opText, payload: "aaaaaa"),
                  maxMessageBytes = 8).ready
    let o = a.offer(Frame(fin: true, opcode: opText, payload: "bbbbbb"), maxMessageBytes = 8)
    check o.ready and o.message.data == "bbbbbb"

  test "the assembler should reject invalid UTF-8 in a text message":
    var a: WsAssembler
    expect ValueError:
      discard a.offer(Frame(fin: true, opcode: opText, payload: "\xff\xfe"))

  test "the assembler should accept valid multibyte UTF-8 in a text message":
    var a: WsAssembler
    let o = a.offer(Frame(fin: true, opcode: opText, payload: "caf\xc3\xa9"))  # "café"
    check o.ready and o.message.data == "caf\xc3\xa9"

  test "the assembler should not UTF-8-validate a binary message":
    var a: WsAssembler
    let o = a.offer(Frame(fin: true, opcode: opBinary, payload: "\xff\xfe\x00"))
    check o.ready and o.message.data == "\xff\xfe\x00"

  test "the assembler should reject invalid UTF-8 in a text message split across fragments":
    var a: WsAssembler
    check not a.offer(Frame(fin: false, opcode: opText, payload: "ok")).ready
    expect ValueError:
      discard a.offer(Frame(fin: true, opcode: opContinuation, payload: "\xff"))

  test "parseClose should split a close frame into its code and reason":
    let (code, reason) = parseClose(Frame(fin: true, opcode: opClose,
                                          payload: closePayload(closeGoingAway, "later")))
    check code == closeGoingAway
    check reason == "later"

  test "parseClose should report an absent code as 1005 and reject an invalid frame":
    template closeFrame(body: string): Frame =
      Frame(fin: true, opcode: opClose, payload: body)
    check parseClose(closeFrame("")).code == closeNoStatus
    expect ValueError:
      discard parseClose(closeFrame("\x03"))               # a 1-byte body
    expect ValueError:
      discard parseClose(closeFrame(closePayload(closeAbnormal)))   # never on the wire
    expect ValueError:
      discard parseClose(closeFrame(closePayload(closeNormal) & "\xff"))  # bad UTF-8

  test "the assembler should reject invalid UTF-8 in a close reason":
    var a: WsAssembler
    expect ValueError:
      discard a.offer(Frame(fin: true, opcode: opClose,
                            payload: closePayload(closeNormal, "\xff")))

suite "websocket handshake head cap (#406)":
  test "an endless non-CRLF 101 head should raise HeaderTooLargeError, not grow forever":
    # The handshake reader used to accumulate the 101 response until it saw a blank
    # line, with no bound and a whole-buffer rescan per read. An origin that answers
    # "HTTP/1.1 101 ...\r\nX: " and then streams non-CRLF bytes forever would grow
    # that buffer until the process died; maxResponseBytes caps only body bytes.
    var th: Thread[WsSrv]
    var port: int
    startWsHeaderFlood(th, port)

    let api = newNavi()
    var msg = ""
    try:
      discard api.websocket("ws://127.0.0.1:" & $port & "/chat")
    except HeaderTooLargeError as e:
      msg = e.msg
    check "maxHeaderListBytes" in msg
    joinThread(th)

suite "websocket incremental UTF-8 validation, chunk splits (#414)":
  # `scanUtf8` validates each chunk in place from an offset instead of joining the
  # carried bytes onto a copy of it, so the split points are where it can go wrong:
  # these check it against an independent whole-string reference at every offset.
  proc refValidUtf8(s: string): bool =
    ## Reference decoder (RFC 3629): decodes each code point by value and rejects
    ## overlongs, surrogates and anything above U+10FFFF from the value itself.
    var i = 0
    while i < s.len:
      let b = uint32(uint8(s[i]))
      var need: int
      var cp: uint32
      if b < 0x80'u32: (need, cp) = (0, b)
      elif b >= 0xC0'u32 and b < 0xE0'u32: (need, cp) = (1, b and 0x1F'u32)
      elif b >= 0xE0'u32 and b < 0xF0'u32: (need, cp) = (2, b and 0x0F'u32)
      elif b >= 0xF0'u32 and b < 0xF8'u32: (need, cp) = (3, b and 0x07'u32)
      else: return false
      if i + need >= s.len: return false
      for k in 1 .. need:
        let c = uint32(uint8(s[i + k]))
        if (c and 0xC0'u32) != 0x80'u32: return false
        cp = (cp shl 6) or (c and 0x3F'u32)
      if cp > 0x10FFFF'u32: return false
      if cp >= 0xD800'u32 and cp <= 0xDFFF'u32: return false
      if (need == 1 and cp < 0x80'u32) or (need == 2 and cp < 0x800'u32) or
         (need == 3 and cp < 0x10000'u32): return false          # overlong
      i += need + 1
    true

  proc chunkedValid(parts: varargs[string]): bool =
    ## Feed the parts through one scanner: valid only if no chunk was rejected and
    ## the stream did not end part-way through a code point (RFC 6455 8.1).
    var v: WsUtf8Scanner
    for p in parts:
      if not v.scanUtf8(p): return false
    not v.midCodePoint

  const samples = [
    "ab\xc3\xa9cd",                                   # 2-byte (U+00E9)
    "\xe2\x82\xac euro",                              # 3-byte (U+20AC)
    "hi \xf0\x9f\x92\xa9!",                           # 4-byte (U+1F4A9)
    "\xc2\x80\xdf\xbf\xe0\xa0\x80\xef\xbf\xbf" &
      "\xf0\x90\x80\x80\xf4\x8f\xbf\xbf",             # the range edges, back to back
    "\xed\x9f\xbf\xee\x80\x80",                       # U+D7FF / U+E000: around the surrogates
  ]

  test "a valid message should scan the same split across two chunks at every offset":
    for s in samples:
      check refValidUtf8(s)                          # the sample itself is well-formed
      for i in 0 .. s.len:
        check chunkedValid(s[0 ..< i], s[i .. ^1])

  test "a valid message should scan the same split across three chunks at every offset":
    for s in samples:
      for i in 0 .. s.len:
        for j in i .. s.len:
          check chunkedValid(s[0 ..< i], s[i ..< j], s[j .. ^1])

  test "an invalid message should be rejected at every split across two chunks":
    const bad = [
      "\xed\xa0\x80",             # U+D800: a surrogate
      "\xed\xbf\xbf",             # U+DFFF: a surrogate
      "\xc0\xaf",                 # overlong "/"
      "\xe0\x80\xaf",             # overlong "/" again
      "\xf0\x80\x80\xaf",         # and once more
      "\xf4\x90\x80\x80",         # U+110000: past the last code point
      "\xf5\x80\x80\x80",         # lead byte that can never start a sequence
      "a\xe2\x28\xa1b",           # a continuation byte that is not one
      "ok\xff!",                  # 0xFF is never valid UTF-8
      "\x80\xbf",                 # bare continuation bytes
    ]
    for s in bad:
      check not refValidUtf8(s)
      for i in 0 .. s.len:
        check not chunkedValid(s[0 ..< i], s[i .. ^1])
        for j in i .. s.len:
          check not chunkedValid(s[0 ..< i], s[i ..< j], s[j .. ^1])

  test "a message ending part-way through a code point should be rejected":
    for s in ["\xc3", "\xe2\x82", "\xf0\x9f\x92", "text \xf0"]:
      check not chunkedValid(s)                      # incomplete as one chunk
      for i in 0 .. s.len:                           # ... and however it is split
        check not chunkedValid(s[0 ..< i], s[i .. ^1])

  test "empty chunks should not disturb a carried code point":
    var v: WsUtf8Scanner
    check v.scanUtf8("\xf0")
    check v.scanUtf8("")                             # nothing to complete it with
    check v.midCodePoint
    check v.scanUtf8("\x9f\x92")
    check v.midCodePoint
    check v.scanUtf8("\xa9 done")
    check not v.midCodePoint

  test "a chunk should still be validated in place after completing a carry":
    # The bytes after the completed carry are scanned from an offset, so a fault
    # there must still be caught (and a clean tail must still pass).
    var ok: WsUtf8Scanner
    check ok.scanUtf8("\xe2\x82")
    check ok.scanUtf8("\xac plain ascii tail")
    check not ok.midCodePoint
    var bad: WsUtf8Scanner
    check bad.scanUtf8("\xe2\x82")
    check not bad.scanUtf8("\xac tail \xc3\x28")
