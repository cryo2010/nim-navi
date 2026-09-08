## Sans-io WebSocket core: RFC 6455 handshake + frame codec vectors.

import unittest
import std/[base64, strutils]
import navi/proto/ws
import ./support      # hexToBytes
import ./support_ws   # shared WebSocket test servers (WsSrv / startWs*)

suite "websocket handshake":
  test "the handshake should compute the accept key from the client key (RFC 6455 1.3)":
    check acceptFor("dGhlIHNhbXBsZSBub25jZQ==") == "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="

  test "the handshake should generate a fresh 16-byte base64 nonce key":
    check base64.decode(genKey()).len == 16
    check genKey() != genKey()

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

  test "the assembler should reject invalid UTF-8 in a close reason":
    var a: WsAssembler
    expect ValueError:
      discard a.offer(Frame(fin: true, opcode: opClose,
                            payload: closePayload(closeNormal, "\xff")))
