## Sans-io WebSocket core: RFC 6455 handshake + frame codec vectors.

import unittest
import std/[base64, strutils]
import navi/proto/ws
import ./support   # hexToBytes

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

# End-to-end: navi's sync WebSocket client against an in-process echo server
# built from the same sans-io core (server frames unmasked).
import std/[net, os]
import navi

var wsReady: bool

proc wsEcho(port: int) {.thread.} =
  # Unbuffered: recv returns whatever is available instead of blocking until the
  # requested count arrives (which would deadlock on small WebSocket frames).
  var server = newSocket(buffered = false)
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(port), "127.0.0.1")
  server.listen()
  wsReady = true
  var c: Socket
  server.accept(c)

  var head = ""
  while "\r\n\r\n" notin head: head.add c.recv(1)
  var key = ""
  for line in head.splitLines:
    let i = line.find(':')
    if i > 0 and cmpIgnoreCase(line[0 ..< i].strip, "sec-websocket-key") == 0:
      key = line[i + 1 .. ^1].strip
  c.send("HTTP/1.1 101 Switching Protocols\r\n" &
         "Upgrade: websocket\r\nConnection: Upgrade\r\n" &
         "Sec-WebSocket-Accept: " & acceptFor(key) & "\r\n\r\n")

  var dec: WsDecoder
  var running = true
  while running:
    var f: Frame
    while not dec.next(f):
      let chunk = c.recv(4096)
      if chunk.len == 0: running = false; break
      dec.feed(chunk)
    if not running: break
    case f.opcode
    of opText:
      if f.payload == "please fragment":       # reply as two fragments
        c.send(encodeFrame(opText, "frag", masked = false, fin = false))
        c.send(encodeFrame(opContinuation, "-ment", masked = false, fin = true))
      elif f.payload == "bye":                 # server-initiated close
        c.send(encodeFrame(opClose, closePayload(closeNormal), masked = false))
        running = false
      else:
        c.send(encodeFrame(opText, f.payload, masked = false))
    of opBinary:
      c.send(encodeFrame(opBinary, f.payload, masked = false))
    of opPing:
      c.send(encodeFrame(opPong, f.payload, masked = false))
    of opClose:
      running = false                          # client closed; just stop
    else: discard
  c.close()
  server.close()

suite "websocket client end to end":
  test "the WebSocket client should handshake, echo text and binary, reassemble fragments, and close":
    const port = 8996
    var th: Thread[int]
    createThread(th, wsEcho, port)
    while not wsReady: sleep(5)

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
