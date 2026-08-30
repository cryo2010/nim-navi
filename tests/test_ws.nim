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

# End-to-end: navi's sync WebSocket client against an in-process echo server
# built from the same sans-io core (server frames unmasked).
import std/[net, os]
import navi
import navi/core/response   # navi's TimeoutError (qualified; std/net has one too)

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

proc wsSilent(port: int) {.thread.} =
  ## Handshake, then never respond (ignore pings). Reads and discards until the
  ## client closes -- so a client with keepalive must time out and drop us.
  var srv = newSocket(buffered = false)
  srv.setSockOpt(OptReuseAddr, true)
  srv.bindAddr(Port(port), "127.0.0.1")
  srv.listen()
  wsReady = true
  var c: Socket
  srv.accept(c)
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
  while c.recv(4096).len > 0: discard    # swallow the pings; never pong
  c.close(); srv.close()

proc wsPingCounter(port: int) {.thread.} =
  ## Handshake, pong every ping, and after the second ping send a text message --
  ## so a client with keepalive stays alive across pings and finally receives it.
  var srv = newSocket(buffered = false)
  srv.setSockOpt(OptReuseAddr, true)
  srv.bindAddr(Port(port), "127.0.0.1")
  srv.listen()
  wsReady = true
  var c: Socket
  srv.accept(c)
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
  var pings = 0
  var running = true
  while running:
    var f: Frame
    while not dec.next(f):
      let chunk = c.recv(4096)
      if chunk.len == 0: running = false; break
      dec.feed(chunk)
    if not running: break
    case f.opcode
    of opPing:
      c.send(encodeFrame(opPong, f.payload, masked = false))
      inc pings
      if pings == 2: c.send(encodeFrame(opText, "alive", masked = false))
    of opClose: running = false
    else: discard
  c.close(); srv.close()

suite "websocket client end to end":
  test "the WebSocket client should handshake, echo text and binary, reassemble fragments, and close":
    const port = 9240
    wsReady = false   # reset so a looped run waits for THIS server, not a stale flag
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

  test "receive should raise WsMessageTooLarge when a message exceeds maxMessageBytes":
    const port = 9241
    wsReady = false
    var th: Thread[int]
    createThread(th, wsEcho, port)
    while not wsReady: sleep(5)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat", maxMessageBytes = 8)
    ws.send(repeat("x", 100))                  # sending is not capped; the echo is
    expect WsMessageTooLarge:
      discard ws.receive()                     # 100-byte echo > 8-byte cap -> raises + 1009
    ws.close()                                 # idempotent no-op: already dropped on 1009
    joinThread(th)

suite "websocket keepalive":
  test "receive should raise TimeoutError when keepalive gets no response":
    const port = 9242
    wsReady = false
    var th: Thread[int]
    createThread(th, wsSilent, port)
    while not wsReady: sleep(5)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat", keepAlive = 40)
    expect response.TimeoutError:
      discard ws.receive()                     # ping at 40ms, dead at 80ms (no pong)
    joinThread(th)

  test "keepalive should keep the connection alive across pings until a message arrives":
    const port = 9243
    wsReady = false
    var th: Thread[int]
    createThread(th, wsPingCounter, port)
    while not wsReady: sleep(5)

    let api = newNavi()
    let ws = api.websocket("ws://127.0.0.1:" & $port & "/chat", keepAlive = 40)
    let m = ws.receive()                       # pinged twice (each ponged), then "alive"
    check m.kind == wmText
    check m.data == "alive"
    ws.close()
    joinThread(th)
