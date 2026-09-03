## Async WebSocket client (navi/asyncdispatch) against an in-process echo server
## built from the same sans-io core (server frames unmasked).

import unittest
import std/[net, os, strutils]
import navi/asyncdispatch
import navi/proto/ws        # server-side codec helpers
import navi/core/response   # navi's TimeoutError (qualified; std/net has one too)

var wsReady: bool

proc wsEcho(port: int) {.thread.} =
  # Unbuffered so recv returns available bytes instead of blocking for a full
  # buffer (which deadlocks on small frames).
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
      if f.payload == "please fragment":
        c.send(encodeFrame(opText, "frag", masked = false, fin = false))
        c.send(encodeFrame(opContinuation, "-ment", masked = false, fin = true))
      elif f.payload == "bye":
        c.send(encodeFrame(opClose, closePayload(closeNormal), masked = false))
        running = false
      else:
        c.send(encodeFrame(opText, f.payload, masked = false))
    of opBinary:
      c.send(encodeFrame(opBinary, f.payload, masked = false))
    of opPing:
      c.send(encodeFrame(opPong, f.payload, masked = false))
    of opClose:
      running = false
    else: discard
  c.close()
  server.close()

proc wsSilent(port: int) {.thread.} =
  ## Handshake, then never respond (ignore pings), reading and discarding until the
  ## client gives up -- so a client with keepalive must time out and drop us.
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
  # An abrupt client close (the keepalive-death path) can surface as a recv error
  # rather than EOF; swallow it so the thread never dies by unhandled exception
  # (which trips AddressSanitizer's join check).
  try:
    while c.recv(4096).len > 0: discard
  except CatchableError: discard
  c.close(); server.close()

proc wsStreamEcho(port: int) {.thread.} =
  ## Reassembles messages and, on "fragment", replies with a 3-fragment message;
  ## otherwise echoes the whole message as one frame.
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
  var asmb: WsAssembler
  var running = true
  while running:
    var f: Frame
    while not dec.next(f):
      let chunk = c.recv(4096)
      if chunk.len == 0: running = false; break
      dec.feed(chunk)
    if not running: break
    let o = asmb.offer(f)
    case o.reply
    of wrPong: c.send(encodeFrame(opPong, o.replyPayload, masked = false))
    of wrCloseEcho: running = false
    of wrNone: discard
    if o.ready:
      case o.message.kind
      of wmText:
        if o.message.data == "fragment":
          c.send(encodeFrame(opText, "one", masked = false, fin = false))
          c.send(encodeFrame(opContinuation, "-two", masked = false, fin = false))
          c.send(encodeFrame(opContinuation, "-three", masked = false, fin = true))
        else:
          c.send(encodeFrame(opText, o.message.data, masked = false))
      of wmBinary: c.send(encodeFrame(opBinary, o.message.data, masked = false))
      of wmClose: running = false
  c.close(); server.close()

suite "async websocket client end to end":
  test "the WebSocket client should handshake, echo text and binary, reassemble fragments, and close":
    const port = 9241
    wsReady = false   # reset so a looped run waits for THIS server, not a stale flag
    var th: Thread[int]
    createThread(th, wsEcho, port)
    while not wsReady: os.sleep(5)

    proc run() {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat")

      await ws.send("hello")
      let m1 = await ws.receive()
      check m1.kind == wmText
      check m1.data == "hello"

      await ws.send("\x00\x01\x02 bytes", binary = true)
      let m2 = await ws.receive()
      check m2.kind == wmBinary
      check m2.data == "\x00\x01\x02 bytes"

      await ws.send("please fragment")
      let m3 = await ws.receive()
      check m3.kind == wmText
      check m3.data == "frag-ment"

      await ws.send("bye")
      let m4 = await ws.receive()
      check m4.kind == wmClose
      check m4.closeCode == closeNormal
      await ws.close()

    waitFor run()

  test "keepalive should raise TimeoutError when the peer never responds":
    const port = 9244
    wsReady = false
    var th: Thread[int]
    createThread(th, wsSilent, port)
    while not wsReady: os.sleep(5)

    proc run2() {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat", keepAlive = 40)
      expect response.TimeoutError:
        discard await ws.receive()             # ping at 40ms, dead at 80ms (no pong)

    waitFor run2()
    joinThread(th)

  test "stream()/stream(writer) should read and write a message as fragments":
    const port = 9251
    wsReady = false
    var th: Thread[int]
    createThread(th, wsStreamEcho, port)
    while not wsReady: os.sleep(5)

    proc run3() {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat")

      await ws.send("fragment")                  # server replies with 3 fragments
      let reader = await ws.stream()
      check reader.kind == wmText
      var chunks: seq[string]
      reader.each(chunk):
        chunks.add chunk
      check chunks == @["one", "-two", "-three"]

      ws.stream(writer):                         # streamed upload; server echoes whole
        for part in @["aa", "bb", "cc"]:
          await writer.write(part)
      let m = await ws.receive()
      check m.kind == wmText
      check m.data == "aabbcc"
      await ws.close()

    waitFor run3()
    joinThread(th)

suite "WebSocket transport selection (asyncdispatch)":
  test "websocket over {H2} on a non-TLS URL should raise ProtocolError":
    # {H2} excludes h1; h2 needs TLS, so a ws:// (plaintext) target has no usable
    # transport -> ProtocolError before any connection is attempted.
    proc run() {.async.} =
      var cfg = initNaviConfig()
      cfg.http = {H2}
      let api = newNavi(cfg)
      var raised = false
      try: discard await api.websocket("ws://127.0.0.1:1/never")
      except ProtocolError: raised = true
      check raised
      await api.close()
    waitFor run()
