## Async WebSocket client (navi/asyncdispatch) against an in-process echo server
## built from the same sans-io core (server frames unmasked).

import unittest
import std/[net, os, strutils]
import navi/asyncdispatch
import navi/proto/ws        # server-side codec helpers

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

suite "async websocket client end to end":
  test "handshakes, echoes text and binary, reassembles fragments, closes":
    const port = 8997
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
    joinThread(th)
