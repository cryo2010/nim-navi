## Chronos WebSocket client (navi/chronos) against an in-process echo server
## built from the same sans-io core (server frames unmasked).

import unittest
import std/[net, os, strutils]
import pkg/chronos
import navi/chronos
import navi/proto/ws        # server-side codec helpers

var wsReady: bool
var stallReady: bool

proc wsStall(port: int) {.thread.} =
  ## Accept the connection and read the upgrade request, but never send the 101
  ## response, so the client's open blocks until its timeout fires.
  var server = newSocket(buffered = false)
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(port), "127.0.0.1")
  server.listen()
  stallReady = true
  var c: Socket
  server.accept(c)
  var head = ""
  try:
    while "\r\n\r\n" notin head: head.add c.recv(1)
  except CatchableError: discard
  os.sleep(2000)   # hold past the client's timeout, then tear down
  c.close()
  server.close()

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

suite "chronos websocket client end to end":
  test "handshakes, echoes text and binary, reassembles fragments, closes":
    const port = 8998
    var th: Thread[int]
    createThread(th, wsEcho, port)
    while not wsReady: os.sleep(5)

    # Checks live in the sync body: chronos's strict exception tracking rejects
    # unittest's `check` (which can raise) inside an {.async.} proc.
    proc run(): Future[seq[WsMessage]] {.async.} =
      let api = newNavi()
      let ws = await api.websocket("ws://127.0.0.1:" & $port & "/chat")
      await ws.send("hello")
      result.add await ws.receive()
      await ws.send("\x00\x01\x02 bytes", binary = true)
      result.add await ws.receive()
      await ws.send("please fragment")
      result.add await ws.receive()
      await ws.send("bye")
      result.add await ws.receive()
      await ws.close()

    let m = waitFor run()
    joinThread(th)
    check m.len == 4
    check m[0].kind == wmText and m[0].data == "hello"
    check m[1].kind == wmBinary and m[1].data == "\x00\x01\x02 bytes"
    check m[2].kind == wmText and m[2].data == "frag-ment"
    check m[3].kind == wmClose and m[3].closeCode == closeNormal

  test "open times out when the server never completes the handshake":
    const port = 8997
    var th: Thread[int]
    createThread(th, wsStall, port)
    while not stallReady: os.sleep(5)

    proc run(): Future[string] {.async.} =
      let api = newNavi(NaviOptions(timeout: some(600)))
      try:
        discard await api.websocket("ws://127.0.0.1:" & $port & "/")
        return "opened"
      except CatchableError as e:
        # navi raises its own TimeoutError; match by name to avoid the
        # std/net vs navi ambiguity on the bare type.
        return (if $e.name == "TimeoutError": "timeout" else: "other:" & $e.name)

    let outcome = waitFor run()
    joinThread(th)
    check outcome == "timeout"
