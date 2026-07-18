## A tiny WebSocket echo server for the backend examples, built from navi's
## sans-io core (`navi/proto/ws`). Every message it receives, it sends straight
## back. Each connection is handled on its own thread, so overlapping clients
## work -- in particular the interactive browser page holds its socket open, and
## a refresh briefly runs two connections at once.
##
##   nim c -r examples/websocket/echo_server.nim
##
## Listens on ws://127.0.0.1:9700/. Ctrl-C to stop.

import std/[net, strutils, os, sequtils]
import navi/proto/ws

const port = 9700
# Loopback by default; set NAVI_WS_HOST=0.0.0.0 to accept connections forwarded
# from outside (e.g. Docker port publishing, so a browser on the host can reach it).
let bindHost = getEnv("NAVI_WS_HOST", "127.0.0.1")

proc serveConn(c: Socket) =
  ## Complete the handshake, then echo frames until the peer closes.
  var head = ""
  while "\r\n\r\n" notin head: head.add c.recv(1)
  var key = ""
  for line in head.splitLines:
    let i = line.find(':')
    if i > 0 and cmpIgnoreCase(line[0 ..< i].strip, "sec-websocket-key") == 0:
      key = line[i + 1 .. ^1].strip
  c.send("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n" &
         "Connection: Upgrade\r\nSec-WebSocket-Accept: " & acceptFor(key) & "\r\n\r\n")

  var dec: WsDecoder
  while true:
    var f: Frame
    while not dec.next(f):
      let chunk = c.recv(4096)
      if chunk.len == 0: return          # peer went away
      dec.feed(chunk)
    case f.opcode
    of opText, opBinary:
      echo "echo ", f.opcode, ": ", f.payload
      c.send(encodeFrame(f.opcode, f.payload, masked = false))   # server frames unmasked
    of opPing:
      c.send(encodeFrame(opPong, f.payload, masked = false))
    of opClose:
      return
    else: discard

proc handle(c: Socket) {.thread.} =
  try: serveConn(c)
  except CatchableError as e: echo "connection error: ", e.msg
  c.close()

var server = newSocket(buffered = false)   # unbuffered: recv returns available bytes
server.setSockOpt(OptReuseAddr, true)
server.bindAddr(Port(port), bindHost)
server.listen()
echo "WebSocket echo server on ws://", bindHost, ":", port, "/  (Ctrl-C to stop)"

var threads: seq[ref Thread[Socket]]
while true:
  var c: Socket
  server.accept(c)
  threads.keepItIf(it[].running)   # drop handlers whose connection has closed
  let t = new(Thread[Socket])
  threads.add t
  createThread(t[], handle, c)
