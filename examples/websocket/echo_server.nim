## A tiny WebSocket echo server for the backend examples, built from navi's
## sans-io core (`navi/proto/ws`). Every message it receives, it sends straight
## back. Serves one connection at a time in a loop, so you can run each example
## client against it in turn (and point a browser at it for the navi/js one).
##
##   nim c -r examples/websocket/echo_server.nim
##
## Listens on ws://127.0.0.1:9700/. Ctrl-C to stop.

import std/[net, strutils]
import navi/proto/ws

const port = 9700

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

var server = newSocket(buffered = false)   # unbuffered: recv returns available bytes
server.setSockOpt(OptReuseAddr, true)
server.bindAddr(Port(port), "127.0.0.1")
server.listen()
echo "WebSocket echo server on ws://127.0.0.1:", port, "/  (Ctrl-C to stop)"

while true:
  var c: Socket
  server.accept(c)
  try: serveConn(c)
  except CatchableError as e: echo "connection error: ", e.msg
  c.close()
