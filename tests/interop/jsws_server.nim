## Native WebSocket echo server (built from the sans-io core) for the navi/js
## interop test: navi/js connects to it under Node. Serves one connection.
import std/[net, os, strutils]
import navi/proto/ws

let port = parseInt(paramStr(1))
var server = newSocket(buffered = false)
server.setSockOpt(OptReuseAddr, true)
server.bindAddr(Port(port), "127.0.0.1")
server.listen()
stderr.writeLine("ready")
var c: Socket
server.accept(c)

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
var running = true
while running:
  var f: Frame
  while not dec.next(f):
    let chunk = c.recv(4096)
    if chunk.len == 0: running = false; break
    dec.feed(chunk)
  if not running: break
  case f.opcode
  of opText, opBinary: c.send(encodeFrame(f.opcode, f.payload, masked = false))
  of opPing: c.send(encodeFrame(opPong, f.payload, masked = false))
  of opClose: running = false
  else: discard
c.close(); server.close()
