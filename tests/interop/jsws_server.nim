## Native WebSocket server (built from the sans-io core) for the navi/js interop
## test: navi/js connects to it under Node. One connection per scenario, chosen by
## the request path:
##
##   /chat            echo text and binary frames back
##   /close-code      close with an explicit code and reason (the client sees it)
##   /close-nostatus  close with an empty close frame (the client sees 1005)
##   /abrupt          drop the TCP connection with no close frame (the client sees 1006)
##
## Runs until killed, so the client can drive the scenarios in any order.
import std/[net, os, strutils]
import navi/proto/ws

const explicitCode* = 4001'u16   ## private-use close code the client asserts on

let port = parseInt(paramStr(1))
var server = newSocket(buffered = false)
server.setSockOpt(OptReuseAddr, true)
server.bindAddr(Port(port), "127.0.0.1")
server.listen()
stderr.writeLine("ready")

proc nextFrame(c: Socket, dec: var WsDecoder, f: var Frame): bool =
  ## The next frame, or false once the peer has gone away.
  while not dec.next(f):
    let chunk = c.recv(4096)
    if chunk.len == 0: return false
    dec.feed(chunk)
  true

proc awaitCloseEcho(c: Socket, dec: var WsDecoder) =
  ## Wait for the client's close echo (or EOF) before dropping the socket, so the
  ## close frame we just sent is read before the FIN reaches the runtime. Without
  ## this the client could report 1006 for a perfectly clean close.
  var f: Frame
  while c.nextFrame(dec, f):
    if f.opcode == opClose: break

proc handshake(c: Socket): string =
  ## Complete the RFC 6455 upgrade and return the requested path ("" on EOF).
  var head = ""
  while "\r\n\r\n" notin head:
    let b = c.recv(1)
    if b.len == 0: return ""
    head.add b
  var key = ""
  for line in head.splitLines:
    let i = line.find(':')
    if i > 0 and cmpIgnoreCase(line[0 ..< i].strip, "sec-websocket-key") == 0:
      key = line[i + 1 .. ^1].strip
  c.send("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n" &
         "Connection: Upgrade\r\nSec-WebSocket-Accept: " & acceptFor(key) & "\r\n\r\n")
  let parts = head.splitLines[0].split(' ')
  result = if parts.len > 1: parts[1] else: "/"

proc echoLoop(c: Socket, dec: var WsDecoder) =
  var f: Frame
  while c.nextFrame(dec, f):
    case f.opcode
    of opText, opBinary: c.send(encodeFrame(f.opcode, f.payload, masked = false))
    of opPing: c.send(encodeFrame(opPong, f.payload, masked = false))
    of opClose: break
    else: discard

while true:
  var c: Socket
  server.accept(c)
  let path = c.handshake()
  var dec: WsDecoder
  # Every close scenario waits for one client frame first, so the runtime has
  # certainly reached OPEN (and the client has a `stream()` pending) before the
  # connection goes away: an EOF racing the handshake would surface as a failed
  # open instead of the close code under test.
  var trigger: Frame
  case path
  of "/close-code":
    if c.nextFrame(dec, trigger):
      c.send(encodeFrame(opClose, closePayload(explicitCode, "so long"), masked = false))
      c.awaitCloseEcho(dec)
  of "/close-nostatus":
    if c.nextFrame(dec, trigger):
      c.send(encodeFrame(opClose, "", masked = false))   # no status code in the body
      c.awaitCloseEcho(dec)
  of "/abrupt":
    discard c.nextFrame(dec, trigger)                    # then drop with no close frame
  else:
    c.echoLoop(dec)
  c.close()
