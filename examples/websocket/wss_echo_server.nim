## A WebSocket-over-TLS (wss) echo server for the wss backend examples. Same as
## echo_server.nim but each accepted connection is wrapped in server-side TLS
## before the WebSocket handshake. A self-signed cert for localhost is generated
## on first run (via openssl), so this just works out of the box.
##
##   nim c -r -d:ssl examples/websocket/wss_echo_server.nim
##
## Listens on wss://127.0.0.1:9701/. Ctrl-C to stop.

when not defined(ssl):
  {.error: "compile the wss echo server with -d:ssl (OpenSSL)".}

import std/[net, strutils, os, osproc]
import navi/proto/ws

const port = 9701
let
  certDir = getTempDir() / "navi-wss-demo"
  certFile = certDir / "cert.pem"
  keyFile = certDir / "key.pem"

proc ensureCert() =
  if fileExists(certFile) and fileExists(keyFile): return
  createDir(certDir)
  discard execProcess("openssl", args = [
    "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "365",
    "-keyout", keyFile, "-out", certFile,
    "-subj", "/CN=localhost",
    "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"],
    options = {poUsePath, poStdErrToStdOut})
  if not (fileExists(certFile) and fileExists(keyFile)):
    quit("could not generate a self-signed cert (is openssl installed?)")
  echo "generated a self-signed cert in ", certDir

proc serveConn(c: Socket) =
  ## Complete the WebSocket handshake, then echo frames until the peer closes.
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
      if chunk.len == 0: return
      dec.feed(chunk)
    case f.opcode
    of opText, opBinary:
      echo "echo ", f.opcode, ": ", f.payload
      c.send(encodeFrame(f.opcode, f.payload, masked = false))
    of opPing:
      c.send(encodeFrame(opPong, f.payload, masked = false))
    of opClose:
      return
    else: discard

ensureCert()
let ctx = newContext(certFile = certFile, keyFile = keyFile)
var server = newSocket(buffered = false)
server.setSockOpt(OptReuseAddr, true)
# NAVI_WS_HOST=0.0.0.0 lets Docker's published port reach the server.
server.bindAddr(Port(port), getEnv("NAVI_WS_HOST", "127.0.0.1"))
server.listen()
echo "WSS echo server on wss://127.0.0.1:", port, "/  (Ctrl-C to stop)"

while true:
  var c: Socket
  server.accept(c)
  try:
    ctx.wrapConnectedSocket(c, handshakeAsServer)   # server-side TLS handshake
    serveConn(c)
  except CatchableError as e:
    echo "connection error: ", e.msg
  c.close()
