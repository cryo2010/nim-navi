## A WebSocket-over-TLS (wss) echo server for the wss backend examples. Same as
## echo_server.nim but each accepted connection is wrapped in server-side TLS
## before the WebSocket handshake. A self-signed cert for localhost is generated
## on first run (via openssl), so this just works out of the box. Each connection
## runs on its own thread, so overlapping clients work -- in particular a browser
## refresh, which briefly runs two connections at once.
##
##   nim c -r -d:ssl examples/websocket/wss_echo_server.nim
##
## Listens on wss://127.0.0.1:9701/. Ctrl-C to stop.

when not defined(ssl):
  {.error: "compile the wss echo server with -d:ssl (OpenSSL)".}

import std/[net, strutils, os, osproc, sequtils, openssl]
import navi/proto/ws

# std/net's `sessionIdContext=` binds SSL_CTX_set_session_id_context with a Nim
# `string` where C wants `const unsigned char*`, so it hands OpenSSL the string
# object instead of its bytes and the context is never actually set. A client
# that resumes a TLS session (e.g. a browser tab on reload) then fails the
# handshake with "session id context uninitialized". Bind it correctly.
proc setSessionIdContext(ctx: SslCtx, sid: cstring, len: cuint): cint
  {.cdecl, dynlib: DLLSSLName, importc: "SSL_CTX_set_session_id_context".}

const port = 9701
let
  certDir = getTempDir() / "navi-wss-demo"
  # Defaults to a self-signed cert (auto-generated, fine for the native clients).
  # For the browser demo, point these at a browser-trusted cert -- e.g. mkcert:
  #   mkcert -install && mkcert localhost 127.0.0.1
  #   NAVI_WSS_CERT=localhost+1.pem NAVI_WSS_KEY=localhost+1-key.pem nim c -r -d:ssl ...
  certFile = getEnv("NAVI_WSS_CERT", certDir / "cert.pem")
  keyFile = getEnv("NAVI_WSS_KEY", certDir / "key.pem")

proc ensureCert() =
  if fileExists(certFile) and fileExists(keyFile): return
  if existsEnv("NAVI_WSS_CERT") or existsEnv("NAVI_WSS_KEY"):
    quit("NAVI_WSS_CERT / NAVI_WSS_KEY set but the files were not found")
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

proc handle(a: tuple[c: Socket, ctx: SslContext]) {.thread.} =
  ## Own the connection end to end so the accept loop is never blocked: the TLS
  ## handshake runs here too. ctx is passed in (not read as a global) to keep the
  ## thread GC-safe; OpenSSL's SSL_CTX is safe to share across threads.
  try:
    a.ctx.wrapConnectedSocket(a.c, handshakeAsServer)   # server-side TLS handshake
    serveConn(a.c)
  except CatchableError as e:
    echo "connection error: ", e.msg
  a.c.close()

ensureCert()
let ctx = newContext(certFile = certFile, keyFile = keyFile)
# Server-side OpenSSL needs a session-id context once a client (e.g. a browser)
# attempts TLS session resumption; without it the handshake fails.
const sidCtx = "navi-wss-demo"
if setSessionIdContext(ctx.context, sidCtx.cstring, sidCtx.len.cuint) != 1:
  quit("could not set the TLS session-id context")
var server = newSocket(buffered = false)
server.setSockOpt(OptReuseAddr, true)
# NAVI_WS_HOST=0.0.0.0 lets Docker's published port reach the server.
server.bindAddr(Port(port), getEnv("NAVI_WS_HOST", "127.0.0.1"))
server.listen()
echo "WSS echo server on wss://127.0.0.1:", port, "/  (Ctrl-C to stop)"

var threads: seq[ref Thread[tuple[c: Socket, ctx: SslContext]]]
while true:
  var c: Socket
  server.accept(c)
  threads.keepItIf(it[].running)   # drop handlers whose connection has closed
  let t = new(Thread[tuple[c: Socket, ctx: SslContext]])
  threads.add t
  createThread(t[], handle, (c, ctx))
