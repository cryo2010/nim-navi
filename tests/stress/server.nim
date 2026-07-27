## TLS test server for the backend stress harness (tests/stress). Speaks HTTP/1.1
## over TLS on one thread per connection, and handles two things:
##
##   - any HTTP method on /echo: replies 200 with a small JSON body echoing the
##     request method, the `x-stress` header (proof a middleware ran), and the
##     received body length. HEAD replies headers only, with the Content-Length a
##     GET would have carried, so it exercises the client's HEAD handling.
##   - a WebSocket upgrade on /ws: RFC 6455 handshake, then echoes every frame
##     until the peer closes.
##
## Connections are keep-alive, so a pooling client reuses one socket for many
## requests. Self-signed cert for localhost is generated on first run.
##
##   NAVI_STRESS_HOST=0.0.0.0 NAVI_STRESS_PORT=9443 nim c -r -d:ssl tests/stress/server.nim

when not defined(ssl):
  {.error: "compile the stress server with -d:ssl (OpenSSL)".}

import std/[net, strutils, os, osproc, sequtils, openssl]
import navi/proto/ws

proc setSessionIdContext(ctx: SslCtx, sid: cstring, len: cuint): cint
  {.cdecl, dynlib: DLLSSLName, importc: "SSL_CTX_set_session_id_context".}

let
  host = getEnv("NAVI_STRESS_HOST", "127.0.0.1")
  port = parseInt(getEnv("NAVI_STRESS_PORT", "9443"))
  certDir = getTempDir() / "navi-stress"
  certFile = getEnv("NAVI_STRESS_CERT", certDir / "cert.pem")
  keyFile = getEnv("NAVI_STRESS_KEY", certDir / "key.pem")

proc ensureCert() =
  if fileExists(certFile) and fileExists(keyFile): return
  createDir(certFile.parentDir)
  discard execProcess("openssl", args = [
    "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "365",
    "-keyout", keyFile, "-out", certFile, "-subj", "/CN=localhost",
    # DNS:127.0.0.1 (as well as the IP SAN) so chronos's BearSSL, which matches
    # the connect host against dNSName SANs and not IP SANs, accepts the loopback
    # IP; OpenSSL (sync/asyncdispatch) matches the IP SAN directly.
    "-addext", "subjectAltName=DNS:localhost,DNS:127.0.0.1,IP:127.0.0.1"],
    options = {poUsePath, poStdErrToStdOut})
  if not (fileExists(certFile) and fileExists(keyFile)):
    quit("could not generate a self-signed cert (is openssl installed?)")

proc recvHead(c: Socket): string =
  ## Read up to the end of the request/response head (blank line).
  while "\r\n\r\n" notin result:
    let b = c.recv(1)
    if b.len == 0: return ""       # peer closed
    result.add b

proc headerValue(head, name: string): string =
  for line in head.splitLines:
    let i = line.find(':')
    if i > 0 and cmpIgnoreCase(line[0 ..< i].strip, name) == 0:
      return line[i + 1 .. ^1].strip

proc serveWs(c: Socket, head: string) =
  let key = headerValue(head, "sec-websocket-key")
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
    of opText, opBinary: c.send(encodeFrame(f.opcode, f.payload, masked = false))
    of opPing: c.send(encodeFrame(opPong, f.payload, masked = false))
    of opClose: return
    else: discard

proc serveHttp(c: Socket, firstHead: string) =
  ## Keep-alive HTTP loop: reply to each request until the peer closes. The reply
  ## echoes the request method and `x-stress` header (as x-echo-* response
  ## headers, proof the verb and middleware reached us) and the request body
  ## verbatim, reflecting its Content-Type and Content-Length.
  var head = firstHead
  while head.len > 0:
    let meth = head.splitLines[0].split(' ')[0]
    let stress = headerValue(head, "x-stress")
    let ct = headerValue(head, "content-type")
    var body = ""
    let cl = headerValue(head, "content-length")
    if cl.len > 0:
      let n = parseInt(cl)
      while body.len < n:                        # read the request body to echo it
        let chunk = c.recv(n - body.len)
        if chunk.len == 0: return
        body.add chunk
    var resp = "HTTP/1.1 200 OK\r\nx-echo-method: " & meth &
               "\r\nx-echo-stress: " & stress & "\r\nConnection: keep-alive\r\n"
    if meth == "HEAD":
      # Headers only, with a non-zero Content-Length: exercises the client's HEAD
      # handling (it must not wait for a body). No body follows.
      resp.add "Content-Type: application/octet-stream\r\nContent-Length: 24\r\n\r\n"
      c.send(resp)
    else:
      if ct.len > 0: resp.add "Content-Type: " & ct & "\r\n"
      resp.add "Content-Length: " & $body.len & "\r\n\r\n" & body
      c.send(resp)
    head = recvHead(c)

proc handle(a: tuple[c: Socket, ctx: SslContext]) {.thread.} =
  try:
    a.ctx.wrapConnectedSocket(a.c, handshakeAsServer)
    let head = recvHead(a.c)
    if head.len > 0:
      if cmpIgnoreCase(headerValue(head, "upgrade"), "websocket") == 0:
        serveWs(a.c, head)
      else:
        serveHttp(a.c, head)
  except CatchableError:
    discard
  a.c.close()

ensureCert()
let ctx = newContext(certFile = certFile, keyFile = keyFile)
const sidCtx = "navi-stress"
if setSessionIdContext(ctx.context, sidCtx.cstring, sidCtx.len.cuint) != 1:
  quit("could not set the TLS session-id context")
var server = newSocket(buffered = false)
server.setSockOpt(OptReuseAddr, true)
server.bindAddr(Port(port), host)
server.listen()
echo "stress server on https://", host, ":", port, "/ (Ctrl-C to stop)"

var threads: seq[ref Thread[tuple[c: Socket, ctx: SslContext]]]
while true:
  var c: Socket
  server.accept(c)
  threads.keepItIf(it[].running)
  let t = new(Thread[tuple[c: Socket, ctx: SslContext]])
  threads.add t
  createThread(t[], handle, (c, ctx))
