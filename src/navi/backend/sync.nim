## Synchronous transport backend: blocking std/net sockets.
##
## `await` is an identity template here so the shared engine's `await`-shaped
## body compiles to straight-line blocking code.

import std/[net, os, strutils]
import ./api

export api

type
  Conn* = object
    socket: Socket

template await*(x: untyped): untyped = x

proc sleep*(ms: int) = os.sleep(ms)

proc proxyConnect(socket: Socket, host: string, port: int) =
  ## Establish a CONNECT tunnel to `host:port` through an already-dialed proxy.
  let target = host & ":" & $port
  socket.send("CONNECT " & target & " HTTP/1.1\r\nHost: " & target & "\r\n\r\n")
  var resp = newString(1024)
  let n = socket.recv(addr resp[0], resp.len)
  resp.setLen(max(n, 0))
  if not resp.startsWith("HTTP/1.1 200") and not resp.startsWith("HTTP/1.0 200"):
    raise newException(ValueError, "navi: proxy CONNECT failed: " & resp.splitLines()[0])

proc connect*(host: string, port: int, tls: bool, cfg: TlsConfig,
              proxy: ProxyTarget): Conn =
  ## Dial `host:port` (IPv4 or IPv6, resolved by std/net), upgrading to TLS for
  ## https. Through a proxy, https targets get a CONNECT tunnel and http targets
  ## are dialed directly to the proxy (the engine sends an absolute-URI).
  ## TLS requires compiling with `-d:ssl` (OpenSSL).
  ##
  ## The socket is unbuffered: std/net's buffered `recv(pointer, size)` blocks
  ## until it fills the whole buffer, which deadlocks on a kept-alive connection
  ## where the response is smaller than the buffer.
  if proxy.isSet:
    result.socket = dial(proxy.host, Port(proxy.port), buffered = false)
    if tls:
      proxyConnect(result.socket, host, port)
  else:
    result.socket = dial(host, Port(port), buffered = false)
  if tls:
    when defined(ssl):
      let ctx = newContext(
        verifyMode = if cfg.verify: CVerifyPeer else: CVerifyNone,
        caFile = cfg.caFile)
      ctx.wrapConnectedSocket(result.socket, handshakeAsClient, host)
    else:
      raise newException(ValueError,
        "navi: https requires compiling with -d:ssl")

proc sendAll*(c: Conn, data: string) =
  c.socket.send(data)

proc recvSome*(c: Conn): string =
  ## One chunk of up to 4096 bytes; "" means the peer closed.
  result = newString(4096)
  let n = c.socket.recv(addr result[0], result.len)
  if n <= 0:
    result.setLen(0)
  else:
    result.setLen(n)

proc close*(c: Conn) =
  c.socket.close()
