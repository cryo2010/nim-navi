## Asynchronous transport backend built on std/asyncnet.

import std/[asyncdispatch, asyncnet, net, strutils]
import ./api

export api, asyncdispatch

type
  Conn* = object
    socket: AsyncSocket

proc proxyConnect(socket: AsyncSocket, host: string, port: int) {.async.} =
  let target = host & ":" & $port
  await socket.send("CONNECT " & target & " HTTP/1.1\r\nHost: " & target & "\r\n\r\n")
  let resp = await socket.recv(1024)
  if not resp.startsWith("HTTP/1.1 200") and not resp.startsWith("HTTP/1.0 200"):
    raise newException(ValueError, "navi: proxy CONNECT failed: " & resp.splitLines()[0])

proc connect*(host: string, port: int, tls: bool, cfg: TlsConfig,
              proxy: ProxyTarget): Future[Conn] {.async.} =
  ## Dial `host:port` (or the proxy), upgrading to TLS for https with a CONNECT
  ## tunnel when proxied. Unbuffered so recv returns the available chunk instead
  ## of blocking to fill the buffer. TLS requires `-d:ssl`.
  let socket =
    if proxy.isSet: await asyncnet.dial(proxy.host, Port(proxy.port), buffered = false)
    else: await asyncnet.dial(host, Port(port), buffered = false)
  if proxy.isSet and tls:
    await proxyConnect(socket, host, port)
  if tls:
    when defined(ssl):
      let ctx = newContext(
        verifyMode = if cfg.verify: CVerifyPeer else: CVerifyNone,
        caFile = cfg.caFile)
      ctx.wrapConnectedSocket(socket, handshakeAsClient, host)
    else:
      raise newException(ValueError,
        "navi: https requires compiling with -d:ssl")
  result.socket = socket

proc sendAll*(c: Conn, data: string): Future[void] =
  c.socket.send(data)

proc recvSome*(c: Conn): Future[string] =
  ## One chunk of up to 4096 bytes; "" means the peer closed.
  c.socket.recv(4096)

proc close*(c: Conn): Future[void] {.async.} =
  c.socket.close()

proc sleep*(ms: int): Future[void] = sleepAsync(ms)
