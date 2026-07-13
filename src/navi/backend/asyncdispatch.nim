## Asynchronous transport backend built on std/asyncnet.

import std/[asyncdispatch, asyncnet, net]
import ./api

export api, asyncdispatch

type
  Conn* = object
    socket: AsyncSocket

proc connect*(host: string, port: int, tls: bool, cfg: TlsConfig): Future[Conn] {.async.} =
  ## Dial `host:port`, upgrading to TLS for https. TLS requires `-d:ssl`.
  ## Unbuffered so recv returns the available chunk instead of blocking to fill
  ## the buffer, which would deadlock on a kept-alive connection (same reason as
  ## the sync backend).
  let socket = await asyncnet.dial(host, Port(port), buffered = false)
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
