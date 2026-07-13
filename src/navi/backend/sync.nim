## Synchronous transport backend: blocking std/net sockets.
##
## `await` is an identity template here so the shared engine's `await`-shaped
## body compiles to straight-line blocking code.

import std/[net, os]
import ./api

export api

type
  Conn* = object
    socket: Socket

template await*(x: untyped): untyped = x

proc sleep*(ms: int) = os.sleep(ms)

proc connect*(host: string, port: int, tls: bool, cfg: TlsConfig): Conn =
  ## Dial `host:port` (IPv4 or IPv6, resolved by std/net), upgrading to TLS for
  ## https. TLS requires compiling with `-d:ssl` (OpenSSL).
  ##
  ## The socket is unbuffered: std/net's buffered `recv(pointer, size)` blocks
  ## until it fills the whole buffer, which deadlocks on a kept-alive connection
  ## where the response is smaller than the buffer. Unbuffered recv returns
  ## whatever bytes are available, which is the chunk semantics the engine wants.
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
