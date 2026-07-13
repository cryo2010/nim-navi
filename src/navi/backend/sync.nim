## Synchronous transport backend: blocking std/net sockets.
##
## `await` is an identity template here so the shared engine's `await`-shaped
## body compiles to straight-line blocking code.

import std/net
import ./api

export api

type
  Conn* = object
    socket: Socket

template await*(x: untyped): untyped = x

proc connect*(host: string, port: int, tls: bool, cfg: TlsConfig): Conn =
  ## Dial `host:port` (IPv4 or IPv6, resolved by std/net). TLS lands next.
  result.socket = dial(host, Port(port))

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
