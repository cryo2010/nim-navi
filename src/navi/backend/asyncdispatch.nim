## Asynchronous transport backend built on std/asyncnet.

import std/[asyncdispatch, asyncnet]
import ./api

export api, asyncdispatch

type
  Conn* = object
    socket: AsyncSocket

proc connect*(host: string, port: int, tls: bool, cfg: TlsConfig): Future[Conn] {.async.} =
  result.socket = await asyncnet.dial(host, Port(port))

proc sendAll*(c: Conn, data: string): Future[void] =
  c.socket.send(data)

proc recvSome*(c: Conn): Future[string] =
  ## One chunk of up to 4096 bytes; "" means the peer closed.
  c.socket.recv(4096)

proc close*(c: Conn): Future[void] {.async.} =
  c.socket.close()
