## Asynchronous transport backend built on chronos stream transports.

import pkg/chronos, pkg/chronos/transports/stream
import ./api

export api, chronos

type
  Conn* = object
    transport: StreamTransport

proc connect*(host: string, port: int, tls: bool, cfg: TlsConfig): Future[Conn] {.async.} =
  result.transport = await connect(resolveTAddress(host, Port(port))[0])

proc sendAll*(c: Conn, data: string): Future[void] {.async.} =
  discard await c.transport.write(data)

proc recvSome*(c: Conn): Future[string] {.async.} =
  ## One chunk of up to 4096 bytes; "" means the peer closed.
  var buf = newString(4096)
  let n = await c.transport.readOnce(addr buf[0], buf.len)
  buf.setLen(n)
  result = buf

proc close*(c: Conn): Future[void] {.async.} =
  await c.transport.closeWait()
