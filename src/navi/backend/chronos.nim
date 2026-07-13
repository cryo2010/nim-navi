## Asynchronous transport backend built on chronos stream transports.
##
## Both plaintext and TLS connections read/write through AsyncStream
## reader/writer pairs, so the send/recv paths are identical. TLS uses
## chronos's BearSSL streams, which verify against the bundled Mozilla trust
## anchors by default (no system CA sourcing needed).

import pkg/chronos, pkg/chronos/transports/stream
import pkg/chronos/streams/[asyncstream, tlsstream]
import ./api

export api, chronos

type
  Conn* = object
    transport: StreamTransport
    reader: AsyncStreamReader
    writer: AsyncStreamWriter
    tls: TLSAsyncStream  ## kept alive for the connection's lifetime; nil if plaintext

proc connect*(host: string, port: int, tls: bool, cfg: TlsConfig): Future[Conn] {.async.} =
  let transport = await connect(resolveTAddress(host, Port(port))[0])
  result.transport = transport
  if tls:
    # caFile is not yet honored here; chronos/BearSSL uses its bundled Mozilla
    # anchors. Custom CA support for this backend is a follow-up.
    let flags =
      if cfg.verify: {} else: {TLSFlags.NoVerifyHost, TLSFlags.NoVerifyServerName}
    # This chronos/BearSSL build negotiates up to TLS 1.2 only.
    let stream = newTLSClientAsyncStream(
      newAsyncStreamReader(transport), newAsyncStreamWriter(transport), host,
      flags = flags)
    result.tls = stream
    result.reader = stream.reader
    result.writer = stream.writer
  else:
    result.reader = newAsyncStreamReader(transport)
    result.writer = newAsyncStreamWriter(transport)

proc sendAll*(c: Conn, data: string): Future[void] {.async.} =
  await c.writer.write(data)

proc recvSome*(c: Conn): Future[string] {.async.} =
  ## One chunk of up to 4096 bytes; "" means the peer closed.
  var buf = newString(4096)
  var n = 0
  try:
    n = await c.reader.readOnce(addr buf[0], buf.len)
  except AsyncStreamError:
    n = 0  # remote closed mid-stream; treat as EOF for the parser
  buf.setLen(n)
  result = buf

proc close*(c: Conn): Future[void] {.async.} =
  await c.writer.closeWait()
  await c.reader.closeWait()
  await c.transport.closeWait()
