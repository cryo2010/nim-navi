## Asynchronous transport backend built on chronos stream transports.
##
## Both plaintext and TLS connections read/write through AsyncStream
## reader/writer pairs, so the send/recv paths are identical. TLS uses
## chronos's BearSSL streams, which verify against the bundled Mozilla trust
## anchors by default, or a custom CA when TlsConfig.caFile is set.

import std/[strutils, os]
import pkg/chronos, pkg/chronos/transports/stream
import pkg/chronos/streams/[asyncstream, tlsstream]
import ./api, ./chronos_castore
import ../core/response  # for navi's TimeoutError

export api, chronos

proc chronosVer(v: api.TlsVersion,
                whenDefault: tlsstream.TLSVersion): tlsstream.TLSVersion =
  ## Map a navi `TlsVersion` to chronos's (`TLSVersion` collides by name, so both
  ## are qualified); BearSSL here tops out at TLS 1.2.
  case v
  of tlsDefault: whenDefault
  of tls10: TLS10
  of tls11: TLS11
  of tls12: TLS12
  of tls13:
    raise newException(ValueError,
      "navi: the chronos backend (BearSSL) supports up to TLS 1.2; tls13 is unavailable")

type
  Conn* = object
    transport: StreamTransport
    reader: AsyncStreamReader
    writer: AsyncStreamWriter
    tls: TLSAsyncStream  ## kept alive for the connection's lifetime; nil if plaintext
    caStore: CaTrustStore  ## keeps custom-CA anchors alive; BearSSL holds raw pointers into it
    protocol*: string    ## ALPN protocol; always "" here (this backend is http/1.1)
    readMs: int          ## per-read stall timeout in ms; 0 blocks indefinitely

proc newTlsStore*(cfg: TlsConfig): RootRef =
  ## No-op for chronos: BearSSL client-side session resumption is not available in
  ## the chronos versions navi targets (the session-cache API is server-only, and
  ## `TLSSessionCache.init` does not compile on some releases). Kept for a uniform
  ## client interface across backends; TLS resumption applies to the OpenSSL
  ## backends (sync, asyncdispatch). Always nil.
  discard cfg

proc closeTlsStore*(store: RootRef) =
  discard store

proc proxyConnect(transport: StreamTransport, host: string, port: int) {.async.} =
  let target = host & ":" & $port
  discard await transport.write(
    "CONNECT " & target & " HTTP/1.1\r\nHost: " & target & "\r\n\r\n")
  var buf = newString(1024)
  let n = await transport.readOnce(addr buf[0], buf.len)
  buf.setLen(n)
  if not (buf.startsWith("HTTP/1.1 200") or buf.startsWith("HTTP/1.0 200")):
    raise newException(ValueError, "navi: proxy CONNECT failed: " & buf.splitLines()[0])

proc connect*(host: string, port: int, tls: bool, cfg: TlsConfig,
              proxy: ProxyTarget, alpn: seq[string] = @[],
              connectMs = 0, readMs = 0, totalMs = 0): Future[Conn] {.async.} =
  ## `connectMs` bounds establishment (TCP connect + TLS handshake); `readMs` is
  ## stored for per-read timeouts. `totalMs` is enforced by the chronos entry's
  ## guard (structured cancellation), so it is unused here.
  discard totalMs
  var conn: Conn
  conn.readMs = readMs

  proc establish() {.async.} =
    let dialAddr =
      if proxy.isSet: resolveTAddress(proxy.host, Port(proxy.port))[0]
      else: resolveTAddress(host, Port(port))[0]
    let transport = await connect(dialAddr)
    conn.transport = transport
    # Close the transport if the CONNECT tunnel or TLS setup fails (or the connect
    # times out -- withTimeout cancels this future, raising CancelledError here).
    try:
      if proxy.isSet and tls:
        await proxyConnect(transport, host, port)
      if tls:
        # BearSSL uses a fixed cipher profile; cipher selection is not available.
        if cfg.ciphers.len > 0 or cfg.cipherSuites.len > 0:
          raise newException(ValueError,
            "navi: the chronos backend (BearSSL) does not support cipher selection")
        # Client certificates are not honored here (BearSSL client presents none).
        let flags =
          if cfg.wantsVerify: {} else: {TLSFlags.NoVerifyHost, TLSFlags.NoVerifyServerName}
        if cfg.wantsVerify and cfg.caFile.len > 0:
          if not fileExists(cfg.caFile):
            raise newException(IOError, "navi: CA file not found: " & cfg.caFile)
          conn.caStore = loadCaTrustStore(readFile(cfg.caFile))
        let rdr = newAsyncStreamReader(transport)
        let wtr = newAsyncStreamWriter(transport)
        # BearSSL tops out at TLS 1.2. Unpinned, keep chronos's 1.2-only default;
        # when the user pins a bound, widen the other end to the extremes so the
        # requested range is honored. No client session resumption here.
        let pinned = cfg.minVersion != tlsDefault or cfg.maxVersion != tlsDefault
        let vmin = if pinned: chronosVer(cfg.minVersion, TLS10) else: TLS12
        let vmax = if pinned: chronosVer(cfg.maxVersion, TLS12) else: TLS12
        let stream =
          if conn.caStore != nil:
            newTLSClientAsyncStream(rdr, wtr, host, flags = flags,
                                    minVersion = vmin, maxVersion = vmax,
                                    trustAnchors = conn.caStore.store)
          else:
            newTLSClientAsyncStream(rdr, wtr, host, flags = flags,
                                    minVersion = vmin, maxVersion = vmax)
        conn.tls = stream
        conn.reader = stream.reader
        conn.writer = stream.writer
        # Drive the handshake now so a verification failure raises here, not mid-read.
        await stream.handshake()
      else:
        conn.reader = newAsyncStreamReader(transport)
        conn.writer = newAsyncStreamWriter(transport)
    except CatchableError:
      try: await transport.closeWait()
      except CatchableError: discard
      raise

  if connectMs > 0:
    if not await withTimeout(establish(), connectMs):
      raise newException(response.TimeoutError,
                         "navi: connect timed out after " & $connectMs & " ms")
  else:
    await establish()
  return conn

proc sendAll*(c: Conn, data: string): Future[void] {.async.} =
  await c.writer.write(data)

proc recvSome*(c: Conn): Future[string] {.async.} =
  ## One chunk of up to 4096 bytes; "" means the peer closed. Bounded by `readMs`
  ## (the per-read stall timeout) when set; on expiry the read is cancelled and
  ## TimeoutError is raised.
  var buf = newString(4096)
  var n = 0
  try:
    if c.readMs > 0:
      let fut = c.reader.readOnce(addr buf[0], buf.len)
      if not await withTimeout(fut, c.readMs):
        raise newException(response.TimeoutError,
                           "navi: read timed out after " & $c.readMs & " ms")
      n = await fut
    else:
      n = await c.reader.readOnce(addr buf[0], buf.len)
  except TLSStreamError:
    raise  # a real TLS/handshake failure is not an EOF -- surface it, don't spin
  except AsyncStreamError:
    n = 0  # remote closed mid-stream; treat as EOF for the parser
  buf.setLen(n)
  result = buf

proc close*(c: Conn): Future[void] {.async.} =
  await c.writer.closeWait()
  await c.reader.closeWait()
  await c.transport.closeWait()

proc sleep*(ms: int): Future[void] {.async.} =
  await sleepAsync(ms.milliseconds)
