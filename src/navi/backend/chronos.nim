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
from ./happyeyeballs import heAttemptDelayMs

export api, chronos

type
  BodySink* = proc(data: string): Future[void] {.closure.}
    ## Streaming download sink for the chronos backend. Awaitable: the engine
    ## `await`s it, so a slow sink applies cooperative backpressure (stalling the
    ## per-read loop) rather than buffering the whole body in memory. Takes an owned
    ## `string` (navi's native body type, an 8-bit-clean byte buffer): the chunk
    ## crosses an `await` so it must be owned, not a borrowed view; being navi's own
    ## body type lets the engine move each chunk in with no copy.

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

proc newTlsCtxStore*(cfg: TlsConfig): RootRef =
  ## No-op for chronos: BearSSL does not build a shared OpenSSL `SSL_CTX`, so there
  ## is nothing to cache per client. Kept for a uniform interface across backends;
  ## the shared-context optimization applies to the OpenSSL backends. Always nil.
  discard cfg

proc closeTlsCtxStore*(store: RootRef) =
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

proc interleaveTAddr(addrs: seq[TransportAddress]): seq[TransportAddress] =
  ## RFC 8305 §4 family interleaving over resolved transport addresses, leading
  ## with the family the resolver put first.
  var v6, v4: seq[TransportAddress]
  for a in addrs:
    if a.family == AddressFamily.IPv6: v6.add a else: v4.add a
  let (x, y) =
    if addrs.len > 0 and addrs[0].family == AddressFamily.IPv6: (v6, v4) else: (v4, v6)
  var i = 0
  while i < x.len or i < y.len:
    if i < x.len: result.add x[i]
    if i < y.len: result.add y[i]
    inc i

proc discardLoser(f: Future[StreamTransport]) {.async.} =
  ## Cancel a losing Happy Eyeballs attempt; if it had already connected, close the
  ## transport so a late winner-loser tie does not leak it.
  try: await f.cancelAndWait()
  except CatchableError: discard
  if f.completed:
    try: await f.read().closeWait()
    except CatchableError: discard

proc happyConnect*(addrs: seq[TransportAddress]):
    Future[tuple[transport: StreamTransport, idx: int]] {.async.} =
  ## Happy Eyeballs (RFC 8305): start chronos connects to `addrs` (interleaved by
  ## family) staggered by ~250ms and return the (transport, index) of the first to
  ## complete, so a slow or blackholed address does not stall the others. Losing
  ## attempts are cancelled (chronos structured cancellation reclaims them cleanly).
  if addrs.len == 0:
    raise newException(IOError, "navi: no address to connect to")
  var
    inflight: seq[tuple[fut: Future[StreamTransport], idx: int]]
    nextIdx = 0
    lastStart = Moment.now()
    lastErr: ref CatchableError
  try:
    while true:
      # Start the next attempt: the first at once; the rest when nothing is in
      # flight or the stagger window has elapsed.
      if nextIdx < addrs.len and
         (inflight.len == 0 or Moment.now() - lastStart >= heAttemptDelayMs.milliseconds):
        let f: Future[StreamTransport] = connect(addrs[nextIdx])
        inflight.add (f, nextIdx)
        inc nextIdx
        lastStart = Moment.now()
        continue
      if inflight.len == 0:
        break                       # nothing pending and nothing left to start
      # Wait for any attempt to finish, or -- if attempts remain -- the stagger
      # window, whichever is first.
      var cands: seq[FutureBase]
      for e in inflight: cands.add FutureBase(e.fut)
      var timer: Future[void] = nil
      if nextIdx < addrs.len:
        timer = sleepAsync(heAttemptDelayMs.milliseconds)
        cands.add FutureBase(timer)
      discard await race(cands)
      if timer != nil and not timer.finished: await timer.cancelAndWait()
      # Harvest finished attempts: first success wins; failures are dropped.
      var i = 0
      while i < inflight.len:
        let e = inflight[i]
        if e.fut.finished:
          if e.fut.completed:
            let t = e.fut.read()
            inflight.delete(i)
            for other in inflight: asyncSpawn discardLoser(other.fut)  # cancel losers
            return (t, e.idx)
          else:                       # failed or cancelled
            lastErr = e.fut.error
            inflight.delete(i)
        else:
          inc i
  except CatchableError:
    for e in inflight: asyncSpawn discardLoser(e.fut)
    raise
  if lastErr != nil: raise lastErr
  raise newException(IOError, "navi: could not connect")

proc connect*(host: string, port: int, tls: bool, cfg: TlsConfig,
              proxy: ProxyTarget, alpn: seq[string] = @[],
              connectMs = 0, readMs = 0, totalMs = 0): Future[Conn] {.async.} =
  ## `connectMs` bounds establishment (TCP connect + TLS handshake); `readMs` is
  ## stored for per-read timeouts. `totalMs` is enforced by the chronos entry's
  ## guard (structured cancellation), so it is unused here.
  discard totalMs
  # BearSSL uses a fixed cipher profile; reject cipher selection up front rather
  # than after dialing.
  if tls and (cfg.ciphers.len > 0 or cfg.cipherSuites.len > 0):
    raise newException(ValueError,
      "navi: the chronos backend (BearSSL) does not support cipher selection")
  var conn: Conn
  conn.readMs = readMs

  proc establish() {.async.} =
    let dialHost = if proxy.isSet: proxy.host else: host
    let dialPort = if proxy.isSet: proxy.port else: port
    var pool = interleaveTAddr(resolveTAddress(dialHost, Port(dialPort)))
    if pool.len == 0:
      raise newException(IOError, "navi: could not resolve " & dialHost)
    var lastErr: ref CatchableError
    # Happy-Eyeballs TCP race, then proxy/TLS on the winner; on a *handshake*
    # failure drop that address and re-race the rest (handshake-aware fallback).
    while pool.len > 0:
      let (transport, idx) = await happyConnect(pool)
      conn.transport = transport
      # Close the transport if the CONNECT tunnel or TLS setup fails (or the connect
      # times out -- withTimeout cancels this future, raising CancelledError here).
      try:
        if proxy.isSet and tls:
          await proxyConnect(transport, host, port)
        if tls:
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
        return                                     # established
      except CatchableError as e:
        try: await transport.closeWait()
        except CatchableError: discard
        conn.tls = nil; conn.reader = nil; conn.writer = nil; conn.caStore = nil
        pool.delete(idx)
        lastErr = e
    raise lastErr

  if connectMs > 0:
    if not await withTimeout(establish(), connectMs.milliseconds):
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
      if not await withTimeout(fut, c.readMs.milliseconds):
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

proc closeSync*(c: Conn) =
  ## Synchronous close, for a destructor that cannot `await` (an abandoned
  ## streaming handle reclaimed by GC). chronos's non-`Wait` `close` initiates
  ## teardown and returns; the event loop frees the resources afterwards. Same
  ## teardown as `close`, minus the awaits.
  if not c.writer.isNil: c.writer.close()
  if not c.reader.isNil: c.reader.close()
  if not c.transport.isNil: c.transport.close()

proc sleep*(ms: int): Future[void] {.async.} =
  await sleepAsync(ms.milliseconds)
