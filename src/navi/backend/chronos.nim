## Asynchronous transport backend built on chronos stream transports.
##
## Plaintext connections read/write through an AsyncStream reader/writer pair; TLS
## connections run OpenSSL over the raw chronos `StreamTransport` via the
## memory-BIO pump in `chronos_tls` (so the backend reaches full parity with the
## sync/asyncdispatch OpenSSL backends: ALPN + HTTP/2, TLS 1.3, cipher selection,
## mTLS, and session resumption). TLS therefore requires compiling with `-d:ssl`
## (it links OpenSSL, exactly as the other native backends do).

import std/[strutils, base64]
import pkg/chronos, pkg/chronos/transports/stream
import pkg/chronos/streams/asyncstream
import ./api
import ./tls_store, ./tunnel, ./timing
import ../core/response  # for navi's TimeoutError
import ../core/socks

const shutdownGraceMs = 1000
  ## upper bound on the FIN handshake `gracefulShutdown` waits for before `close`
  ## falls through to `closesocket` anyway
from ./happyeyeballs import heAttemptDelayMs
when defined(ssl):
  import ./openssl_ctx, ./chronos_tls

export api, chronos, tls_store

type
  BodySink* = proc(data: string): Future[void] {.closure.}
    ## Streaming download sink for the chronos backend. Awaitable: the engine
    ## `await`s it, so a slow sink applies cooperative backpressure (stalling the
    ## per-read loop) rather than buffering the whole body in memory. Takes an owned
    ## `string` (navi's native body type, an 8-bit-clean byte buffer): the chunk
    ## crosses an `await` so it must be owned, not a borrowed view; being navi's own
    ## body type lets the engine move each chunk in with no copy.

  GatedBodySink* = proc(data: string): Future[bool] {.closure.}
    ## A response sink for `request()` that can stop the download early. Like
    ## `BodySink` it receives decoded body chunks of the FINAL surfaced response and
    ## is awaited (backpressure), but returns `Future[bool]`: `true` keeps the
    ## transfer going, `false` stops it cleanly (the request returns normally with
    ## `res.body == ""` and `res.bodyTruncated == true`). Only the final response's
    ## body is delivered; redirect/retry/digest/thrown-error bodies never reach it. A
    ## bare closure (no chronos raises annotation, portable spelling); the wrap site
    ## discharges chronos's strict gcsafe/raises obligation with a cast.

  AsyncBodyProducer* = proc(): Future[string] {.closure.}
    ## Pull-based upload source for the chronos backend: returns the next body chunk
    ## (or "" at end of body). The engine `await`s each call, so producing a chunk may
    ## itself await -- e.g. reading from a streaming download to pipe it into the
    ## upload in constant memory. The async analog of the sync `BodyProducer`
    ## (`core/request.nim`); its Future type is backend-specific, so it lives here
    ## rather than on `Request` and threads through the send paths (mirroring
    ## `BodySink`). A bare closure so a plain `{.async.}` proc assigns to it directly
    ## (no chronos raises annotation, portable spelling); the send site discharges
    ## chronos's strict gcsafe/raises obligation with a cast, as the sink path does.
    ## Not replayable, like `bodyStream`.

type
  ParkedRead = object
    ## One-slot holder for an in-flight read abandoned by an expired `recvWithin`
    ## (see `Conn.parked`). At most one read is ever outstanding per connection, so
    ## a single slot is the whole bookkeeping.
    fut: Future[string]

  Conn* = object
    transport: StreamTransport
    reader: AsyncStreamReader  ## plaintext only; nil for TLS
    writer: AsyncStreamWriter  ## plaintext only; nil for TLS
    when defined(ssl):
      tls: ChronosTls          ## OpenSSL pump; nil if plaintext
      ctx: SslContext          ## the (usually shared) context this connection used
      ownsCtx: bool            ## true only for an unshared ctx `close` must destroy
    protocol*: string    ## negotiated ALPN protocol ("h2" / "http/1.1" / "")
    readMs: int          ## per-read stall timeout in ms; 0 blocks indefinitely
    parked: ref ParkedRead
                         ## the read a `recvWithin` started and abandoned when its
                         ## bound expired. chronos CAN cancel a read, but a cancelled
                         ## `readOnce` gives no way to recover bytes it had already
                         ## taken off the transport, so the read is parked and resumed
                         ## instead -- the same contract as the asyncdispatch backend,
                         ## which has no cancellation at all. A `ref` so every value
                         ## copy of the Conn shares the one slot.

when defined(ssl):
  # openssl_ctx builds contexts through std/net, whose procs are declared
  # `raises: [Exception]`. chronos's `{.async.}` tracks effects strictly and
  # forbids a bare `Exception`, so these thin wrappers narrow it to navi's
  # CatchableError contract at the backend boundary.
  proc obtainCtx(cfg: TlsConfig, alpn: seq[string]): tuple[ctx: SslContext, owned: bool] =
    try:
      result = obtainContext(cfg.contextStore, cfg, alpn)
    except CatchableError as e:
      raise e
    except Exception as e:
      raise newException(IOError, "navi: TLS context setup failed: " & e.msg)

  proc destroyCtx(ctx: SslContext) =
    try: ctx.destroyContext()
    except CatchableError: discard
    except Exception: discard

# Byte-I/O primitives the shared tunnel drivers (backend/tunnel) mix in.
proc sockWrite(transport: StreamTransport, s: string) {.async.} =
  discard await transport.write(s)

proc sockReadExactly(transport: StreamTransport, n: int): Future[string] {.async.} =
  ## Read exactly `n` bytes or raise; SOCKS5 replies are fixed-size frames.
  var buf = newString(n)
  var off = 0
  while off < n:
    let r = await transport.readOnce(addr buf[off], n - off)
    if r <= 0: raise newException(IOError, "navi: SOCKS5 proxy closed the connection")
    off += r
  buf

proc sockReadSome(transport: StreamTransport, max: int): Future[string] {.async.} =
  ## One read of up to `max` bytes (the proxy CONNECT reply fits in one read).
  var buf = newString(max)
  let n = await transport.readOnce(addr buf[0], max)
  buf.setLen(n)
  buf

proc proxyConnect(transport: StreamTransport, host: string, port: int,
                  user, pass: string) {.async.} =
  proxyConnectDriver(transport, host, port, user, pass)

proc socksConnect(transport: StreamTransport, host: string, port: int,
                  user, pass: string) {.async.} =
  socksConnectDriver(transport, host, port, user, pass)

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
        # Disable Nagle to match the sync/asyncdispatch backends: without it a
        # streamed upload's trailing partial segments stall on delayed-ACK (~40ms
        # each), collapsing throughput by ~10x.
        let f: Future[StreamTransport] =
          connect(addrs[nextIdx], flags = {SocketFlags.TcpNoDelay})
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
  ## guard (structured cancellation), so it is unused here. `alpn` (e.g.
  ## @["h2","http/1.1"]) is offered on the TLS handshake; the negotiated protocol
  ## lands in `Conn.protocol`. TLS requires `-d:ssl`.
  discard totalMs
  var conn: Conn
  conn.readMs = readMs
  conn.parked = new(ParkedRead)   # empty; filled only by an expired `recvWithin`

  proc establish() {.async.} =
    if proxy.kind == pkUnix:
      # A Unix path (leading '/') builds a Unix TransportAddress; chronos dials it
      # like any StreamTransport. TLS still layers over it using the URL host.
      let transport = await connect(initTAddress(proxy.host))
      conn.transport = transport
      if tls:
        when defined(ssl):
          let (ctx, owned) = obtainCtx(cfg, alpn)
          var ok = false
          try:
            let slot = resumeSlot(cfg, host & ":" & $port)
            let tlsc = newChronosTls(transport, ctx, host, slot)
            conn.tls = tlsc
            await tlsc.handshake()
            verifyPeer(tlsc.sslPtr, host, cfg.wantsVerify)
            postHandshakeVerify(tlsc.sslPtr, host, cfg)
            conn.protocol = negotiatedProtocol(tlsc.sslPtr)
            conn.ctx = ctx
            conn.ownsCtx = owned
            ok = true
          finally:
            if owned and not ok and not ctx.isNil: destroyCtx(ctx)
        else:
          raise newException(ValueError,
            "navi: the chronos backend requires -d:ssl for https")
      else:
        conn.reader = newAsyncStreamReader(transport)
        conn.writer = newAsyncStreamWriter(transport)
      return
    let dialHost = if proxy.isSet: proxy.host else: host
    let dialPort = if proxy.isSet: proxy.port else: port
    var pool = interleaveTAddr(resolveTAddress(dialHost, Port(dialPort)))
    if pool.len == 0:
      raise newException(IOError, "navi: could not resolve " & dialHost)

    when not defined(ssl):
      if tls:
        raise newException(ValueError,
          "navi: the chronos backend requires -d:ssl for https")
      var lastErr: ref CatchableError
      while pool.len > 0:
        let (transport, idx) = await happyConnect(pool)
        conn.transport = transport
        try:
          conn.reader = newAsyncStreamReader(transport)
          conn.writer = newAsyncStreamWriter(transport)
          return
        except CatchableError as e:
          (try: await transport.closeWait() except CatchableError: discard)
          conn.reader = nil; conn.writer = nil
          pool.delete(idx); lastErr = e
      raise lastErr
    else:
      # Build the shared TLS context once (reused across address attempts); free it
      # only if we own it (a bare TlsConfig) and never handed it to a live conn.
      var ctx: SslContext
      var owned = false
      if tls: (ctx, owned) = obtainCtx(cfg, alpn)
      var keepCtx = false
      try:
        var lastErr: ref CatchableError
        # Happy-Eyeballs TCP race, then proxy/TLS on the winner; on a handshake
        # failure drop that address and re-race the rest (handshake-aware fallback).
        while pool.len > 0:
          let (transport, idx) = await happyConnect(pool)
          conn.transport = transport
          try:
            # SOCKS5 tunnels every target; an HTTP proxy tunnels only https (CONNECT).
            if proxy.kind == pkSocks5:
              await socksConnect(transport, host, port, proxy.user, proxy.pass)
            elif proxy.isSet and tls:
              await proxyConnect(transport, host, port, proxy.user, proxy.pass)
            if tls:
              let slot = resumeSlot(cfg, host & ":" & $port)
              let tlsc = newChronosTls(transport, ctx, host, slot)
              conn.tls = tlsc
              # Drive the handshake now so a verification failure raises here, not
              # mid-read; verifyPeer re-checks the chain + hostname/IP identity.
              await tlsc.handshake()
              verifyPeer(tlsc.sslPtr, host, cfg.wantsVerify)
              postHandshakeVerify(tlsc.sslPtr, host, cfg)   # SPKI pin + verify callback
              conn.protocol = negotiatedProtocol(tlsc.sslPtr)
              conn.ctx = ctx
              conn.ownsCtx = owned
              keepCtx = owned          # the conn owns it now; don't free below
            else:
              conn.reader = newAsyncStreamReader(transport)
              conn.writer = newAsyncStreamWriter(transport)
            return                                   # established
          except CatchableError as e:
            if not conn.tls.isNil:
              await conn.tls.close()                 # frees ssl + transport
              conn.tls = nil
            else:
              (try: await transport.closeWait() except CatchableError: discard)
            conn.reader = nil; conn.writer = nil
            pool.delete(idx); lastErr = e
        raise lastErr
      finally:
        if tls and owned and not keepCtx and not ctx.isNil:
          destroyCtx(ctx)

  if connectMs > 0:
    if not await withTimeout(establish(), connectMs.milliseconds):
      raise newException(response.TimeoutError, connectTimeoutMsg(connectMs))
  else:
    await establish()
  return conn

proc sendAll*(c: Conn, data: string): Future[void] {.async.} =
  when defined(ssl):
    if not c.tls.isNil:
      await c.tls.write(data)
      return
  # A conn with neither a live TLS session nor a plaintext writer is closed/half-
  # established (e.g. a connection whose ALPN never resolved under event-loop
  # starvation, mis-routed onto the h1 path): writing would deref a nil
  # AsyncStreamWriter and SIGSEGV. Raise a typed transport error instead, so the
  # h1 write-time classifier tears the conn down and retries on a fresh one rather
  # than crashing the whole loop (surfaced by the h2 headerbomb chaos mode).
  if c.writer.isNil:
    raise newException(IOError, "navi: send on a closed connection")
  await c.writer.write(data)

proc plaintextRead(c: Conn): Future[string] {.async.} =
  var buf = newStringUninit(naviReadBufSize)   # overwritten by readOnce then setLen(n): no zero-fill
  var n = 0
  try:
    n = await c.reader.readOnce(addr buf[0], buf.len)
  except AsyncStreamError:
    n = 0  # remote closed mid-stream; treat as EOF for the parser
  buf.setLen(n)
  result = buf

proc rearm*(c: var Conn, readMs = 0, totalMs = 0) =
  ## Re-apply the current config's read timeout to a connection taken from the idle
  ## pool, so a reused connection honors navi's live-config contract rather than the
  ## value it was opened with (issue #360). `totalMs` is accepted for signature parity
  ## with the sync backend but ignored: the chronos entry's outer `guard` enforces the
  ## whole-request deadline, so there is no per-conn deadline to re-arm here.
  discard totalMs
  c.readMs = readMs

proc startRead(c: Conn): Future[string] =
  ## Begin one read, resuming the one a previous `recvWithin` parked rather than
  ## starting a second read on the same transport (which would race it for the bytes).
  if not c.parked.isNil and not c.parked.fut.isNil:
    result = c.parked.fut
    c.parked.fut = nil
    return
  when defined(ssl):
    if not c.tls.isNil:
      return c.tls.readSome()
  plaintextRead(c)

proc recvSome*(c: Conn): Future[string] {.async.} =
  ## One chunk; "" means the peer closed. Bounded by `readMs` (the per-read stall
  ## timeout) when set; on expiry the read is cancelled and TimeoutError is raised
  ## (terminal: the caller tears the connection down). A read parked by an earlier
  ## `recvWithin` is resumed here, so its bytes reach this caller instead of being lost.
  let readFut = c.startRead()
  if c.readMs > 0:
    if not await withTimeout(readFut, c.readMs.milliseconds):
      raise newException(response.TimeoutError, readTimeoutMsg(c.readMs))
  result = await readFut

proc recvWithin*(c: Conn, ms: int): Future[tuple[timedOut: bool, data: string]] {.async.} =
  ## A bounded read that leaves the connection usable when it expires: waits at most
  ## `ms` for a chunk, otherwise reports `timedOut` with the read PARKED rather than
  ## cancelled. The `Expect: 100-continue` gate uses it to wait for the interim
  ## response and then keep reading the same connection, which a plain `recvSome` +
  ## read timeout cannot do (that one is terminal).
  ##
  ## `withTimeout` is deliberately not used: it cancels the loser, and a cancelled
  ## `readOnce` (or a cancelled OpenSSL pump mid-`feedIn`) can drop bytes it had
  ## already taken off the transport, desyncing the response that follows. `race`
  ## leaves both futures alone, so the unfinished read is parked intact and the next
  ## read resumes it.
  if ms <= 0: return (true, "")
  let readFut = c.startRead()
  let timer = sleepAsync(ms.milliseconds)
  discard await race(readFut, timer)
  if not readFut.finished():
    if not c.parked.isNil: c.parked.fut = readFut
    return (true, "")
  timer.cancelSoon()               # the loser: drop it from the timer heap
  return (false, await readFut)

proc discardParked(c: Conn) =
  ## Retire a parked read on teardown: nobody will await it, so cancel it rather
  ## than leave an in-flight future on a transport that is about to be freed.
  if c.parked.isNil or c.parked.fut.isNil: return
  let fut = c.parked.fut
  c.parked.fut = nil
  if not fut.finished(): fut.cancelSoon()

proc gracefulShutdown*(transport: StreamTransport) {.async.} =
  ## Send FIN before the socket is closed, so bytes already written are delivered
  ## ahead of the close. chronos's `closeWait` calls `closesocket` straight away,
  ## and on Windows a socket closed that way can drop the last write on the floor
  ## (a WebSocket close frame written right before `close` arrived at the peer as a
  ## bare EOF); Microsoft's own guidance is to `shutdown` first, which is what the
  ## asyncdispatch client's `shutdownConn` already does. Best effort and bounded:
  ## a peer that never drains its receive buffer must not turn `close` into a hang.
  if transport.isNil: return
  try:
    discard await withTimeout(transport.shutdownWait(), shutdownGraceMs.milliseconds)
  except CatchableError: discard          # already reset / closed: nothing to flush

proc close*(c: Conn): Future[void] {.async.} =
  c.discardParked()
  when defined(ssl):
    if not c.tls.isNil:
      await c.tls.close()   # frees the SSL (and its BIOs) and the transport
      if c.ownsCtx and not c.ctx.isNil: destroyCtx(c.ctx)
      return
  if not c.writer.isNil: await c.writer.closeWait()
  await gracefulShutdown(c.transport)
  if not c.reader.isNil: await c.reader.closeWait()
  if not c.transport.isNil: await c.transport.closeWait()

proc closeSync*(c: Conn) =
  ## Synchronous close, for a destructor that cannot `await` (an abandoned
  ## streaming handle reclaimed by GC). chronos's non-`Wait` `close` initiates
  ## teardown and returns; the event loop frees the resources afterwards.
  c.discardParked()
  when defined(ssl):
    if not c.tls.isNil:
      c.tls.closeSync()
      if c.ownsCtx and not c.ctx.isNil: destroyCtx(c.ctx)
      return
  if not c.writer.isNil: c.writer.close()
  if not c.reader.isNil: c.reader.close()
  if not c.transport.isNil: c.transport.close()

proc shutdownConn*(c: Conn) =
  ## Initiate transport close without awaiting, to unblock a reader parked on a
  ## pending read (used by the h2 mux's `close`); the reader then observes EOF.
  when defined(ssl):
    if not c.tls.isNil:
      c.tls.shutdownTransport(); return
  if not c.transport.isNil: c.transport.close()

proc sleep*(ms: int): Future[void] {.async.} =
  await sleepAsync(ms.milliseconds)
