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
import ../core/response  # for navi's TimeoutError
import ../core/socks
from ./happyeyeballs import heAttemptDelayMs
when defined(ssl):
  import ./openssl_ctx, ./chronos_tls

export api, chronos

type
  BodySink* = proc(data: string): Future[void] {.closure.}
    ## Streaming download sink for the chronos backend. Awaitable: the engine
    ## `await`s it, so a slow sink applies cooperative backpressure (stalling the
    ## per-read loop) rather than buffering the whole body in memory. Takes an owned
    ## `string` (navi's native body type, an 8-bit-clean byte buffer): the chunk
    ## crosses an `await` so it must be owned, not a borrowed view; being navi's own
    ## body type lets the engine move each chunk in with no copy.

type
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

proc newTlsStore*(cfg: TlsConfig): RootRef =
  ## The per-client TLS session cache, or nil when resumption is off or on a
  ## non-`-d:ssl` build. The entry puts it on `config.tls.sessionCache`.
  when defined(ssl):
    if cfg.wantsResume: result = newTlsSessionCache()
  else:
    discard cfg

proc closeTlsStore*(store: RootRef) =
  when defined(ssl):
    if not store.isNil: close(cast[TlsSessionCache](store))
  else:
    discard store

proc newTlsCtxStore*(cfg: TlsConfig): RootRef =
  ## The per-client shared TLS-context store (empty until the first TLS connect),
  ## or nil on a non-`-d:ssl` build. The entry puts it on `config.tls.contextStore`.
  when defined(ssl):
    result = newTlsContextStore()
  else:
    discard cfg

proc closeTlsCtxStore*(store: RootRef) =
  when defined(ssl):
    if not store.isNil: close(cast[TlsContextStore](store))
  else:
    discard store

when defined(ssl):
  proc resumeSlot(cfg: TlsConfig, origin: string): SessionSlot =
    ## When resumption is on and the client has a session cache, return a slot keyed
    ## by `origin`; otherwise nil. The context is armed once in `obtainContext`, so
    ## this only mints the per-connection link.
    if cfg.wantsResume and not cfg.sessionCache.isNil:
      result = newSlot(cast[TlsSessionCache](cfg.sessionCache), origin)

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

proc proxyConnect(transport: StreamTransport, host: string, port: int,
                  user, pass: string) {.async.} =
  let target = host & ":" & $port
  var req = "CONNECT " & target & " HTTP/1.1\r\nHost: " & target & "\r\n"
  if user.len > 0 or pass.len > 0:
    req.add("Proxy-Authorization: Basic " & encode(user & ":" & pass) & "\r\n")
  req.add("\r\n")
  discard await transport.write(req)
  var buf = newString(1024)
  let n = await transport.readOnce(addr buf[0], buf.len)
  buf.setLen(n)
  if not (buf.startsWith("HTTP/1.1 200") or buf.startsWith("HTTP/1.0 200")):
    raise newException(ValueError, "navi: proxy CONNECT failed: " & buf.splitLines()[0])

proc recvExactly(transport: StreamTransport, n: int): Future[string] {.async.} =
  ## Read exactly `n` bytes or raise; SOCKS5 replies are fixed-size frames.
  var buf = newString(n)
  var off = 0
  while off < n:
    let r = await transport.readOnce(addr buf[off], n - off)
    if r <= 0: raise newException(IOError, "navi: SOCKS5 proxy closed the connection")
    off += r
  buf

proc socksConnect(transport: StreamTransport, host: string, port: int,
                  user, pass: string) {.async.} =
  ## SOCKS5 handshake to tunnel to `host:port` (RFC 1928 + RFC 1929). The target is
  ## sent as a domain name so the proxy resolves DNS.
  let hasAuth = user.len > 0 or pass.len > 0
  discard await transport.write(greeting(hasAuth))
  case selectedMethod(await recvExactly(transport, 2))
  of methodUserPass:
    if not hasAuth:
      raise newException(ValueError, "navi: SOCKS5 proxy requires authentication")
    discard await transport.write(authRequest(user, pass))
    checkAuthReply(await recvExactly(transport, 2))
  of methodNoAuth: discard
  else: raise newException(ValueError, "navi: SOCKS5 proxy rejected the offered auth methods")
  discard await transport.write(connectRequest(host, port))
  let header = await recvExactly(transport, 4)
  let status = replyStatus(header)
  if status != 0: raiseReply(status)
  let tail = boundTailLen(int(uint8(header[3])))
  if tail >= 0: discard await recvExactly(transport, tail)
  else:
    let dlen = int(uint8((await recvExactly(transport, 1))[0]))
    discard await recvExactly(transport, dlen + 2)

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
      raise newException(response.TimeoutError,
                         "navi: connect timed out after " & $connectMs & " ms")
  else:
    await establish()
  return conn

proc sendAll*(c: Conn, data: string): Future[void] {.async.} =
  when defined(ssl):
    if not c.tls.isNil:
      await c.tls.write(data)
      return
  await c.writer.write(data)

proc plaintextRead(c: Conn): Future[string] {.async.} =
  var buf = newString(4096)
  var n = 0
  try:
    n = await c.reader.readOnce(addr buf[0], buf.len)
  except AsyncStreamError:
    n = 0  # remote closed mid-stream; treat as EOF for the parser
  buf.setLen(n)
  result = buf

proc recvSome*(c: Conn): Future[string] {.async.} =
  ## One chunk; "" means the peer closed. Bounded by `readMs` (the per-read stall
  ## timeout) when set; on expiry the read is cancelled and TimeoutError is raised.
  var readFut: Future[string]
  when defined(ssl):
    if not c.tls.isNil:
      readFut = c.tls.readSome()
  if readFut.isNil:
    readFut = plaintextRead(c)
  if c.readMs > 0:
    if not await withTimeout(readFut, c.readMs.milliseconds):
      raise newException(response.TimeoutError,
                         "navi: read timed out after " & $c.readMs & " ms")
  result = await readFut

proc close*(c: Conn): Future[void] {.async.} =
  when defined(ssl):
    if not c.tls.isNil:
      await c.tls.close()   # frees the SSL (and its BIOs) and the transport
      if c.ownsCtx and not c.ctx.isNil: destroyCtx(c.ctx)
      return
  if not c.writer.isNil: await c.writer.closeWait()
  if not c.reader.isNil: await c.reader.closeWait()
  if not c.transport.isNil: await c.transport.closeWait()

proc closeSync*(c: Conn) =
  ## Synchronous close, for a destructor that cannot `await` (an abandoned
  ## streaming handle reclaimed by GC). chronos's non-`Wait` `close` initiates
  ## teardown and returns; the event loop frees the resources afterwards.
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
