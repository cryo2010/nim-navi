## Shared native async client body, `include`d by private/asyncdispatch_impl.nim
## and private/chronos_impl.nim. The includer's prelude provides: the backend
## imports (Future/await/{.async.}/Conn/H2Mux/sleepAsync/withTimeout resolve
## there), claimEntry, `naviMwClosure`, `QuicConn`/`openQuicConn`, `msOf`, and
## `guard`; `kaRecv` is forward-declared here and defined per backend AFTER the
## include. Not a standalone module: do not `nim check` it directly.


type
  NaviContext* = ref object
    ## Carried through the middleware chain. A middleware reads and mutates it,
    ## then `await ctx.next()` runs the rest of the chain (which fills `res`).
    req*: Request            ## the outgoing request; modify it before `next`
    res*: Response           ## the response; set by `next`, adjust it after
    clientv: Navi            ## the owning client (see `client`)
    idx: int                 ## index of the next middleware to run
    asyncStream: AsyncBodyProducer  ## awaited pull-based upload producer, threaded
                             ## outside `req` (its Future type is backend-specific);
                             ## nil for a buffered/sync-streamed body. `next` forwards
                             ## it to the transport once the middleware chain is spent.
    userSink: BodySink       ## wrapped gated response sink (nil for a buffered call);
                             ## forwarded to the request core once the chain is spent
    gate: SinkGate           ## the sink's delivery gate (nil for a buffered call)
  NaviMiddleware* = proc(ctx: NaviContext): Future[void] {.naviMwClosure.}
    ## A middleware step; may be async. A closure, so it can capture: read/modify
    ## `ctx.req`, `await ctx.next()` to proceed -- or skip it to short-circuit --
    ## then read/modify `ctx.res`. Write it as a plain `{.async.}` proc (identical
    ## spelling on every backend); a factory `proc bearer(token): NaviMiddleware`
    ## closes over config. The `gcsafe` chronos requires is carried by the
    ## `naviMwClosure` pragma, not this public type; chronos's strict-raises
    ## obligation is discharged in `next` (see the cast there).

  NaviConfig* {.requiresInit.} = object of NaviConfigBase
    ## `requiresInit`: build it with `initNaviConfig()`, not a bare `NaviConfig(...)`.
    middleware*: seq[NaviMiddleware]

  Navi* = ref object
    config*: NaviConfig
      ## The client's live configuration. Mutate it to reconfigure between
      ## requests, e.g. `client.config.headers["authorization"] = "Bearer " & tok`;
      ## the change applies from the next request on (the request path reads these
      ## fields live). Exceptions: `tls`, `http`, and `proxy` are bound when
      ## connections are opened, so change those by building a new client (or
      ## `extend`), not in place.
    pool*: Pool[PooledConn[Conn]]
    jar*: CookieJar
    muxes: TableRef[string, H2Mux]              ## live shared h2 connections
    pendingMux: TableRef[string, Future[H2Mux]] ## in-flight connects (coalescing)
    orphanCloses: seq[Future[void]]             ## detached teardowns of displaced/evicted
                                                ## shared connections (h2 muxes, h3 conns);
                                                ## close() awaits them so their fds are
                                                ## reaped deterministically, not left to
                                                ## background draining (see trackOrphan).
    when defined(naviHttp3):
      altSvc: AltSvcCache                       ## per-origin h3 discovery cache
      h3conns: TableRef[string, QuicConn]  ## live multiplexed h3 connections
      pendingH3: TableRef[string, Future[QuicConn]] ## in-flight h3 connects

proc initNaviConfig*(): NaviConfig =
  ## The only way to build a config (`NaviConfig` requires every field). Sets the
  ## safe defaults; override the fields you want, then pass it to `newNavi`.
  NaviConfig(
    prefixUrl: "", headers: initHeaders(), http: defaultHttpVersions, tls: defaultTls(),
    decompress: true, throwHttpErrors: true, maxRedirects: 20,
    retry: defaultRetryPolicy(), maxResponseBytes: 0,
    auth: Auth(), proxy: "", unixSocket: "",
    maxIdleConns: 0, maxIdleConnsPerHost: 0, idleConnTimeout: 0,
    timeouts: Timeouts(h2KeepAlive: defaultH2KeepAliveMs), resolvedProxy: nil,
    middleware: @[])

when not defined(naviHttp3):
  var h3BuildWarned {.threadvar.}: bool   # per-thread once-flag (a shared global races)

proc newNavi*(config = initNaviConfig()): Navi =
  when not defined(naviHttp3):
    # H3 in `http` is a silent no-op without -d:naviHttp3 (h1/h2 only); warn once.
    if H3 in config.http and not h3BuildWarned:
      h3BuildWarned = true
      stderr.writeLine("navi: config.http includes H3 but this build lacks " &
        "-d:naviHttp3; HTTP/3 will not be attempted (using h1/h2). Rebuild with " &
        "-d:naviHttp3 to enable HTTP/3.")
  var cfg = config
  cfg.tls.sessionCache = newTlsStore(cfg.tls)   # always its own cache, so a config
  cfg.tls.contextStore = newTlsCtxStore(cfg.tls) # cloned from another client (e.g.
                                                 # newNavi(other.config)) is isolated
  cfg.resolvedProxy = buildResolvedProxy(cfg)    # resolve env/proxy/NO_PROXY once (#361)
  result = Navi(config: cfg,
       pool: newPool[PooledConn[Conn]](cfg.idlePerHost, cfg.idleGlobal, cfg.idleTimeoutMs),
       jar: newCookieJar(),
       muxes: newTable[string, H2Mux](),
       pendingMux: newTable[string, Future[H2Mux]]())
  when defined(naviHttp3):
    result.altSvc = newAltSvcCache()
    result.h3conns = newTable[string, QuicConn]()
    result.pendingH3 = newTable[string, Future[QuicConn]]()

proc extend*(client: Navi, config: NaviConfig): Navi =
  var merged = mergeBase(client.config, config)
  merged.middleware = client.config.middleware & config.middleware
  merged.tls.sessionCache = newTlsStore(merged.tls)  # its own cache, not the parent's
  merged.tls.contextStore = newTlsCtxStore(merged.tls)  # its own contexts too
  merged.resolvedProxy = buildResolvedProxy(merged)  # its own resolved proxy (#361):
                                                     # an extended client with a
                                                     # different proxy must not inherit
                                                     # the parent's cache
  result = Navi(config: merged,
       pool: newPool[PooledConn[Conn]](merged.idlePerHost, merged.idleGlobal, merged.idleTimeoutMs),
       jar: newCookieJar(),
       muxes: newTable[string, H2Mux](),
       pendingMux: newTable[string, Future[H2Mux]]())
  when defined(naviHttp3):
    result.altSvc = newAltSvcCache()
    result.h3conns = newTable[string, QuicConn]()
    result.pendingH3 = newTable[string, Future[QuicConn]]()

proc close*(client: Navi): Future[void] {.async.} =
  ## Close all pooled connections and shared h2 connections, freeing their TLS
  ## contexts. Any in-flight request on a shared connection fails with IOError.
  ## Optional but recommended when done with the client.
  for pc in client.pool.drain():
    await close(pc.transport)
  # Teardown of the shared-connection tables runs as a STABILIZING loop, not a single
  # pass, because a connect can resolve and cache its connection AFTER we drain the
  # tables. Two ways that happens: an in-flight coalesced connect resolving late
  # (issue #315), and -- the harder one the h3 chaos exposed -- under asyncdispatch a
  # request whose per-attempt timeout fired leaves its `getH3Conn`/resolveOrConnect
  # frame ABANDONED but still running in the background (asyncdispatch has no true
  # cancellation -- see guard); that frame finishes its `openQuicConn`/`connect` and
  # caches the fresh connection into `h3conns`/`muxes` after a single-pass close()
  # already drained them, orphaning the connection with its reader + fds where nothing
  # can ever reach it (the never-closed H3Conns the h3 chaos slowbody/stall timeouts
  # accreted, one leaked UDP socket + wake-pipe pair each). So we drain, yield the loop
  # a tick to let any such straggler land, and repeat until a full pass finds every
  # table stable-empty. Each pass snapshots-then-clears before awaiting a close, so an
  # in-flight request's error path deleting its own entry mid-await cannot trip the
  # tables' "length changed while iterating" defect. The `sweeps >= 64` cap bounds the
  # loop so a pathologically self-refilling table can never spin forever; in practice
  # one or two passes suffice (a straggler lands, gets reaped the next pass).
  var sweeps = 0
  while true:
    inc sweeps
    var pendingMuxes: seq[Future[H2Mux]]
    for f in client.pendingMux.values: pendingMuxes.add f
    client.pendingMux.clear()
    for f in pendingMuxes:
      try:
        let mux = await f
        if mux != nil: await mux.close()
      except CatchableError: discard   # a failed connect has nothing to close
    var muxList: seq[H2Mux]
    for mux in client.muxes.values: muxList.add mux
    client.muxes.clear()
    for mux in muxList:
      await mux.close()
    when defined(naviHttp3):
      var pendingConns: seq[Future[QuicConn]]
      for f in client.pendingH3.values: pendingConns.add f
      client.pendingH3.clear()
      for f in pendingConns:
        try:
          let qc = await f
          if qc != nil: await qc.closeConn()
        except CatchableError: discard
      var connList: seq[QuicConn]
      for qc in client.h3conns.values: connList.add qc
      client.h3conns.clear()
      for qc in connList:
        await qc.closeConn()
    # Reap every displaced/evicted shared connection whose teardown was detached off
    # the request path (trackOrphan): a QuicError-evicted h3 conn or a race-displaced
    # h2 mux. Under asyncdispatch these detached futures are NOT reliably driven to
    # completion by background draining before the process samples its fds, so awaiting
    # them here makes close() deterministic instead of leaving one un-closed connection
    # per eviction.
    while client.orphanCloses.len > 0:
      let batch = client.orphanCloses
      client.orphanCloses.setLen(0)
      for f in batch:
        try: await f
        except CatchableError: discard
    # This pass drained every table. Yield one tick so any abandoned background connect
    # (asyncdispatch has no true cancellation, so a timed-out request's
    # `getH3Conn`/connect frame finishes and caches its fresh connection LATER) can
    # land in a table before we re-check. Loop again only if a straggler materialised;
    # a stable-empty pass, or the sweep cap, ends the loop.
    await sleepAsync(msOf(0))   # msOf: 0 for asyncdispatch, 0.milliseconds for chronos
    var refilled = client.pendingMux.len > 0 or client.muxes.len > 0 or
                   client.orphanCloses.len > 0
    when defined(naviHttp3):
      refilled = refilled or client.pendingH3.len > 0 or client.h3conns.len > 0
    if not refilled or sweeps >= 64: break
  closeTlsStore(client.config.tls.sessionCache)
  closeTlsCtxStore(client.config.tls.contextStore)

when defined(naviHttp3):
  proc h3ConnCount*(client: Navi): int = client.h3conns.len
    ## Live multiplexed HTTP/3 connections; for tests/introspection.

proc muxRequest(client: Navi, mux: H2Mux, req: Request,
                sink: BodySink,
                asyncStream: AsyncBodyProducer = nil,
                userSink: BodySink = nil, gate: SinkGate = nil): Future[Response] {.async.} =
  # The mux delivers a streaming request's body to `sink` incrementally (decoding
  # content-encoding as it arrives), so the returned response's body is empty.
  # A non-streaming request (sink == nil) still buffers into r.body as before.
  # `asyncStream` (when set) streams the request body up, awaited chunk by chunk.
  #
  # `userSink`/`gate` (when set) are the buffered `request()` gated-sink path. The real
  # h2 mux streaming path is sendAndReadHeaders + drainDownload (H2Mux.request's own
  # `sink` param is dead code), so route it here: read headers, snapshot, register a
  # trailer capture, then either drain to the gated sink (gate wants this response) or
  # buffer via an accumulator sink for the policy layer. `mux.readChunk` returns DECODED
  # bytes (per-sid CappedDecoder), so a buffered body is already plaintext -- mark it
  # decoded so `decodeBody` upstream does not inflate it twice.
  if userSink.isNil or gate.isNil:
    # The buffered request path: `sink` is always nil here (stream() drains via the
    # handle's own sendAndReadHeaders, not muxRequest), so this buffers into r.body.
    result = toResponse(await mux.request(h2HeaderList(req), req.body, req.bodyStream,
                                          h2TrailerList(req), asyncStream))
    return
  let sid = await mux.sendAndReadHeaders(h2HeaderList(req), req.body, req.bodyStream,
                                         h2TrailerList(req), false, asyncStream)
  var snap = toResponse(mux.respSnapshot(sid))
  mux.captureTrailers(sid)                 # so a full drain keeps the trailing HEADERS
  let eff = (if gate.wantsDelivery(snap.httpVersion, snap.status, snap.headers):
               userSink else: BodySink(nil))
  if not eff.isNil:
    try:
      await mux.drainDownload(sid, eff)     # its except RSTs + frees via endStream
    except SinkStopSignal:
      await mux.abandon(sid)                # stop the peer; the connection stays up
      snap.bodyTruncated = true
      snap.body = ""
      return snap
    snap.body = ""
    snap.trailers = initHeaders(mux.takeTrailers(sid))
    return snap
  # Not the final response: buffer the body so the policy layer can decide. Accumulate
  # via a sink into a local, then attach it (already decoded) to the snapshot.
  var buf = ""
  let acc: BodySink = proc(data: string): Future[void] {.async.} =
    buf.add data
  await mux.drainDownload(sid, acc)
  snap.body = buf
  snap.trailers = initHeaders(mux.takeTrailers(sid))
  markStreamDecoded(snap)                   # readChunk already decoded content-encoding
  result = snap

proc h1OnConn(client: Navi, conn: Conn, origin: string, req: Request,
              sink: BodySink,
              asyncStream: AsyncBodyProducer = nil,
              userSink: BodySink = nil, gate: SinkGate = nil): Future[Response] {.async.} =
  var keep = false
  # The exchange can raise (timeout, RST/close mid-body, malformed response, a
  # cancelled `total` guard). On any raise the success-path pool/close below is
  # skipped, so close `conn` here or its fd/socket leaks -- a hostile or slow peer
  # that makes every request time out would otherwise leak one connection each
  # (surfaced by the chaos stress harness's FD assertion). A clean exchange still
  # takes the pool-or-close path unchanged.
  try:
    if not userSink.isNil and not gate.isNil:
      var parser = h1SendAndReadHeaders(conn, req, true, asyncStream)
      result = h1GatedFinish(conn, parser, userSink, gate, keep,
                             client.config.wantsDecompress, client.config.maxResponseBytes)
    else:
      result = h1Exchange(conn, req, sink, keep,
                          client.config.wantsDecompress, client.config.maxResponseBytes,
                          asyncStream)
  except CatchableError:
    await close(conn)
    raise
  let pc = PooledConn[Conn](transport: conn)
  if not (keep and pushIdle(client.pool, origin, pc)):
    await close(conn)

proc pruneDeadMuxes(client: Navi) =
  ## Drop shared h2 connections that can no longer be reused (reader exited / GOAWAY /
  ## stream-id exhausted). A revisited origin overwrites its own entry, but an origin
  ## that dies and is never contacted again would otherwise keep its dead `H2Mux` for
  ## the client's lifetime, so a client fanning out across many h2 origins leaks one
  ## entry per dead origin (the h3conns table is already pruned this way). The sweep
  ## has no await, so it is atomic w.r.t. the event loop; an in-flight request holds
  ## its own mux ref, so dropping the table entry never disturbs a request under way
  ## (issue #312).
  var dead: seq[string]
  for origin, mux in client.muxes:
    if not mux.canReuse: dead.add origin
  for origin in dead: client.muxes.del(origin)

proc closeOrphanMux(mux: H2Mux) {.async.} =
  ## Fire-and-forget close of a mux that was displaced from `client.muxes` while still
  ## live (its reader + keepalive still running). Swallows every error, so its future
  ## never fails: safe to detach off the request path (the request must not block on
  ## tearing an orphan down, but the orphan must still be reachable for close, else its
  ## fd/reader leak where `client.close`/`pruneDeadMuxes` can never see it), and it
  ## satisfies chronos's `asyncSpawn` no-failure contract at the spawn site.
  try: await mux.close()
  except CatchableError: discard

proc trackOrphan(client: Navi, fut: Future[void]) =
  ## Retain a detached teardown future (an orphan mux/h3-conn close) so `close()`
  ## can await it, instead of firing it fully-and-forget. The request path still
  ## does not block on it -- it just stays reachable. We prune already-finished
  ## entries opportunistically so a long-lived client under heavy eviction churn
  ## does not accumulate the seq unboundedly between closes.
  var live: seq[Future[void]]
  for f in client.orphanCloses:
    if not f.finished: live.add f
  live.add fut
  client.orphanCloses = live

proc resolveReusableMux(client: Navi, origin: string): Future[H2Mux] {.async.} =
  ## Resolve a shared h2 connection to reuse for `origin`: a live cached mux, or -- when
  ## a concurrent connect is in flight -- the mux that coalescing connect yields. Returns
  ## nil when there is neither (or the pending connect turned out http/1.1), so the caller
  ## falls through to the pooled-h1 / fresh-connect steps. Awaiting `pendingMux` here (not
  ## in the caller's try) keeps a coalesced waiter off the fresh-connect path, so a burst
  ## -- including racers displaced by a keep-alive race -- still lands on ONE connection.
  if client.muxes.hasKey(origin) and client.muxes[origin].canReuse:
    return client.muxes[origin]
  if client.pendingMux.hasKey(origin):
    let mux = await client.pendingMux[origin]
    if mux != nil and mux.canReuse: return mux
    # else: that connect turned out http/1.1, or resolved dead -- fall through
  return nil

proc openFreshConn(client: Navi, rq: Request, origin: string,
                   wantH2: bool): Future[tuple[conn: Conn, mux: H2Mux]] {.async.} =
  ## Open a fresh connection to `rq`'s origin, coalescing concurrent cold connects
  ## to the same new origin through one `pendingMux` future so a burst still lands on
  ## a single h2 connection. Returns (conn, mux): a non-nil `mux` is a live, cached
  ## shared h2 connection whose pending future has been completed (use `mux`); a nil
  ## `mux` means the origin negotiated http/1.1 (use `conn`). Raises on connect/
  ## handshake failure, failing the pending future so coalesced waiters observe it
  ## too. `rq.absoluteForm` must already be set by the caller (it is what the caller
  ## sends with). Shared by the buffered (transportInner) and streaming
  ## (openStreamConn) routers so the coalescing dance lives in one place.
  let proxyTarget = resolveProxy(client.config, rq.url)
  let alpn = if wantH2: @["h2", "http/1.1"] else: @[]
  if not wantH2:
    let conn = await connect(rq.url.host, rq.url.port, rq.url.isTls,
                             client.config.tls, proxyTarget, alpn,
                             client.config.connectMs, client.config.readMs)
    return (conn, H2Mux(nil))
  # A concurrent racer may already have a connect in flight (or a live mux) for this
  # origin -- e.g. N waiters displaced from a reused mux by a keep-alive race in one
  # tick, each reaching here. Coalesce onto it (no await before this check ran in the
  # caller's lookup, but the reap/popIdle awaits since then open a window) rather than
  # opening a second connection and overwriting the pendingMux slot, which would orphan
  # the racer's connection where `client.close`/`pruneDeadMuxes` can never reach it.
  if client.pendingMux.hasKey(origin):
    let existing = await client.pendingMux[origin]
    if existing != nil and existing.canReuse: return (default(Conn), existing)
    # else: that connect turned out http/1.1 or resolved dead -- open our own below
  let pending = newFuture[H2Mux]("navi.pendingMux")
  client.pendingMux[origin] = pending
  try:
    let conn = await connect(rq.url.host, rq.url.port, rq.url.isTls,
                             client.config.tls, proxyTarget, alpn,
                             client.config.connectMs, client.config.readMs)
    if conn.protocol == "h2":
      let mux = await newH2Mux(conn, client.config.maxResponseBytes,
                               client.config.wantsDecompress,
                               client.config.h2KeepAliveMs)
      # Installing into `muxes` must not silently orphan a DIFFERENT live mux another
      # racer cached in the meantime: its reader + keepalive would run forever, its fd
      # unreachable for close. Close the displaced one fire-and-forget (off the request
      # path -- close is async and we must not block here); our own entry, if a racer
      # replaced it, is left alone. `pending.del` likewise only removes OUR future.
      let prior = client.muxes.getOrDefault(origin, nil)
      if prior != nil and prior != mux:      # a racer cached a DIFFERENT live mux here:
        client.trackOrphan(closeOrphanMux(prior))  # close it off the request path but keep
                                             # the future reachable so close() reaps it; its
                                             # own dead-guard no-ops if it already exited.
      client.muxes[origin] = mux
      if client.pendingMux.getOrDefault(origin, nil) == pending:
        client.pendingMux.del(origin)
      pending.complete(mux)
      return (conn, mux)
    else:
      if client.pendingMux.getOrDefault(origin, nil) == pending:
        client.pendingMux.del(origin)
      pending.complete(nil)          # this origin is http/1.1
      return (conn, H2Mux(nil))
  except CatchableError as e:
    if client.pendingMux.getOrDefault(origin, nil) == pending:
      client.pendingMux.del(origin)
    # A failure after the branch already completed `pending` (h1 fallback, or a
    # post-handshake error) must not complete the future twice (mirrors chronos).
    if not pending.finished: pending.fail(e)
    raise

proc transportInner(client: Navi, req: Request, sink: BodySink,
                    asyncStream: AsyncBodyProducer = nil,
                    userSink: BodySink = nil, gate: SinkGate = nil): Future[Response] {.async.} =
  ## Multiplex over a shared h2 connection when available/negotiable; otherwise
  ## pool http/1.1. Concurrent connects to the same new origin are coalesced so a
  ## cold burst still ends up on one h2 connection. `asyncStream` (when set) is an
  ## awaited pull-based upload producer, streamed up in place of a buffered body.
  ## `userSink`/`gate` (when set) stream the FINAL response body to the caller's gated
  ## sink (the buffered `request()` sink path).
  let origin = originKey(req.url)
  let wantH2 = client.config.wantsH2 and req.url.isTls
  client.pruneDeadMuxes()          # evict muxes that died since the last request (#312)

  if wantH2:
    # 1. A live shared connection, or one currently being established. A reused mux
    # can be torn down by the peer at any time (idle recycle, a GOAWAY-less close), so
    # a request dispatched on it that dies BEFORE any response HEADERS is the classic
    # keep-alive race: the mux surfaces that as `KeepAliveRaceError` (see failAll), or
    # `UnprocessedError` when the mux was found dead before the request was sent (a
    # TOCTOU dead mux, or a GOAWAY while parked on a concurrency slot). Neither was
    # answered, so replay it -- a race for an idempotent/keyed method, an unprocessed
    # error for any method (the shared `replayableAfterError`). A gated body already
    # fed, or a non-rewindable streamed body, must not be re-issued, so those propagate.
    #
    # The retry does NOT jump straight to a fresh connection: a concurrent racer may
    # have already opened one (registered in `pendingMux`), so re-enter the lookup once
    # and coalesce onto it rather than each racer opening -- and orphaning -- its own
    # connection. `resolveReusableMux` (below) does the muxes/pendingMux lookup; we run
    # it at most twice (the initial dispatch, then one retry after a replayable race).
    var attempted = false            # whether we already dispatched on a resolved mux
    while true:
      let mux = await client.resolveReusableMux(origin)
      if mux == nil: break           # no live/pending mux (or it turned out h1): fall through
      mux.applyKeepAlive(client.config.h2KeepAliveMs)   # adopt the CURRENT keepalive
                                     # interval, not the one it was opened with (#360)
      try:
        return await client.muxRequest(mux, req, sink, asyncStream, userSink, gate)
      except CatchableError as e:
        # Fall through to a fresh connection only when the error class is replayable
        # AND this request may be replayed; a gated body already fed never is. On the
        # first replayable race retry the lookup once (to coalesce onto a peer's fresh
        # connect); a second failure falls through to steps 2/3. Any other error class
        # (a post-response truncation, a cancellation) is terminal and propagates.
        if (gate != nil and gate.fed) or
           not (isReplayClassError(e) and isReplayable(req) and
                replayableAfterError(req, e)): raise
        if attempted: break          # already retried once: stop coalescing, go fresh
        attempted = true             # loop once more through resolveReusableMux

  # 2. A pooled http/1.1 connection.
  for dead in reapExpired(client.pool):    # close idle connections past idleConnTimeout
    await close(dead.transport)
  var (found, pc) = popIdle(client.pool, origin)
  if found:
    # A pooled connection adopts the CURRENT config read timeout, not the one it was
    # opened with, honoring navi's live-config contract (issue #360). The whole-request
    # deadline is enforced by the async entry's `guard`, so only `readMs` is re-armed.
    rearm(pc.transport, client.config.readMs)
    let gated = not userSink.isNil and not gate.isNil
    try:
      var keep = false
      var parser = h1SendAndReadHeaders(pc.transport, req, not sink.isNil or gated,
                                        asyncStream)
      if gated:
        result = h1GatedFinish(pc.transport, parser, userSink, gate, keep,
                               client.config.wantsDecompress, client.config.maxResponseBytes)
      else:
        h1DrainBody(pc.transport, parser, sink, keep,
                    client.config.wantsDecompress, client.config.maxResponseBytes)
        result = parser.toResponse()
      if not (keep and pushIdle(client.pool, origin, pc)): await close(pc.transport)
      return
    except CatchableError as e:
      await close(pc.transport)  # stale
      # A half-delivered gated body must never be replayed onto a fresh connection.
      if gate != nil and gate.fed: raise
      # Replay on a fresh connection only when safe (matching Go net/http; RFC 9110
      # 9.2.2), via the shared predicate: an idempotent method, a proven-unprocessed
      # error, or an Idempotency-Key-vouched keep-alive race. A non-idempotent method
      # without a key, or a post-response truncation, is not replayed; a non-rewindable
      # streamed body is never retried.
      if not (isReplayable(req) and replayableAfterError(req, e)):
        raise
      # else fall through to a fresh connection below

  # 3. Open a fresh connection (coalescing concurrent cold h2 connects).
  var rq = req
  rq.absoluteForm = usesAbsoluteForm(resolveProxy(client.config, rq.url), rq.url.isTls)
  let (conn, mux) = await client.openFreshConn(rq, origin, wantH2)
  if mux != nil:
    result = await client.muxRequest(mux, rq, sink, asyncStream, userSink, gate)
  else:
    result = await client.h1OnConn(conn, origin, rq, sink, asyncStream, userSink, gate)

when defined(naviHttp3):
  proc recordAltSvc(client: Navi, req: Request, resp: Response) =
    ## Cache an h3 endpoint the origin advertised, so later requests can upgrade.
    let alt = resp.headers.get("alt-svc")
    if alt.len > 0:
      client.altSvc.record("https", req.url.host, req.url.port, alt)

  proc closeOrphanQuic(qc: QuicConn) {.async.} =
    ## Fire-and-forget close of a QUIC connection displaced from `client.h3conns`
    ## (a stale slot getH3Conn overwrites) or evicted by a stream-scoped QuicError,
    ## while its reader may still be running. Swallows every error so its future
    ## never fails -- safe to detach off the request path and satisfies chronos's
    ## `asyncSpawn` no-failure contract, mirroring closeOrphanMux. Defined before
    ## getH3Conn/h3Transport so both spawn sites see it (Nim #head instantiates the
    ## async templates at the call site and will not resolve a later definition).
    try: await qc.closeConn()
    except CatchableError: discard

  proc getH3Conn(client: Navi, origin: string, ep: AltSvcEndpoint,
                 req: Request): Future[QuicConn] {.async.} =
    ## Reuse the origin's live h3 connection, or open one and cache it. Concurrent
    ## cold connects to the same origin are coalesced through a single in-flight
    ## future (mirrors pendingMux for h2): without it, a burst of streams to a new
    ## origin each opens -- and leaks -- its own QUIC connection, since the plain
    ## open-then-recheck races (both see an empty cache and both cache a survivor).
    while true:
      if client.h3conns.hasKey(origin) and client.h3conns[origin].alive:
        return client.h3conns[origin]
      if client.pendingH3.hasKey(origin):
        let qc = await client.pendingH3[origin]
        if qc != nil and qc.alive: return qc
        continue          # that connect resolved dead (rare race): re-check from top
      # Register the in-flight connect synchronously (no await before this) so racing
      # callers await it instead of opening a second connection.
      let pending = newFuture[QuicConn]("navi.pendingH3")
      client.pendingH3[origin] = pending
      try:
        let qc = await openQuicConn(ep.host, ep.port, req.url.host,
                                     client.config.tls.caFile,
                                     client.config.tls.wantsVerify,
                                     uint64(max(0, client.config.maxResponseBytes)))
        # A dead-but-uncleaned prior conn can occupy this slot: the loop above only
        # returns a cached entry when it is `alive`, so a `not alive` one falls
        # through to here and would be silently overwritten. Its background reader may
        # still be parked (a stream-scoped failure that never tore the whole
        # connection down), holding its UDP socket + wake-pipe fds where neither the
        # next request nor client.close can ever reach them again -- the exact 9
        # never-closed H3Conns the asyncdispatch h3 chaos leak accretes. Close the
        # displaced conn off the request path (trackOrphan keeps it reachable for
        # close()), before replacing the slot.
        let prior = client.h3conns.getOrDefault(origin, nil)
        if prior != nil and prior != qc:
          client.trackOrphan(closeOrphanQuic(prior))
        client.h3conns[origin] = qc
        client.pendingH3.del(origin)
        pending.complete(qc)
        return qc
      except CatchableError as e:
        client.pendingH3.del(origin)
        if not pending.finished: pending.fail(e)
        raise

  proc h3Transport(client: Navi, req: Request,
                        ep: AltSvcEndpoint): Future[Response] {.async.} =
    ## Send `req` (any verb with a buffered body) over a shared HTTP/3 connection
    ## (multiplexed with concurrent requests), building a navi Response so the
    ## policy layer is reused unchanged. Raises `QuicError`.
    var fwd: seq[(string, string)]
    for k, v in req.headers:
      let lk = k.toLowerAscii
      if lk notin h3SkipHeaders: fwd.add((lk, v))
    var fwdTrl: seq[(string, string)]
    for k, v in req.trailers:
      let lk = k.toLowerAscii
      if lk.len > 0 and lk[0] != ':' and lk notin h3SkipHeaders and lk notin ["te", "trailer"]:
        fwdTrl.add((lk, v))
    let origin = originKey(req.url)
    let qc = await client.getH3Conn(origin, ep, req)
    try:
      let r = await qc.requestOnConn($req.verb, req.url.requestTarget, fwd, req.body,
                                     req.bodyStream, fwdTrl)
      result = initResponse(r.status, "", "HTTP/3", initHeaders(r.headers), r.body)
      result.trailers = initHeaders(r.trailers)
    except QuicError:
      if client.h3conns.getOrDefault(origin, nil) == qc:
        client.h3conns.del(origin)       # drop a dead connection
        # A QuicError can be stream-scoped (e.g. the server RESET_STREAMed one
        # response) with the connection object still alive: its background reader
        # keeps the UDP fd registered until closeConn. Evicting such a conn from the
        # table without closing it orphans that fd + reader where neither the next
        # request nor client.close can ever reach them -- under a hostile server
        # (stress chaos: truncate/garbage/vanish) evictions are constant and the
        # process accretes one leaked fd per eviction. Close it off the request path,
        # exactly like closeOrphanMux for displaced h2 muxes; a conn whose reader
        # already died self-cleaned (closeConn no-ops), so this only acts on the
        # live-orphan case. Retain the future via trackOrphan so close() awaits it:
        # under asyncdispatch a detached (asyncSpawn) close is NOT reliably driven to
        # completion by background draining before the process samples its fds, so a
        # hostile server that forces constant evictions leaked one UDP socket +
        # wake-pipe pair per eviction (the CHAOS-FAIL(fd) the h3 chaos surfaced).
        client.trackOrphan(closeOrphanQuic(qc))
      raise

proc transport(client: Navi, req: Request, sink: BodySink,
               asyncStream: AsyncBodyProducer = nil,
               userSink: BodySink = nil, gate: SinkGate = nil): Future[Response] {.async.} =
  ## The wire transport `run` calls. In a `-d:naviHttp3` build, a buffered-body
  ## request to an origin that has advertised h3 (Alt-Svc) goes over HTTP/3, with
  ## any QUIC failure falling back to h2/h1; `alt-svc` on h2/h1 responses is
  ## captured for later upgrades. `asyncStream` (when set) is an awaited upload
  ## producer streamed up in place of a buffered body. `userSink`/`gate` (when set)
  ## stream the FINAL response body to the caller's gated sink; the h3 leg stays
  ## buffered (the performRequest fallback delivers its final body).
  var rq = req
  var producer = asyncStream
  when defined(naviHttp3):
    if client.config.wantsH3 and rq.url.isTls:   # buffered or streamed body
      let ep = client.altSvc.h3Endpoint("https", rq.url.host, rq.url.port)
      if ep.isSome:
        # The h3 request body is pulled by a synchronous C callback (h3PullThunk),
        # which cannot await, so an async producer cannot feed it incrementally. Drain
        # it into a buffered body before the h3 attempt (constant-memory piping is lost
        # only on the h3 leg; the h2/h1 fallback below still streams it, since a QUIC
        # failure means the producer was never pulled). buildRequest set
        # hasStreamedBody; a buffered body is replayable, so clear the flag for the h3
        # request. On QUIC failure we fall back with the ORIGINAL producer (rq/producer
        # here are locals; the drained buffer stays on the h3-only `h3rq`).
        var h3rq = rq
        if producer != nil:
          var buffered = ""
          while true:
            # bare closure (portable spelling): discharge chronos's gcsafe/raises here.
            var chunk: string
            {.cast(gcsafe).}:
              {.cast(raises: [CatchableError]).}:
                chunk = await producer()
            if chunk.len == 0: break
            buffered.add chunk
          h3rq.body = buffered
          h3rq.hasStreamedBody = false
          producer = nil   # already drained: the fallback sends h3rq's buffered body
          rq = h3rq
        try: return await h3Transport(client, h3rq, ep.get)
        except QuicError: discard   # fall back to h2/h1 below (rq now buffered)
  result = await transportInner(client, rq, sink, producer, userSink, gate)
  when defined(naviHttp3):
    client.recordAltSvc(rq, result)

template guardedAttempt*(client, startReq, resp, attemptMs, cancel,
                         asyncStream, userSink, gate: typed) =
  ## Run one attempt of the retry loop, bounded by the per-attempt budget (issue
  ## #375). The async backends ignore `deadlineMs` at connect and enforce timeouts
  ## with a `guard`; here an INNER guard bounds just this attempt (all its redirect
  ## hops). Because it fires inside the retry loop, a per-attempt timeout surfaces as
  ## a retryable `TimeoutError`, whereas the OUTER `total` guard (which wraps the
  ## whole loop) aborts everything and stays terminal. With no per-attempt cap the
  ## attempt runs inline, exactly as before.
  mixin guard, followRedirects
  if attemptMs > 0:
    proc attemptOnce(): Future[Response] {.async.} =
      var r: Response
      followRedirects(client, startReq, r, asyncStream, userSink, gate)
      return r
    resp = await guard(attemptMs, attemptOnce(), cancel)
  else:
    followRedirects(client, startReq, resp, asyncStream, userSink, gate)

proc doRequest(client: Navi, req: Request,
               asyncStream: AsyncBodyProducer = nil,
               userSink: BodySink = nil, gate: SinkGate = nil): Future[Response] {.async.} =
  result = performRequest(client, req, nil, asyncStream, userSink, gate)

proc client*(ctx: NaviContext): Navi = ctx.clientv
  ## The client handling this request (e.g. to read `ctx.client.config`).

proc cookies*(client: Navi): seq[StoredCookie] =
  ## A read-only snapshot of the client's cookie jar, for inspection/debugging:
  ## every cookie currently stored (all origins), across the whole jar rather than
  ## the URL-scoped view a request sees. Expired-but-not-yet-pruned entries are
  ## included; `StoredCookie.expires` reveals staleness. See also `items`/`len`/`$`
  ## on `client.jar`.
  for c in client.jar: result.add c

proc next*(ctx: NaviContext): Future[void] {.async.} =
  ## Run the rest of the chain: the next middleware, or -- once they are
  ## exhausted -- the request itself. The outcome lands in `ctx.res`.
  let mws = ctx.clientv.config.middleware
  if ctx.idx >= mws.len:
    ctx.res = await doRequest(ctx.clientv, ctx.req, ctx.asyncStream,
                              ctx.userSink, ctx.gate)
  else:
    let m = mws[ctx.idx]
    inc ctx.idx
    # The public NaviMiddleware type is a plain closure (portable to js), so it
    # carries no chronos raises annotation. Middleware raise at most CatchableError
    # (navi's error contract; CancelledError is one, so cancellation still flows),
    # which we assert here to satisfy chronos's strict effect tracking.
    {.cast(raises: [CatchableError]).}:
      await m(ctx)

proc runChain(ctx: NaviContext): Future[Response] {.async.} =
  await ctx.next()
  return ctx.res

proc wrapSink(s: GatedBodySink, gate: SinkGate): BodySink =
  ## Adapt a caller's gated sink into the internal `BodySink` the engine drains into:
  ## mark the gate as fed (so the replay guards bar a retry) before each delivery, and
  ## turn a `false` result into a `SinkStopSignal` the drain site catches and converts
  ## to a normal early stop. The engine awaits the sink, so this awaits `s`. Chronos's
  ## strict gcsafe/raises obligation on the portable `s` is discharged with a cast, as
  ## the deliverChunk path does. Nil in -> nil out.
  if s.isNil: return nil
  result = proc(data: string): Future[void] {.async.} =
    gate.fed = true
    var keep: bool
    {.cast(gcsafe).}:
      {.cast(raises: [CatchableError]).}:
        keep = await s(data)
    if not keep:
      raise newException(SinkStopSignal, "navi: sink requested early stop")

proc wrapSink(s: BodySink, gate: SinkGate): BodySink =
  ## Adapt a caller's void sink (always continue) into the internal `BodySink`, marking
  ## the gate as fed before each delivery. Nil in -> nil out.
  if s.isNil: return nil
  result = proc(data: string): Future[void] {.async.} =
    gate.fed = true
    {.cast(gcsafe).}:
      {.cast(raises: [CatchableError]).}:
        await s(data)

proc requestResolved(client: Navi, verb: HttpVerb, target: string,
                     headers: Headers, body: ResolvedBody,
                     form: seq[(string, string)],
                     params: seq[(string, string)],
                     cancel: CancelToken,
                     trailers: Headers,
                     asyncStream: AsyncBodyProducer = nil,
                     userSink: BodySink = nil,
                     gate: SinkGate = nil): Future[Response] {.async.} =
  ## `asyncStream` (default nil) is an awaited pull-based upload producer, threaded
  ## alongside the built `Request` because its Future type is backend-specific (it
  ## cannot be a field on the core `Request`/`ResolvedBody`). When set, `body` is the
  ## default `ResolvedBody()` (no buffered body) and the request is flagged
  ## non-replayable, mirroring a sync `bodyStream`. `userSink`/`gate` (when set) stream
  ## the FINAL response body to the caller's gated sink.
  var req = buildRequest(client.config, verb, target, headers, body,
                         form, params, trailers)
  if asyncStream != nil:
    req.hasStreamedBody = true   # non-replayable: pulled once, cannot rewind
  if client.config.middleware.len == 0:
    return await guard(client.config.totalMs,
                       doRequest(client, req, asyncStream, userSink, gate), cancel)
  let ctx = NaviContext(req: req, clientv: client, asyncStream: asyncStream,
                        userSink: userSink, gate: gate)
  return await guard(client.config.totalMs, runChain(ctx), cancel)

proc request*[B](client: Navi, verb: HttpVerb, target: string,
                 headers = initHeaders(), body: B = "",
                 form: seq[(string, string)] = @[],
                 params: seq[(string, string)] = @[],
                 cancel: CancelToken = nil,
                 trailers = initHeaders()): Future[Response] =
  ## Perform a request; configured middleware wraps the whole call. `body` is
  ## dispatched by type: a `string` is the raw body, a `JsonNode` is sent as JSON,
  ## a `Multipart` as multipart/form-data, a `BodyProducer` or closure
  ## `BodyIterator` streams a chunked upload, and any other value is serialized to
  ## JSON. `form` encodes a urlencoded body and is outranked by a typed `body`.
  ## `params` are appended to the URL query; `cancel` aborts the in-flight request.
  ## `trailers` are sent after the body (chunked on h1, a trailing HEADERS block on
  ## h2/h3). An `AsyncBodyProducer` (`proc(): Future[string]`) streams a chunked
  ## upload pulled with `await` -- one call per chunk, "" ends the body -- so
  ## producing a chunk can itself await (e.g. piping a streaming download into the
  ## upload). Like `bodyStream` it is not replayable (no retry/redirect/digest replay).
  when B is AsyncBodyProducer:
    requestResolved(client, verb, target, headers, ResolvedBody(), form, params,
                    cancel, trailers, asyncStream = body)
  else:
    requestResolved(client, verb, target, headers, toBody(body), form, params,
                    cancel, trailers)

proc request*[B](client: Navi, verb: HttpVerb, target: string,
                 headers: Headers, body: B, sink: GatedBodySink,
                 form: seq[(string, string)] = @[],
                 params: seq[(string, string)] = @[],
                 cancel: CancelToken = nil,
                 trailers = initHeaders()): Future[Response] =
  ## Like `request`, but streams the FINAL response body to `sink` instead of
  ## buffering it into `res.body`. The full policy layer still runs (redirects,
  ## retries, digest, middleware, throw-on-non-2xx): only the body of the response
  ## actually surfaced to you reaches the sink; redirect/retry/digest/thrown-error
  ## bodies never do (an `HttpError` still carries its buffered body). `sink` returns
  ## `bool`: `true` keeps going, `false` stops the download early -- the request then
  ## returns normally with `res.body == ""` and `res.bodyTruncated == true`. On a full
  ## drain `res.body` is "" and any trailers are populated; HEAD/204/304 never call it.
  let gate = newSinkGate()
  when B is AsyncBodyProducer:
    requestResolved(client, verb, target, headers, ResolvedBody(), form, params,
                    cancel, trailers, body, wrapSink(sink, gate), gate)
  else:
    requestResolved(client, verb, target, headers, toBody(body), form, params,
                    cancel, trailers, nil, wrapSink(sink, gate), gate)

proc request*[B](client: Navi, verb: HttpVerb, target: string,
                 headers: Headers, body: B, sink: BodySink,
                 form: seq[(string, string)] = @[],
                 params: seq[(string, string)] = @[],
                 cancel: CancelToken = nil,
                 trailers = initHeaders()): Future[Response] =
  ## Like the gated `request` overload, but `sink` is a void `BodySink` (always
  ## continue): the FINAL response body streams to it and cannot be stopped early, so
  ## `res.bodyTruncated` is never set by it.
  let gate = newSinkGate()
  when B is AsyncBodyProducer:
    requestResolved(client, verb, target, headers, ResolvedBody(), form, params,
                    cancel, trailers, body, wrapSink(sink, gate), gate)
  else:
    requestResolved(client, verb, target, headers, toBody(body), form, params,
                    cancel, trailers, nil, wrapSink(sink, gate), gate)

include navi/private/impl_stream
include navi/private/stream_verbs
include navi/private/impl_sse
include navi/private/verbs
include navi/private/impl_ws
