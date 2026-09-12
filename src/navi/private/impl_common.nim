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
    timeouts: Timeouts(h2KeepAlive: defaultH2KeepAliveMs), middleware: @[])

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
  # Await any in-flight coalesced connects before closing the live tables: a connect
  # that resolves after we clear `muxes` would otherwise cache its mux into the
  # cleared table and never be closed, orphaning the connection and its reader
  # (issue #315). Snapshot the futures first -- a resolving connect `del`s its own
  # pending entry, so iterating the table directly would mutate it mid-iteration.
  var pendingMuxes: seq[Future[H2Mux]]
  for f in client.pendingMux.values: pendingMuxes.add f
  for f in pendingMuxes:
    try:
      let mux = await f
      if mux != nil: await mux.close()
    except CatchableError: discard   # a failed connect has nothing to close
  client.pendingMux.clear()
  for mux in client.muxes.values:
    await mux.close()
  client.muxes.clear()
  when defined(naviHttp3):
    var pendingConns: seq[Future[QuicConn]]
    for f in client.pendingH3.values: pendingConns.add f
    for f in pendingConns:
      try:
        let qc = await f
        if qc != nil: await qc.closeConn()
      except CatchableError: discard
    client.pendingH3.clear()
    for qc in client.h3conns.values:
      await qc.closeConn()
    client.h3conns.clear()
  closeTlsStore(client.config.tls.sessionCache)
  closeTlsCtxStore(client.config.tls.contextStore)

when defined(naviHttp3):
  proc h3ConnCount*(client: Navi): int = client.h3conns.len
    ## Live multiplexed HTTP/3 connections; for tests/introspection.

proc muxRequest(client: Navi, mux: H2Mux, req: Request,
                sink: BodySink): Future[Response] {.async.} =
  # The mux delivers a streaming request's body to `sink` incrementally (decoding
  # content-encoding as it arrives), so the returned response's body is empty.
  # A non-streaming request (sink == nil) still buffers into r.body as before.
  result = toResponse(await mux.request(h2HeaderList(req), req.body, req.bodyStream,
                                        sink, h2TrailerList(req)))

proc h1OnConn(client: Navi, conn: Conn, origin: string, req: Request,
              sink: BodySink): Future[Response] {.async.} =
  var keep = false
  result = h1Exchange(conn, req, sink, keep,
                      client.config.wantsDecompress, client.config.maxResponseBytes)
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
      client.muxes[origin] = mux
      client.pendingMux.del(origin)
      pending.complete(mux)
      return (conn, mux)
    else:
      client.pendingMux.del(origin)
      pending.complete(nil)          # this origin is http/1.1
      return (conn, H2Mux(nil))
  except CatchableError as e:
    client.pendingMux.del(origin)
    # A failure after the branch already completed `pending` (h1 fallback, or a
    # post-handshake error) must not complete the future twice (mirrors chronos).
    if not pending.finished: pending.fail(e)
    raise

proc transportInner(client: Navi, req: Request, sink: BodySink): Future[Response] {.async.} =
  ## Multiplex over a shared h2 connection when available/negotiable; otherwise
  ## pool http/1.1. Concurrent connects to the same new origin are coalesced so a
  ## cold burst still ends up on one h2 connection.
  let origin = originKey(req.url)
  let wantH2 = client.config.wantsH2 and req.url.isTls
  client.pruneDeadMuxes()          # evict muxes that died since the last request (#312)

  if wantH2:
    # 1. A live shared connection, or one currently being established.
    if client.muxes.hasKey(origin) and client.muxes[origin].canReuse:
      return await client.muxRequest(client.muxes[origin], req, sink)
    if client.pendingMux.hasKey(origin):
      let mux = await client.pendingMux[origin]
      if mux != nil and mux.canReuse:
        return await client.muxRequest(mux, req, sink)
      # else: turned out http/1.1, fall through

  # 2. A pooled http/1.1 connection.
  for dead in reapExpired(client.pool):    # close idle connections past idleConnTimeout
    await close(dead.transport)
  var (found, pc) = popIdle(client.pool, origin)
  if found:
    # `gotResponse` distinguishes a reused-connection failure BEFORE any response
    # byte (unprocessed: safe to replay any method) from one AFTER the response began
    # (processed: only an idempotent method may be replayed).
    var gotResponse = false
    try:
      var keep = false
      var parser = h1SendAndReadHeaders(pc.transport, req, not sink.isNil)
      gotResponse = true
      h1DrainBody(pc.transport, parser, sink, keep,
                  client.config.wantsDecompress, client.config.maxResponseBytes)
      result = parser.toResponse()
      if not (keep and pushIdle(client.pool, origin, pc)): await close(pc.transport)
      return
    except CatchableError as e:
      await close(pc.transport)  # stale
      # Safe to replay on a fresh connection when the request was not processed (a
      # reused connection dropped before any response) or the method is idempotent /
      # provably unprocessed; never replay a non-rewindable streamed body.
      let replayable = isReplayable(req)
      if not (replayable and
              (not gotResponse or isIdempotent(req.verb) or (e of UnprocessedError))):
        raise
      # else fall through to a fresh connection below

  # 3. Open a fresh connection (coalescing concurrent cold h2 connects).
  var rq = req
  rq.absoluteForm = usesAbsoluteForm(resolveProxy(client.config, rq.url), rq.url.isTls)
  let (conn, mux) = await client.openFreshConn(rq, origin, wantH2)
  if mux != nil:
    result = await client.muxRequest(mux, rq, sink)
  else:
    result = await client.h1OnConn(conn, origin, rq, sink)

when defined(naviHttp3):
  proc recordAltSvc(client: Navi, req: Request, resp: Response) =
    ## Cache an h3 endpoint the origin advertised, so later requests can upgrade.
    let alt = resp.headers.get("alt-svc")
    if alt.len > 0:
      client.altSvc.record("https", req.url.host, req.url.port, alt)

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
      raise

proc transport(client: Navi, req: Request, sink: BodySink): Future[Response] {.async.} =
  ## The wire transport `run` calls. In a `-d:naviHttp3` build, a buffered-body
  ## request to an origin that has advertised h3 (Alt-Svc) goes over HTTP/3, with
  ## any QUIC failure falling back to h2/h1; `alt-svc` on h2/h1 responses is
  ## captured for later upgrades.
  when defined(naviHttp3):
    if client.config.wantsH3 and req.url.isTls:   # buffered or streamed (bodyStream) body
      let ep = client.altSvc.h3Endpoint("https", req.url.host, req.url.port)
      if ep.isSome:
        try: return await h3Transport(client, req, ep.get)
        except QuicError: discard   # fall back to h2/h1 below
  result = await transportInner(client, req, sink)
  when defined(naviHttp3):
    client.recordAltSvc(req, result)

proc doRequest(client: Navi, req: Request): Future[Response] {.async.} =
  result = performRequest(client, req)

proc client*(ctx: NaviContext): Navi = ctx.clientv
  ## The client handling this request (e.g. to read `ctx.client.config`).

proc next*(ctx: NaviContext): Future[void] {.async.} =
  ## Run the rest of the chain: the next middleware, or -- once they are
  ## exhausted -- the request itself. The outcome lands in `ctx.res`.
  let mws = ctx.clientv.config.middleware
  if ctx.idx >= mws.len:
    ctx.res = await doRequest(ctx.clientv, ctx.req)
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

proc request*(client: Navi, verb: HttpVerb, target: string,
              headers = initHeaders(), body = "", json: JsonNode = nil,
              form: seq[(string, string)] = @[], multipart: Multipart = @[],
              bodyStream: BodyProducer = nil,
              params: seq[(string, string)] = @[],
              cancel: CancelToken = nil,
              trailers = initHeaders()): Future[Response] {.async.} =
  ## Perform a request; configured middleware wraps the whole call. `params` are
  ## appended to the URL query; `cancel` aborts the in-flight request. `trailers`
  ## are sent after the body (chunked on h1, a trailing HEADERS block on h2/h3).
  let req = buildRequest(client.config, verb, target, headers, body, json,
                         form, multipart, bodyStream, params, trailers)
  if client.config.middleware.len == 0:
    return await guard(client.config.totalMs, doRequest(client, req), cancel)
  let ctx = NaviContext(req: req, clientv: client)
  return await guard(client.config.totalMs, runChain(ctx), cancel)

include navi/private/impl_stream
include navi/private/impl_sse
include navi/private/verbs
include navi/private/impl_ws
