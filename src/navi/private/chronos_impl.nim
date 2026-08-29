# The native chronos implementation, `include`d by navi/chronos.nim on non-js
# targets. Kept separate so the entry can fall back to navi/js under `nim js`
# without pulling in the chronos package (which has no JavaScript backend).

import std/[options, tables]
import navi/private/[entryguard, streamguard]
import navi/proto/sse
import navi/core/public
export sse.SseEvent
import navi/core/[engine, pool, session, proxy, h2glue]
import navi/core/[redirect, cookies, digest, cancel, retry, response]
import navi/core/decompress   # StreamDecoder, for the readChunk decode state
import navi/proto/h1
import navi/proto/ws
import navi/backend/[chronos, h2mux_chronos]
from std/strutils import startsWith, find, splitLines, contains, toLowerAscii
when defined(naviHttp3):
  import navi/core/altsvc
  import navi/backend/quic_chronos

claimEntry("navi/chronos")
export public, chronos

type
  NaviContext* = ref object
    ## Carried through the middleware chain. A middleware reads and mutates it,
    ## then `await ctx.next()` runs the rest of the chain (which fills `res`).
    req*: Request            ## the outgoing request; modify it before `next`
    res*: Response           ## the response; set by `next`, adjust it after
    clientv: Navi            ## the owning client (see `client`)
    idx: int                 ## index of the next middleware to run
  NaviMiddleware* = proc(ctx: NaviContext): Future[void] {.closure, gcsafe.}
    ## A middleware step; may be async. A closure, so it can capture: read/modify
    ## `ctx.req`, `await ctx.next()` to proceed -- or skip it to short-circuit --
    ## then read/modify `ctx.res`. Write it as a plain `{.async.}` proc (identical
    ## spelling on every backend); a factory `proc bearer(token): NaviMiddleware`
    ## closes over config. chronos's strict-raises obligation is discharged
    ## internally by `next` (see the cast there), not in this public type.

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
      h3conns: TableRef[string, QuicConnChronos] ## live multiplexed h3 connections
      pendingH3: TableRef[string, Future[QuicConnChronos]] ## in-flight h3 connects

proc initNaviConfig*(): NaviConfig =
  ## The only way to build a config (`NaviConfig` requires every field). Sets the
  ## safe defaults; override the fields you want, then pass it to `newNavi`.
  NaviConfig(
    prefixUrl: "", headers: initHeaders(), http: {H1, H2}, tls: defaultTls(),
    decompress: true, throwHttpErrors: true, maxRedirects: 20,
    retry: defaultRetryPolicy(), maxResponseBytes: 0,
    auth: Auth(), proxy: "", unixSocket: "",
    maxIdleConns: 0, maxIdleConnsPerHost: 0, idleConnTimeout: 0,
    timeouts: Timeouts(), middleware: @[])

proc newNavi*(config = initNaviConfig()): Navi =
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
    result.h3conns = newTable[string, QuicConnChronos]()
    result.pendingH3 = newTable[string, Future[QuicConnChronos]]()

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
    result.h3conns = newTable[string, QuicConnChronos]()
    result.pendingH3 = newTable[string, Future[QuicConnChronos]]()

proc close*(client: Navi): Future[void] {.async.} =
  ## Close all pooled connections and shared h2 connections, freeing their TLS
  ## contexts. Any in-flight request on a shared connection fails with IOError.
  ## Optional but recommended when done with the client.
  for pc in client.pool.drain():
    await close(pc.transport)
  for mux in client.muxes.values:
    await mux.close()
  client.muxes.clear()
  when defined(naviHttp3):
    for qc in client.h3conns.values:
      await qc.closeConn()
    client.h3conns.clear()
  closeTlsStore(client.config.tls.sessionCache)
  closeTlsCtxStore(client.config.tls.contextStore)

when defined(naviHttp3):
  proc recordAltSvc(client: Navi, req: Request, resp: Response) =
    ## Cache an h3 endpoint the origin advertised, so later requests can upgrade.
    let alt = resp.headers.get("alt-svc")
    if alt.len > 0:
      client.altSvc.record("https", req.url.host, req.url.port, alt)

proc muxRequest(client: Navi, mux: H2Mux, req: Request,
                sink: BodySink): Future[Response] {.async.} =
  # The mux delivers a streaming request's body to `sink` incrementally (decoding
  # content-encoding as it arrives), so the returned response's body is empty.
  # A non-streaming request (sink == nil) still buffers into r.body as before.
  result = toResponse(await mux.request(h2HeaderList(req), req.body, req.bodyStream, sink))
  when defined(naviHttp3): client.recordAltSvc(req, result)

proc h1OnConn(client: Navi, conn: Conn, origin: string, req: Request,
              sink: BodySink): Future[Response] {.async.} =
  var keep = false
  result = h1Exchange(conn, req, sink, keep,
                      client.config.wantsDecompress, client.config.maxResponseBytes)
  when defined(naviHttp3): client.recordAltSvc(req, result)
  let pc = PooledConn[Conn](transport: conn)
  if not (keep and pushIdle(client.pool, origin, pc)):
    await close(conn)

when defined(naviHttp3):
  # Fields that must not cross to HTTP/3 (RFC 9114 connection-specific + the
  # pseudo-header sources). accept-encoding IS forwarded so decodeBody decodes.
  const h3SkipHeaders = ["host", "connection", "keep-alive", "proxy-connection",
                         "transfer-encoding", "upgrade", "content-length"]

  proc getH3Conn(client: Navi, origin: string, ep: AltSvcEndpoint,
                 req: Request): Future[QuicConnChronos] {.async.} =
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
      let pending = newFuture[QuicConnChronos]("navi.pendingH3")
      client.pendingH3[origin] = pending
      try:
        let qc = await openConnChronos(ep.host, ep.port, req.url.host,
                                       client.config.tls.caFile,
                                       client.config.tls.wantsVerify)
        client.h3conns[origin] = qc
        client.pendingH3.del(origin)
        pending.complete(qc)
        return qc
      except CatchableError as e:
        client.pendingH3.del(origin)
        if not pending.finished: pending.fail(e)
        raise

  proc h3TransportChronos(client: Navi, req: Request,
                          ep: AltSvcEndpoint): Future[Response] {.async.} =
    ## Send a buffered-body request over a shared HTTP/3 connection. Raises QuicError.
    var fwd: seq[(string, string)]
    for k, v in req.headers:
      let lk = k.toLowerAscii
      if lk notin h3SkipHeaders: fwd.add((lk, v))
    let origin = originKey(req.url)
    let qc = await client.getH3Conn(origin, ep, req)
    try:
      let r = await qc.requestOnConn($req.verb, req.url.requestTarget, fwd, req.body,
                                     req.bodyStream)
      result = initResponse(r.status, "", "HTTP/3", initHeaders(r.headers), r.body)
    except QuicError:
      if client.h3conns.getOrDefault(origin, nil) == qc:
        client.h3conns.del(origin)
      raise

proc transport(client: Navi, req: Request, sink: BodySink): Future[Response] {.async.} =
  ## Multiplex over a shared h2 connection when available/negotiable; otherwise
  ## pool http/1.1. Concurrent connects to the same new origin are coalesced so a
  ## cold burst still ends up on one h2 connection. In a -d:naviHttp3 build, a
  ## buffered request to an h3-advertised origin (Alt-Svc) goes over HTTP/3.
  let origin = originKey(req.url)
  let wantH2 = client.config.wantsH2 and req.url.isTls

  when defined(naviHttp3):
    if client.config.wantsH3 and req.url.isTls:   # buffered or streamed (bodyStream) body
      let ep = client.altSvc.h3Endpoint("https", req.url.host, req.url.port)
      if ep.isSome:
        try: return await h3TransportChronos(client, req, ep.get)
        except QuicError: discard   # fall back to h2/h1 below

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
      let replayable = req.bodyStream == nil
      if not (replayable and
              (not gotResponse or isIdempotent(req.verb) or (e of UnprocessedError))):
        raise
      # else fall through to a fresh connection below

  # 3. Open a fresh connection.
  var rq = req
  let proxyTarget = resolveProxy(client.config, rq.url)
  rq.absoluteForm = usesAbsoluteForm(proxyTarget, rq.url.isTls)
  let alpn = if wantH2: @["h2", "http/1.1"] else: @[]

  if wantH2:
    let pending = newFuture[H2Mux]("navi.pendingMux")
    client.pendingMux[origin] = pending
    try:
      let conn = await connect(rq.url.host, rq.url.port, rq.url.isTls,
                               client.config.tls, proxyTarget, alpn,
                               client.config.connectMs, client.config.readMs)
      if conn.protocol == "h2":
        let mux = await newH2Mux(conn, client.config.maxResponseBytes,
                                 client.config.wantsDecompress)
        client.muxes[origin] = mux
        client.pendingMux.del(origin)
        pending.complete(mux)
        return await client.muxRequest(mux, rq, sink)
      else:
        client.pendingMux.del(origin)
        pending.complete(nil)  # this origin is http/1.1
        return await client.h1OnConn(conn, origin, rq, sink)
    except CatchableError as e:
      client.pendingMux.del(origin)
      # In TLS 1.3 a post-handshake failure (e.g. a rejected client cert) can raise
      # after the http/1.1 branch already completed `pending` with nil, so only fail
      # it when it is still pending -- otherwise we'd complete the future twice.
      if not pending.finished: pending.fail(e)
      raise

  let conn = await connect(rq.url.host, rq.url.port, rq.url.isTls,
                           client.config.tls, proxyTarget, alpn,
                           client.config.connectMs, client.config.readMs)
  result = await client.h1OnConn(conn, origin, rq, sink)

proc doRequest(client: Navi, req: Request): Future[Response] {.async.} =
  result = performRequest(client, req)

proc guard[T](client: Navi, fut: Future[T], cancel: CancelToken): Future[T] {.async.} =
  ## Bound the whole operation by `timeout` and `cancel`. On either, the in-flight
  ## request is cancelled via chronos structured cancellation (its cleanup closes
  ## the socket) and TimeoutError / RequestCancelledError is raised.
  let ms = client.config.totalMs
  if ms <= 0 and cancel == nil:
    return await fut
  var cancelFut = newFuture[void]("navi.cancel")
  if cancel != nil:
    cancel.armHook(proc() {.gcsafe, raises: [].} =
      if not cancelFut.finished: cancelFut.complete())
  var timer: Future[void] = nil
  if ms > 0: timer = sleepAsync(ms.milliseconds)
  try:
    var cands = @[FutureBase(fut), FutureBase(cancelFut)]
    if timer != nil: cands.add(FutureBase(timer))
    discard await race(cands)
    if fut.finished:
      return await fut
    await fut.cancelAndWait()
    if cancel != nil and cancel.cancelled:
      raise newException(RequestCancelledError, "navi: request cancelled")
    raise newException(TimeoutError, "navi: request timed out after " & $ms & " ms")
  finally:
    if cancel != nil: cancel.disarmHook()
    if not cancelFut.finished: cancelFut.complete()
    if timer != nil and not timer.finished: timer.cancelSoon()

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
              cancel: CancelToken = nil): Future[Response] {.async.} =
  ## `params` are appended to the URL query; `cancel` aborts the in-flight request.
  let req = buildRequest(client.config, verb, target, headers, body, json,
                         form, multipart, bodyStream, params)
  if client.config.middleware.len == 0:
    return await client.guard(doRequest(client, req), cancel)
  let ctx = NaviContext(req: req, clientv: client)
  return await client.guard(runChain(ctx), cancel)

# --- Streaming downloads (pull-based handle) ---

type
  StreamKind = enum skH1, skH2, skH3
  StreamResponseObj = object
    ## The response of a streaming request: status/headers are available
    ## immediately while the body is drained on demand. Holds either a checked-out
    ## http/1.1 connection (removed from the pool) or an open stream on the shared
    ## h2 mux, until `drain` finishes it or `close` disposes it.
    resp: Response             ## header snapshot (status/headers; empty body)
    client: Navi
    key: string                ## origin key, for returning an h1 connection to the pool
    decompress: bool
    cap: int
    cancel: CancelToken
    drained: bool              ## body fully read; connection returned/finished
    closed: bool               ## disposed without draining
    guard: StreamGuard         ## closes/resets if the handle is dropped before
                               ## drain/close (see navi/private/streamguard)
    dec: StreamDecoder         ## h1 decode + size-cap state carried across readChunk
    decReady: bool             ## calls (h2 keeps its decoder in the mux)
    seen: int
    case kind: StreamKind
    of skH1:
      transport: Conn          ## the checked-out http/1.1 connection
      parser: H1Parser
    of skH2:
      mux: H2Mux               ## the shared connection (stays live for reuse)
      sid: uint32              ## our stream on it
    of skH3:
      when defined(naviHttp3):
        qc: QuicConnChronos    ## the shared h3 connection (stays live for reuse)
        h3sid: int64           ## our QUIC stream on it
      else: discard
  StreamResponse* = ref StreamResponseObj

proc close*(sr: StreamResponse): Future[void] {.async.} =
  ## Dispose a streaming handle whose body will not be fully drained: closes the
  ## http/1.1 connection (a partially-read response cannot be pooled) or resets the
  ## h2 stream (the shared connection stays up). Idempotent; a no-op once drained.
  if sr.drained or sr.closed: return
  sr.closed = true
  disarm(sr.guard)                    # we do the awaitable teardown ourselves
  case sr.kind
  of skH1: await close(sr.transport)
  of skH2: await sr.mux.abandon(sr.sid)
  of skH3:
    when defined(naviHttp3): sr.qc.freeStream(sr.h3sid)  # STOP_SENDING; conn stays up
    else: discard

proc status*(sr: StreamResponse): int = sr.resp.status
  ## The response status code, available before the body is drained.
proc reason*(sr: StreamResponse): string = sr.resp.reason
proc httpVersion*(sr: StreamResponse): string = sr.resp.httpVersion
proc headers*(sr: StreamResponse): Headers = sr.resp.headers
proc ok*(sr: StreamResponse): bool = sr.resp.ok
  ## Whether the status is 2xx (checked by the caller; the pull API never throws).

proc openStreamConn(client: Navi, req: Request): Future[StreamResponse] {.async.} =
  ## Send one request and read its headers, returning a handle with the body
  ## pending: multiplexed over a shared h2 connection when available/negotiable,
  ## otherwise a pooled http/1.1 connection. Mirrors `transport`, but stops at the
  ## response headers. Does not throw on non-2xx.
  let origin = originKey(req.url)
  let wantH2 = client.config.wantsH2 and req.url.isTls
  let decompress = client.config.wantsDecompress
  let cap = client.config.maxResponseBytes

  when defined(naviHttp3):
    # Stream over HTTP/3 when the origin advertised h3 (Alt-Svc); any QUIC failure
    # falls through to h2/h1. Mirrors the buffered h3TransportChronos path.
    if client.config.wantsH3 and req.url.isTls and req.bodyStream == nil:
      let ep = client.altSvc.h3Endpoint("https", req.url.host, req.url.port)
      if ep.isSome:
        try:
          let qc = await client.getH3Conn(origin, ep.get, req)
          var fwd: seq[(string, string)]
          for k, v in req.headers:
            let lk = k.toLowerAscii
            if lk notin h3SkipHeaders: fwd.add((lk, v))
          let sid = qc.submitStream($req.verb, req.url.requestTarget, fwd, req.body)
          if sid >= 0:
            try:
              let (status, hdrs) = await qc.awaitHeaders(sid)
              return StreamResponse(kind: skH3, qc: qc, h3sid: sid,
                resp: initResponse(status, "", "HTTP/3", initHeaders(hdrs), ""),
                client: client, key: origin, decompress: decompress, cap: cap)
            except CatchableError:                 # header wait failed or was cancelled
              qc.freeStream(sid)                   # (e.g. timeout): free the submitted
              raise                                # stream so it isn't left on the wire
        except QuicError:
          if client.h3conns.getOrDefault(origin, nil) != nil and
             not client.h3conns[origin].alive:
            client.h3conns.del(origin)

  if wantH2:
    if client.muxes.hasKey(origin) and client.muxes[origin].canReuse:
      let mux = client.muxes[origin]
      let sid = await mux.sendAndReadHeaders(h2HeaderList(req), req.body, req.bodyStream)
      return StreamResponse(kind: skH2, mux: mux, sid: sid,
        resp: toResponse(mux.respSnapshot(sid)), client: client, key: origin,
        decompress: decompress, cap: cap)
    if client.pendingMux.hasKey(origin):
      let mux = await client.pendingMux[origin]
      if mux != nil and mux.canReuse:
        let sid = await mux.sendAndReadHeaders(h2HeaderList(req), req.body, req.bodyStream)
        return StreamResponse(kind: skH2, mux: mux, sid: sid,
          resp: toResponse(mux.respSnapshot(sid)), client: client, key: origin,
          decompress: decompress, cap: cap)

  var (found, pc) = popIdle(client.pool, origin)
  if found:
    try:
      let parser = h1SendAndReadHeaders(pc.transport, req, true)
      return StreamResponse(kind: skH1, transport: pc.transport, parser: parser,
        resp: parser.toResponse(), client: client, key: origin,
        decompress: decompress, cap: cap)
    except CatchableError:
      await close(pc.transport)     # pooled connection was stale; open a fresh one

  var rq = req
  let proxyTarget = resolveProxy(client.config, rq.url)
  rq.absoluteForm = usesAbsoluteForm(proxyTarget, rq.url.isTls)
  let alpn = if wantH2: @["h2", "http/1.1"] else: @[]

  if wantH2:
    let pending = newFuture[H2Mux]("navi.pendingMux")
    client.pendingMux[origin] = pending
    try:
      let conn = await connect(rq.url.host, rq.url.port, rq.url.isTls,
                               client.config.tls, proxyTarget, alpn,
                               client.config.connectMs, client.config.readMs)
      if conn.protocol == "h2":
        let mux = await newH2Mux(conn, client.config.maxResponseBytes, decompress)
        client.muxes[origin] = mux
        client.pendingMux.del(origin)
        pending.complete(mux)
        let sid = await mux.sendAndReadHeaders(h2HeaderList(rq), rq.body, rq.bodyStream)
        return StreamResponse(kind: skH2, mux: mux, sid: sid,
          resp: toResponse(mux.respSnapshot(sid)), client: client, key: origin,
          decompress: decompress, cap: cap)
      else:
        client.pendingMux.del(origin)
        pending.complete(nil)
        let parser = h1SendAndReadHeaders(conn, rq, true)
        return StreamResponse(kind: skH1, transport: conn, parser: parser,
          resp: parser.toResponse(), client: client, key: origin,
          decompress: decompress, cap: cap)
    except CatchableError as e:
      client.pendingMux.del(origin)
      # In TLS 1.3 a post-handshake failure (e.g. a rejected client cert) can raise
      # after the http/1.1 branch already completed `pending` with nil, so only fail
      # it when it is still pending -- otherwise we'd complete the future twice.
      if not pending.finished: pending.fail(e)
      raise

  let conn = await connect(rq.url.host, rq.url.port, rq.url.isTls,
                           client.config.tls, proxyTarget, alpn,
                           client.config.connectMs, client.config.readMs)
  let parser = h1SendAndReadHeaders(conn, rq, true)
  return StreamResponse(kind: skH1, transport: conn, parser: parser,
    resp: parser.toResponse(), client: client, key: origin,
    decompress: decompress, cap: cap)

proc stream*(client: Navi, verb: HttpVerb, target: string,
             headers = initHeaders(), params: seq[(string, string)] = @[],
             cancel: CancelToken = nil): Future[StreamResponse] {.async.} =
  ## Open a streaming response: perform the request, follow redirects and digest
  ## auth to the final response, and return a handle whose status/headers are
  ## available immediately while the body streams on demand via `each`/`drain`.
  ##
  ## Unlike `request`, this does NOT throw on a non-2xx status (inspect `status`),
  ## and middleware is not applied. Redirect/digest hops are closed. Consume the
  ## returned handle with `each`/`drain`, or `close` it to skip the body.
  var rreq = buildRequest(client.config, verb, target, headers, params = params)
  var hops = 0
  let limit = client.config.redirectLimit
  while true:
    throwIfCancelled(cancel)
    applyCookies(client.jar, rreq)
    let handle = await openStreamConn(client, rreq)
    handle.cancel = cancel
    when defined(naviHttp3): client.recordAltSvc(rreq, handle.resp)  # learn h3 from a
                                                                     # streamed response too
    # Arm the leak-guard for the synchronous fallback teardown if the handle is
    # dropped without drain/close. Captures only the connection essentials (never
    # `handle`, which would cycle): the h1 transport, or the mux + stream id.
    case handle.kind
    of skH1:
      let t = handle.transport
      handle.guard = newStreamGuard(proc() {.gcsafe, raises: [].} =
        {.cast(gcsafe).}:
          try: t.closeSync()
          except Exception: discard)     # best-effort finalizer: never propagate
    of skH2:
      let mux = handle.mux
      let sid = handle.sid
      handle.guard = newStreamGuard(proc() {.gcsafe, raises: [].} =
        {.cast(gcsafe).}:
          try: (if mux != nil: mux.dropStream(sid))
          except Exception: discard)
    of skH3:
      when defined(naviHttp3):
        let qc = handle.qc
        let sid = handle.h3sid
        handle.guard = newStreamGuard(proc() {.gcsafe, raises: [].} =
          {.cast(gcsafe).}:
            try: (if qc != nil: qc.freeStream(sid))
            except Exception: discard)
      else: discard
    storeCookies(client.jar, rreq.url, handle.resp)
    if handle.status == 401 and client.config.auth.kind == akDigest and
       not rreq.headers.contains("authorization"):
      let chal = bestChallenge(handle.headers.getAll("www-authenticate"))
      if chal.isSome:
        let auth = digestAuthHeader(client.config.auth.user, client.config.auth.pass,
                                    $rreq.verb, rreq.url.requestTarget, chal.get)
        if auth.len > 0:
          await handle.close()
          rreq.headers["authorization"] = auth
          continue
    let location = handle.headers.get("location")
    if limit > 0 and hops < limit and isRedirect(handle.status) and location.len > 0:
      await handle.close()
      rreq = redirectRequest(rreq, handle.status, location)
      inc hops
    else:
      return handle

proc readChunk*(sr: StreamResponse): Future[string] {.async.} =
  ## Pull the next decoded body chunk, or "" once the body is fully read. At end an
  ## h1 connection is returned to the pool (or closed) and an h2 stream is dropped
  ## on the shared connection, and the guard is disarmed; a cap breach or h2 reset
  ## closes/drops and reraises. The guard stays armed across the incremental reads,
  ## so a handle dropped before EOF is still cleaned up by it.
  if sr.drained or sr.closed: return ""
  case sr.kind
  of skH2:
    try:
      result = await sr.mux.readChunk(sr.sid)
      if result.len == 0:                 # stream ended; readChunk dropped it
        sr.drained = true
        disarm(sr.guard)
    except CatchableError:
      if not sr.drained: sr.drained = true
      disarm(sr.guard)                    # readChunk dropped the stream; mux stays up
      raise
  of skH1:
    try:
      result = h1ReadChunk(sr.transport, sr.parser,
                           sr.dec, sr.decReady, sr.seen, sr.decompress, sr.cap)
      if result.len == 0:                 # end of body: we own the teardown now
        sr.drained = true
        disarm(sr.guard)
        if not (sr.parser.keepAliveAfter() and
                pushIdle(sr.client.pool, sr.key, PooledConn[Conn](transport: sr.transport))):
          await close(sr.transport)
    except CatchableError:
      if not sr.drained: sr.drained = true
      disarm(sr.guard)
      await close(sr.transport)
      raise
  of skH3:
    when defined(naviHttp3):
      # h3 body arrives raw; apply the same streamed decode + size-cap as h1, then
      # free the stream at EOF (a reset surfaces as an error). The connection stays up.
      try:
        while true:
          let raw = await sr.qc.readStreamBody(sr.h3sid)
          if raw.len == 0:                # EOF
            sr.drained = true
            disarm(sr.guard)
            let wasReset = sr.qc.streamWasReset(sr.h3sid)
            let lengthBad = sr.qc.streamLengthMismatch(sr.h3sid)  # before freeStream
            sr.qc.freeStream(sr.h3sid)
            if wasReset: raise newException(IOError, "navi: http/3 stream reset")
            if lengthBad: raise newException(IOError, h3BodyLengthErr)
            return ""
          if not sr.decReady:
            sr.dec = if sr.decompress:
                newStreamDecoder(sr.resp.headers.get("content-encoding")) else: nil
            sr.decReady = true
          let decoded = if sr.dec != nil:
              sr.dec.update(raw.toOpenArrayByte(0, raw.high)) else: raw
          if decoded.len == 0: continue   # decoder buffered input; pull more
          sr.seen += decoded.len
          if sr.cap > 0 and sr.seen > sr.cap:
            raise newException(ResponseTooLargeError,
              "navi: response exceeded maxResponseBytes")
          return decoded
      except CatchableError:
        if not sr.drained: sr.drained = true
        disarm(sr.guard)
        sr.qc.freeStream(sr.h3sid)
        raise
    else: discard

proc drain*(sr: StreamResponse, sink: BodySink): Future[void] {.async.} =
  ## Deliver the response body to `sink` as it arrives (decoded and size-capped),
  ## awaiting it per chunk so a slow sink backpressures the peer. Then return an
  ## http/1.1 connection to the pool (or close it if it cannot be reused); an h2
  ## stream just finishes on the shared connection. Consumes the handle: call once.
  ## On error the connection is closed/reset and the error re-raised. Prefer `each`.
  if sr.drained or sr.closed:
    raise newException(IOError, "navi: stream already drained or closed")
  throwIfCancelled(sr.cancel)
  disarm(sr.guard)                         # from here `drain` owns the teardown
  case sr.kind
  of skH2:
    try:
      await sr.mux.drainDownload(sr.sid, sink)
      sr.drained = true                    # drainDownload freed the stream
    except CatchableError:
      sr.drained = true                    # ...on error too, so the guard won't double-free
      raise
  of skH1:
    try:
      var keep = false
      h1DrainBody(sr.transport, sr.parser, sink, keep, sr.decompress, sr.cap)
      sr.drained = true
      if not (keep and pushIdle(sr.client.pool, sr.key, PooledConn[Conn](transport: sr.transport))):
        await close(sr.transport)
    except CatchableError:
      if not sr.drained: sr.drained = true   # mark consumed for the "call once" guard
      await close(sr.transport)
      raise
  of skH3:
    when defined(naviHttp3):
      # Reuse readChunk's decode/cap/free per chunk; it sets `drained` at EOF. The
      # sink is a bare closure (no chronos raises annotation); navi's contract is it
      # raises at most CatchableError -- discharge chronos's strict effects here, as
      # drainDownload does.
      while true:
        let chunk = await sr.readChunk()
        if chunk.len == 0: break
        {.cast(gcsafe).}:
          {.cast(raises: [CatchableError]).}:
            await sink(chunk)
    else: discard

template each*(sr: StreamResponse; chunk, body: untyped): untyped =
  ## Drain the streaming body, running `body` for each decoded chunk with `chunk`
  ## bound to it (an owned `string`, moved from navi's read buffer). The outer
  ## `await` is baked in, so call it inside an async proc without one:
  ##   let res = await api.stream(GET, url)
  ##   res.each(chunk): await outFile.write(chunk)
  ##
  ## `body` runs as a proc, so `break`/`continue`/`return` cannot escape the loop
  ## from inside it. To stop early, don't call `each` and `close` the handle, or
  ## raise from `body` (which closes the connection and propagates).
  await sr.drain(proc(chunk: string): Future[void] {.async.} = body)

# --- Server-Sent Events (text/event-stream) ---

type
  SseStreamObj = object
    ## A first-class SSE stream. Pulls parsed events via `next`/`each`, reconnecting
    ## transparently (Last-Event-ID + the server's retry:) unless `reconnect` is off.
    client: Navi
    verb: HttpVerb
    target: string
    headers: Headers
    params: seq[(string, string)]
    cancel: CancelToken
    reconnect: bool
    baseRetryMs: int
    retryMs: int
    maxRetryMs: int
    handle: StreamResponse
    parser: SseParser
    started: bool
    closed: bool
  SseStream* = ref SseStreamObj

proc openConn(s: SseStream): Future[void] {.async.} =
  ## (Re)open the underlying stream and require a 200 text/event-stream response.
  s.parser.reset()
  var h = s.headers
  let lid = s.parser.lastEventId()
  if lid.len > 0: h["last-event-id"] = lid
  let handle = await s.client.stream(s.verb, s.target, h, s.params, s.cancel)
  if handle.status != 200:
    await handle.close()
    raise newException(IOError, "navi: SSE got status " & $handle.status &
      " (expected 200)")
  if not handle.headers.get("content-type").toLowerAscii.startsWith("text/event-stream"):
    let ct = handle.headers.get("content-type")
    await handle.close()
    raise newException(IOError,
      "navi: SSE expected Content-Type text/event-stream, got '" & ct & "'")
  s.handle = handle

proc sse*(client: Navi, target: string, verb = GET,
          headers = initHeaders(), body = "",
          params: seq[(string, string)] = @[],
          lastEventId = "", reconnect = true,
          retryMs = 3000, maxRetryMs = 30_000,
          cancel: CancelToken = nil): Future[SseStream] {.async.} =
  ## Open a Server-Sent Events stream. The initial response is validated up front (a
  ## non-200 or non `text/event-stream` response raises). Consume with `next` (none
  ## at end) or `each` (a real loop, so break/return work). Reconnects transparently
  ## on a drop, resending Last-Event-ID and honoring the server's retry: (backoff to
  ## `maxRetryMs`), unless `reconnect` is false. The underlying stream runs with the
  ## size cap and read/total timeouts off and shares the client's cookie jar.
  var cfg = client.config
  cfg.maxResponseBytes = 0
  cfg.timeouts.read = 0
  cfg.timeouts.total = 0
  var h = headers
  if not h.contains("accept"): h["accept"] = "text/event-stream"
  if not h.contains("cache-control"): h["cache-control"] = "no-cache"
  let s = SseStream(
    client: newNavi(cfg), verb: verb, target: target, headers: h, params: params,
    cancel: cancel, reconnect: reconnect, baseRetryMs: retryMs, retryMs: retryMs,
    maxRetryMs: maxRetryMs, parser: initSseParser(lastEventId))
  s.client.jar = client.jar
  await s.openConn()
  s.started = true
  return s

proc close*(s: SseStream): Future[void] {.async.} =
  ## Stop consuming and dispose the connection, including the dedicated internal
  ## client (its pool). Idempotent. Call it when done with the stream.
  if s.closed: return
  s.closed = true
  if s.handle != nil:
    await s.handle.close()
    s.handle = nil
  await s.client.close()

proc httpVersion*(s: SseStream): string =
  ## HTTP version of the current underlying connection, or "" between reconnects.
  ## An SSE stream starts on h1/h2 and upgrades to h3 only after a reconnect.
  if s.handle != nil: s.handle.httpVersion else: ""

proc lastEventId*(s: SseStream): string = s.parser.lastEventId()

proc next*(s: SseStream): Future[Option[SseEvent]] {.async.} =
  ## The next event, or none once the stream ends. Reconnects transparently on a
  ## drop when enabled, resending Last-Event-ID.
  if s.closed: return none(SseEvent)
  while true:
    let ev = s.parser.next()
    if ev.isSome:
      if s.parser.retryMs() >= 0:
        s.baseRetryMs = min(s.parser.retryMs(), s.maxRetryMs)
      return ev
    if s.handle == nil:
      if not s.reconnect: return none(SseEvent)
      await sleepAsync(min(s.retryMs, s.maxRetryMs).milliseconds)
      try:
        await s.openConn()
        s.retryMs = s.baseRetryMs
      except CatchableError:
        if s.closed: return none(SseEvent)
        s.retryMs = min(max(s.retryMs, s.baseRetryMs) * 2, s.maxRetryMs)
        continue
    var chunk = ""
    try:
      chunk = await s.handle.readChunk()
    except CatchableError:
      s.handle = nil
      if not s.reconnect: raise
      continue
    if chunk.len == 0:
      s.handle = nil
      if not s.reconnect: return none(SseEvent)
      continue
    s.parser.feed(chunk)

template each*(s: SseStream; ev, body: untyped): untyped =
  ## Consume events until the stream ends, binding `ev` to each `SseEvent`. A real
  ## loop (over the awaited `next`), so break/continue/return work.
  while true:
    let evOpt = await s.next()
    if evOpt.isNone: break
    let ev = evOpt.get
    body

include navi/private/verbs

# --- WebSocket (RFC 6455) ---

export ws.WsMessage, ws.WsMessageKind, ws.closeNormal, ws.closeGoingAway

type
  WebSocket* = ref object
    conn: Conn
    dec: WsDecoder
    asmb: WsAssembler
    open: bool

proc toWsUrl(url: string): Url =
  var s = url
  if s.startsWith("ws://"): s = "http://" & s["ws://".len .. ^1]
  elif s.startsWith("wss://"): s = "https://" & s["wss://".len .. ^1]
  parseUrl(s)

proc doWebsocket(client: Navi, url: string,
                 headers = initHeaders()): Future[WebSocket] {.async.} =
  let u = toWsUrl(url)
  let conn = await connect(u.host, u.port, u.isTls, client.config.tls,
                           resolveProxy(client.config, u), @[],
                           client.config.connectMs)
  let key = genKey()
  # Close the connection on any handshake failure (its close is async, so this
  # uses try/except rather than defer). A timeout cancels this future, raising
  # CancelledError here (a CatchableError), so the connection is closed too.
  try:
    await conn.sendAll(upgradeRequest(u, key, headers))
    var buf = ""
    while "\r\n\r\n" notin buf:
      let chunk = await conn.recvSome()
      if chunk.len == 0:
        raise newException(IOError, "navi: websocket handshake closed by peer")
      buf.add chunk
    let headEnd = buf.find("\r\n\r\n") + 4
    if not validate101(buf[0 ..< headEnd], key):
      raise newException(IOError, "navi: websocket upgrade rejected: " &
        buf[0 ..< headEnd].splitLines[0])
    result = WebSocket(conn: conn, open: true)
    if buf.len > headEnd:
      result.dec.feed(buf[headEnd .. ^1])
  except CatchableError:
    await conn.close()
    raise

proc websocket*(client: Navi, url: string,
                headers = initHeaders()): Future[WebSocket] {.async.} =
  ## Open a WebSocket connection (RFC 6455). Accepts `ws://` / `wss://` (or
  ## http/https); `wss` uses TLS. Does the HTTP/1.1 Upgrade over the transport and
  ## validates `Sec-WebSocket-Accept`. The whole open (connect, TLS handshake, and
  ## Upgrade) is bounded by `timeout`. Use `send`, `receive`, and `close`.
  result = await client.guard(doWebsocket(client, url, headers), nil)

proc send*(ws: WebSocket, data: string, binary = false): Future[void] {.async.} =
  ## Send a text (default) or binary message. Client frames are masked.
  await ws.conn.sendAll(encodeFrame(if binary: opBinary else: opText, data))

proc ping*(ws: WebSocket, data = ""): Future[void] {.async.} =
  await ws.conn.sendAll(encodeFrame(opPing, data))

proc receive*(ws: WebSocket): Future[WsMessage] {.async.} =
  ## Await a full message, answering pings and reassembling fragments. A close
  ## returns `wmClose` (and the connection is then closed).
  while true:
    var f: Frame
    while not ws.dec.next(f):
      let chunk = await ws.conn.recvSome()
      if chunk.len == 0:
        ws.open = false
        return WsMessage(kind: wmClose, closeCode: closeGoingAway)
      ws.dec.feed(chunk)
    let o = ws.asmb.offer(f)
    case o.reply
    of wrPong:
      await ws.conn.sendAll(encodeFrame(opPong, o.replyPayload))
    of wrCloseEcho:
      if ws.open:
        try: await ws.conn.sendAll(encodeFrame(opClose, o.replyPayload))
        except CatchableError: discard
        ws.open = false
        await ws.conn.close()
    of wrNone: discard
    if o.ready: return o.message

proc close*(ws: WebSocket, code = closeNormal, reason = ""): Future[void] {.async.} =
  ## Send a close frame and close the transport (freeing its TLS context).
  ## Idempotent: a no-op once the socket is already closed.
  if not ws.open: return
  try: await ws.conn.sendAll(encodeFrame(opClose, closePayload(code, reason)))
  except CatchableError: discard
  ws.open = false
  await ws.conn.close()
