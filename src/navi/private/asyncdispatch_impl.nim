# The native asyncdispatch implementation, `include`d by navi/asyncdispatch.nim
# on non-js targets. Kept separate so the entry can fall back to navi/js under
# `nim js` without pulling in std/asyncdispatch (which has no JS backend).

import std/[tables, options]
import navi/private/[entryguard, streamguard]
import navi/proto/sse
from std/strutils import toLowerAscii, startsWith, contains
export sse.SseEvent
import navi/core/public
import navi/core/[engine, pool, session, proxy, h2glue]
import navi/core/[redirect, cookies, digest, cancel, retry, response]
import navi/core/decompress   # StreamDecoder, for the h1 readChunk decode state
import navi/proto/h1
import navi/proto/ws
import navi/backend/[asyncdispatch, h2mux]
from std/strutils import startsWith, find, splitLines, contains
when defined(naviHttp3):
  import navi/core/altsvc
  import navi/backend/quic_async

claimEntry("navi/asyncdispatch")
export public, asyncdispatch

type
  NaviContext* = ref object
    ## Carried through the middleware chain. A middleware reads and mutates it,
    ## then `await ctx.next()` runs the rest of the chain (which fills `res`).
    req*: Request            ## the outgoing request; modify it before `next`
    res*: Response           ## the response; set by `next`, adjust it after
    clientv: Navi            ## the owning client (see `client`)
    idx: int                 ## index of the next middleware to run
  NaviMiddleware* = proc(ctx: NaviContext): Future[void] {.closure.}
    ## A middleware step; may be async. A closure, so it can capture: read/modify
    ## `ctx.req`, `await ctx.next()` to proceed -- or skip it to short-circuit --
    ## then read/modify `ctx.res`. Write it as a plain `{.async.}` proc (identical
    ## spelling on every backend); a factory `proc bearer(token): NaviMiddleware`
    ## closes over per-instance config. (No `gcsafe`: asyncdispatch does not
    ## require it -- only chronos does, at its call site.)

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
      h3conns: TableRef[string, QuicConnAsync]  ## live multiplexed h3 connections

proc initNaviConfig*(): NaviConfig =
  ## The only way to build a config (`NaviConfig` requires every field). Sets the
  ## safe defaults; override the fields you want, then pass it to `newNavi`.
  NaviConfig(
    prefixUrl: "", headers: initHeaders(), http: defaultHttpVersions, tls: defaultTls(),
    decompress: true, throwHttpErrors: true, maxRedirects: 20,
    retry: defaultRetryPolicy(), maxResponseBytes: 0,
    auth: Auth(), proxy: "", unixSocket: "",
    maxIdleConns: 0, maxIdleConnsPerHost: 0, idleConnTimeout: 0,
    timeouts: Timeouts(), middleware: @[])

proc newNavi*(config = initNaviConfig()): Navi =
  when not defined(naviHttp3):
    # H3 in `http` is a silent no-op without -d:naviHttp3 (h1/h2 only); warn once.
    var h3BuildWarned {.global.} = false
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
    result.h3conns = newTable[string, QuicConnAsync]()

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
    result.h3conns = newTable[string, QuicConnAsync]()

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

proc transportInner(client: Navi, req: Request, sink: BodySink): Future[Response] {.async.} =
  ## Multiplex over a shared h2 connection when available/negotiable; otherwise
  ## pool http/1.1. Concurrent connects to the same new origin are coalesced so a
  ## cold burst still ends up on one h2 connection.
  let origin = originKey(req.url)
  let wantH2 = client.config.wantsH2 and req.url.isTls

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
      # A failure after the branch already completed `pending` (h1 fallback via
      # `complete(nil)`, or a post-handshake error like a rejected client cert while
      # reading the response) must not complete the future twice (mirrors chronos).
      if not pending.finished: pending.fail(e)
      raise

  let conn = await connect(rq.url.host, rq.url.port, rq.url.isTls,
                           client.config.tls, proxyTarget, alpn,
                           client.config.connectMs, client.config.readMs)
  result = await client.h1OnConn(conn, origin, rq, sink)

when defined(naviHttp3):
  # Fields that must not cross to HTTP/3 (RFC 9114 connection-specific + the
  # pseudo-header sources). accept-encoding IS forwarded so decodeBody decodes.
  const h3SkipHeaders = ["host", "connection", "keep-alive", "proxy-connection",
                         "transfer-encoding", "upgrade", "content-length"]

  proc recordAltSvc(client: Navi, req: Request, resp: Response) =
    ## Cache an h3 endpoint the origin advertised, so later requests can upgrade.
    let alt = resp.headers.get("alt-svc")
    if alt.len > 0:
      client.altSvc.record("https", req.url.host, req.url.port, alt)

  proc getH3Conn(client: Navi, origin: string, ep: AltSvcEndpoint,
                 req: Request): Future[QuicConnAsync] {.async.} =
    ## Reuse the origin's live h3 connection, or open one and cache it. A cold
    ## race (two opens at once) closes the loser rather than leaking it.
    if client.h3conns.hasKey(origin) and client.h3conns[origin].alive:
      return client.h3conns[origin]
    let qc = await openConnAsync(ep.host, ep.port, req.url.host,
                                 client.config.tls.caFile,
                                 client.config.tls.wantsVerify)
    if client.h3conns.hasKey(origin) and client.h3conns[origin].alive:
      await qc.closeConn()               # someone else won the race
      return client.h3conns[origin]
    client.h3conns[origin] = qc
    return qc

  proc h3TransportAsync(client: Navi, req: Request,
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
        try: return await h3TransportAsync(client, req, ep.get)
        except QuicError: discard   # fall back to h2/h1 below
  result = await transportInner(client, req, sink)
  when defined(naviHttp3):
    client.recordAltSvc(req, result)

proc doRequest(client: Navi, req: Request): Future[Response] {.async.} =
  result = performRequest(client, req)

proc guard(client: Navi, fut: Future[Response],
           cancel: CancelToken): Future[Response] {.async.} =
  ## Bound the whole request (all attempts) by `timeout` and `cancel`. On expiry
  ## or cancellation the abandoned future runs to completion in the background
  ## (asyncdispatch has no true cancellation); its socket is later reclaimed.
  let ms = client.config.totalMs
  if ms <= 0 and cancel == nil:
    return await fut
  var cancelFut = newFuture[void]("navi.cancel")
  if cancel != nil:
    cancel.armHook(proc() {.gcsafe, raises: [].} =
      # complete() only raises if already finished, which the guard rules out.
      {.cast(raises: []).}:
        if not cancelFut.finished: cancelFut.complete())
  try:
    if ms > 0:
      await fut or cancelFut or sleepAsync(ms)
    else:
      await fut or cancelFut
    if fut.finished:
      return fut.read
    if cancel != nil and cancel.cancelled:
      raise newException(RequestCancelledError, "navi: request cancelled")
    raise newException(TimeoutError, "navi: request timed out after " & $ms & " ms")
  finally:
    if cancel != nil: cancel.disarmHook()
    if not cancelFut.finished: cancelFut.complete()

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
        qc: QuicConnAsync      ## the shared h3 connection (stays live for reuse)
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
    # Stream over HTTP/3 when the origin has advertised h3 (Alt-Svc). Mirrors the
    # buffered h3TransportAsync path: submit on the shared connection, read headers,
    # return a handle whose readChunk pulls the body incrementally. Any QUIC failure
    # falls through to h2/h1.
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
            client.h3conns.del(origin)     # drop a dead connection; fall back below

  if wantH2:
    if client.muxes.hasKey(origin) and client.muxes[origin].canReuse:
      let mux = client.muxes[origin]
      let sid = await mux.sendAndReadHeaders(h2HeaderList(req), req.body, req.bodyStream, h2TrailerList(req))
      return StreamResponse(kind: skH2, mux: mux, sid: sid,
        resp: toResponse(mux.respSnapshot(sid)), client: client, key: origin,
        decompress: decompress, cap: cap)
    if client.pendingMux.hasKey(origin):
      let mux = await client.pendingMux[origin]
      if mux != nil and mux.canReuse:
        let sid = await mux.sendAndReadHeaders(h2HeaderList(req), req.body, req.bodyStream, h2TrailerList(req))
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
        let sid = await mux.sendAndReadHeaders(h2HeaderList(rq), rq.body, rq.bodyStream, h2TrailerList(rq))
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
      # A failure after the branch already completed `pending` (h1 fallback via
      # `complete(nil)`, or a post-handshake error like a rejected client cert while
      # reading the response) must not complete the future twice (mirrors chronos).
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
    when defined(naviHttp3):                       # learn h3 from a streamed response
      client.recordAltSvc(rreq, handle.resp)        # too, so SSE/stream can upgrade
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
      # free the stream at EOF (a reset surfaces as an error). The mux stays live.
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
  disarm(sr.guard)                      # from here `drain` owns the teardown
  case sr.kind
  of skH2:
    try:
      await sr.mux.drainDownload(sr.sid, sink)
      sr.drained = true                 # drainDownload freed the stream
    except CatchableError:
      sr.drained = true                 # ...on error too, so the guard won't double-free
      raise
  of skH1:
    try:
      var keep = false
      h1DrainBody(sr.transport, sr.parser, sink, keep, sr.decompress, sr.cap)
      sr.drained = true
      if not (keep and pushIdle(sr.client.pool, sr.key, PooledConn[Conn](transport: sr.transport))):
        await close(sr.transport)
    except CatchableError:
      if not sr.drained: sr.drained = true
      await close(sr.transport)
      raise
  of skH3:
    when defined(naviHttp3):
      # Reuse readChunk's decode/cap/free per chunk; it sets `drained` at EOF.
      while true:
        let chunk = await sr.readChunk()
        if chunk.len == 0: break
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
  ## raise from `body` (which closes/resets the connection and propagates).
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
  ## non-200 or non `text/event-stream` response raises). Consume events with `next`
  ## (none at end) or `each` (a real loop, so break/return work). Reconnects
  ## transparently on a drop -- resending Last-Event-ID and honoring the server's
  ## retry: with backoff to `maxRetryMs` -- unless `reconnect` is false. `verb`/
  ## `body`/headers allow POST-SSE and auth. The underlying stream runs with the
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
  s.client.jar = client.jar          # share cookies with the caller
  await s.openConn()
  s.started = true
  return s

proc close*(s: SseStream): Future[void] {.async.} =
  ## Stop consuming and dispose the connection, including the dedicated internal
  ## client (its pool and h2 mux, whose reader is joined). Idempotent. Call it when
  ## done with the stream so the mux does not linger.
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
  while true:
    if s.closed: return none(SseEvent)   # also catches a close during a parked read
    let ev = s.parser.next()
    if ev.isSome:
      if s.parser.retryMs() >= 0:
        s.baseRetryMs = min(s.parser.retryMs(), s.maxRetryMs)
      return ev
    if s.handle == nil:
      if not s.reconnect: return none(SseEvent)
      await sleepAsync(min(s.retryMs, s.maxRetryMs))
      if s.closed: return none(SseEvent)    # closed during the backoff: do not reconnect
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
  ## loop (over the awaited `next`), so break/continue/return work:
  ##   let s = await api.sse(url)
  ##   s.each(ev): await handle(ev)
  while true:
    let evOpt = await s.next()
    if evOpt.isNone: break
    let ev = evOpt.get
    body

include navi/private/verbs

# --- WebSocket (RFC 6455) ---

export ws.WsMessage, ws.WsMessageKind, ws.closeNormal, ws.closeGoingAway,
       ws.closeMessageTooBig, ws.WsMessageTooLarge

type
  WsKind = enum wkH1, wkH2, wkH3
  WsTransport = object
    ## The duplex byte channel under a WebSocket: an h1 Upgrade connection, an h2
    ## Extended CONNECT tunnel stream (RFC 8441), or an h3 Extended CONNECT tunnel
    ## stream (RFC 9220). The frame codec is identical on top of any of them; only
    ## sendRaw/recvRaw/closeRaw differ.
    case kind: WsKind
    of wkH1: conn: Conn
    of wkH2:
      mux: H2Mux
      sid: uint32
    of wkH3:
      when defined(naviHttp3):     # the h3 (QUIC) transport type is opt-in
        qc: QuicConnAsync
        h3sid: int64
  WebSocket* = ref object
    tr: WsTransport
    dec: WsDecoder
    asmb: WsAssembler
    open: bool                ## the WS protocol is open (not yet closed/closing)
    closed: bool              ## the transport has been torn down (closeRaw ran); keeps
                              ## teardown idempotent (h2/h3 own a dedicated connection)
    maxMessageBytes: int      ## cap on a reassembled message; 0 = unlimited
    keepAlive: int            ## ms between keepalive pings while receiving; 0 = off
    pingOutstanding: bool      ## a keepalive ping is awaiting any inbound byte
    pendingRecv: Future[string]  ## the one in-flight read, kept across keepalive
                                 ## timeouts so a timed-out read is not orphaned

proc sendRaw(ws: WebSocket, data: string): Future[void] =
  ## Write raw bytes to the underlying transport (an encoded WS frame).
  case ws.tr.kind
  of wkH1: ws.tr.conn.sendAll(data)
  of wkH2: ws.tr.mux.tunnelSend(ws.tr.sid, data)
  of wkH3:
    when defined(naviHttp3): ws.tr.qc.tunnelSend(ws.tr.h3sid, data)
    else: raise newException(ValueError, "navi: h3 WebSocket without -d:naviHttp3")

proc recvRaw(ws: WebSocket): Future[string] =
  ## Read the next inbound chunk ("" on EOF / peer half-close).
  case ws.tr.kind
  of wkH1: ws.tr.conn.recvSome()
  of wkH2: ws.tr.mux.tunnelRecv(ws.tr.sid)
  of wkH3:
    when defined(naviHttp3): ws.tr.qc.tunnelRecv(ws.tr.h3sid)
    else: raise newException(ValueError, "navi: h3 WebSocket without -d:naviHttp3")

proc closeRaw(ws: WebSocket): Future[void] {.async.} =
  ## Tear down the transport exactly once (h2/h3: half-close the stream, then the
  ## dedicated connection). Idempotent, so a peer-close EOF, an explicit close, and
  ## the streaming error handlers can all call it without a double close of the
  ## dedicated mux/QUIC connection (which would leak or double-free).
  if ws.closed: return
  ws.closed = true
  case ws.tr.kind
  of wkH1: await ws.tr.conn.close()
  of wkH2:
    await ws.tr.mux.tunnelClose(ws.tr.sid)
    await ws.tr.mux.close()
  of wkH3:
    when defined(naviHttp3):
      await ws.tr.qc.tunnelClose(ws.tr.h3sid)
      await ws.tr.qc.closeConn()

proc toWsUrl(url: string): Url =
  var s = url
  if s.startsWith("ws://"): s = "http://" & s["ws://".len .. ^1]
  elif s.startsWith("wss://"): s = "https://" & s["wss://".len .. ^1]
  parseUrl(s)

proc websocketH1(client: Navi, u: Url, headers: Headers,
                 maxMessageBytes, keepAlive: int): Future[WebSocket] {.async.} =
  ## WebSocket over an HTTP/1.1 Upgrade (RFC 6455): the universal transport.
  let conn = await connect(u.host, u.port, u.isTls, client.config.tls,
                           resolveProxy(client.config, u), @[],
                           client.config.connectMs, client.config.readMs)
  let key = genKey()
  # Close the connection on any handshake failure (its close is async, so this
  # uses try/except rather than defer).
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
    result = WebSocket(tr: WsTransport(kind: wkH1, conn: conn), open: true,
                       maxMessageBytes: maxMessageBytes, keepAlive: keepAlive)
    if buf.len > headEnd:
      result.dec.feed(buf[headEnd .. ^1])
  except CatchableError:
    await conn.close()
    raise

proc websocketH2(client: Navi, u: Url, headers: Headers,
                 maxMessageBytes, keepAlive: int): Future[WebSocket] {.async.} =
  ## WebSocket over HTTP/2 Extended CONNECT (RFC 8441). Dials a dedicated h2
  ## connection (ALPN "h2"), opens a CONNECT stream with `:protocol=websocket`,
  ## and tunnels frames as DATA. Sec-WebSocket-Key/Accept are not used over h2.
  let conn = await connect(u.host, u.port, u.isTls, client.config.tls,
                           resolveProxy(client.config, u), @["h2", "http/1.1"],
                           client.config.connectMs, client.config.readMs)
  if conn.protocol != "h2":
    let got = if conn.protocol.len > 0: conn.protocol else: "http/1.1"
    await conn.close()
    raise newException(ProtocolError,
      "navi: WebSocket over h2 requested but the server negotiated " & got)
  let mux = await newH2Mux(conn, client.config.maxResponseBytes,
                           client.config.wantsDecompress)
  try:
    var reqHeaders = headers
    reqHeaders["sec-websocket-version"] = wsVersion
    let sid = await mux.openConnect(h2ConnectHeaderList(u, "websocket", reqHeaders))
    let status = mux.respSnapshot(sid).status
    if status != 200:            # RFC 8441: a 2xx (200) accepts the tunnel
      raise newException(IOError,
        "navi: WebSocket over h2 rejected with :status " & $status)
    result = WebSocket(tr: WsTransport(kind: wkH2, mux: mux, sid: sid), open: true,
                       maxMessageBytes: maxMessageBytes, keepAlive: keepAlive)
  except CatchableError:
    await mux.close()
    raise

when defined(naviHttp3):
  proc websocketH3(client: Navi, u: Url, headers: Headers,
                   maxMessageBytes, keepAlive: int): Future[WebSocket] {.async.} =
    ## WebSocket over HTTP/3 Extended CONNECT (RFC 9220). Dials a dedicated h3
    ## (QUIC) connection to the origin and opens a CONNECT `:protocol=websocket`
    ## stream, tunnelling frames as DATA. Sec-WebSocket-Key/Accept are not used.
    let qc = await openConnAsync(u.host, u.port, u.host, client.config.tls.caFile,
                                 client.config.tls.verify)
    try:
      let (sid, status) = await qc.openConnect(u.requestTarget,
                                               wsExtraFields(headers), "websocket")
      if status != 200:            # RFC 9220 / 8441: a 200 accepts the tunnel
        raise newException(IOError,
          "navi: WebSocket over h3 rejected with :status " & $status)
      result = WebSocket(tr: WsTransport(kind: wkH3, qc: qc, h3sid: sid), open: true,
                         maxMessageBytes: maxMessageBytes, keepAlive: keepAlive)
    except CatchableError:
      await qc.closeConn()
      raise

proc websocket*(client: Navi, url: string,
                headers = initHeaders(),
                maxMessageBytes = 0, keepAlive = 0): Future[WebSocket] {.async.} =
  ## Open a WebSocket connection. Accepts `ws://` / `wss://` (or http/https);
  ## `wss` uses TLS. The transport follows `config.http`: h1 Upgrade (RFC 6455) is
  ## used whenever H1 is allowed (the universal path); to tunnel over Extended
  ## CONNECT instead, exclude H1 -- `config.http = {H2}` for h2 (RFC 8441) or
  ## `{H3}` for h3 (RFC 9220, needs `-d:naviHttp3`). Use `send`, `receive`, `close`.
  ##
  ## `maxMessageBytes` (0 = unlimited) caps a reassembled message: past it `receive`
  ## closes with 1009 and raises `WsMessageTooLarge`. Set it for untrusted servers,
  ## since a peer can otherwise grow one message without bound via continuation frames.
  ##
  ## `keepAlive` (ms, 0 = off) sends a ping after that long with no data *while a
  ## `receive` is in progress*, and raises `TimeoutError` (closing the connection) if
  ## another interval passes with still nothing back -- so a dead peer is detected
  ## instead of awaiting forever.
  let u = toWsUrl(url)
  let httpset = client.config.http
  if httpset.card == 0 or H1 in httpset:             # h1 is the universal ws transport
    return await client.websocketH1(u, headers, maxMessageBytes, keepAlive)
  elif H2 in httpset and u.isTls:                    # opt-in h2 (RFC 8441) by excluding H1
    return await client.websocketH2(u, headers, maxMessageBytes, keepAlive)
  elif H3 in httpset and u.isTls:                    # opt-in h3 (RFC 9220): config.http = {H3}
    when defined(naviHttp3):
      return await client.websocketH3(u, headers, maxMessageBytes, keepAlive)
    else:
      raise newException(ProtocolError,
        "navi: WebSocket over h3 requires a -d:naviHttp3 build")
  else:
    raise newException(ProtocolError,
      "navi: config.http " & $httpset & " permits no usable WebSocket transport " &
      "(h2/h3 need TLS)")

proc send*(ws: WebSocket, data: string, binary = false): Future[void] {.async.} =
  ## Send a text (default) or binary message. Client frames are masked.
  await ws.sendRaw(encodeFrame(if binary: opBinary else: opText, data))

proc ping*(ws: WebSocket, data = ""): Future[void] {.async.} =
  await ws.sendRaw(encodeFrame(opPing, data))

proc kaRecv(ws: WebSocket): Future[string] {.async.} =
  ## One read chunk. With keepalive off, a plain read. With it on, a single
  ## outstanding read is kept in `pendingRecv` (so a timed-out read is never
  ## orphaned to steal the next bytes): on an idle interval send a ping, and on a
  ## second idle interval with a ping still unanswered, declare the peer dead.
  if ws.keepAlive <= 0: return await ws.recvRaw()
  while true:
    if ws.pendingRecv == nil: ws.pendingRecv = ws.recvRaw()
    if await withTimeout(ws.pendingRecv, ws.keepAlive):
      let chunk = ws.pendingRecv.read()     # completed (re-raises a read error)
      ws.pendingRecv = nil
      ws.pingOutstanding = false            # any inbound byte proves liveness
      return chunk
    if ws.pingOutstanding:                   # pinged last interval, still nothing back
      ws.open = false
      try: await ws.closeRaw() except CatchableError: discard
      raise newException(TimeoutError, "navi: websocket keepalive timed out")
    await ws.sendRaw(encodeFrame(opPing, ""))
    ws.pingOutstanding = true

proc receive*(ws: WebSocket): Future[WsMessage] {.async.} =
  ## Await a full message, answering pings and reassembling fragments. A close
  ## returns `wmClose` (and the connection is then closed).
  while true:
    var f: Frame
    while not ws.dec.next(f):
      let chunk = await ws.kaRecv()
      if chunk.len == 0:                        # peer closed: tear the transport down
        ws.open = false                        # now (h2/h3: close the dedicated conn)
        await ws.closeRaw()                    # so it is not leaked when close() no-ops
        return WsMessage(kind: wmClose, closeCode: closeGoingAway)
      ws.dec.feed(chunk)
    var o: WsOutcome
    try:
      o = ws.asmb.offer(f, ws.maxMessageBytes)
    except WsMessageTooLarge:
      if ws.open:      # tell the peer why (1009), then drop the connection
        try: await ws.sendRaw(encodeFrame(opClose, closePayload(closeMessageTooBig)))
        except CatchableError: discard
        ws.open = false
        await ws.closeRaw()
      raise
    case o.reply
    of wrPong:
      await ws.sendRaw(encodeFrame(opPong, o.replyPayload))
    of wrCloseEcho:
      if ws.open:
        try: await ws.sendRaw(encodeFrame(opClose, o.replyPayload))
        except CatchableError: discard
        ws.open = false
        await ws.closeRaw()
    of wrNone: discard
    if o.ready: return o.message

proc close*(ws: WebSocket, code = closeNormal, reason = ""): Future[void] {.async.} =
  ## Send a close frame (if still open) and tear the transport down. Idempotent: safe
  ## after a peer-close, which already set open=false and tore down -- the transport
  ## teardown still runs (once) so an already-closed h2/h3 connection is not leaked.
  if ws.open:
    ws.open = false
    try: await ws.sendRaw(encodeFrame(opClose, closePayload(code, reason)))
    except CatchableError: discard
  await ws.closeRaw()

# --- WebSocket streaming (a large message, one frame at a time) ---

type
  WsReader* = ref object
    ## A message being received incrementally. Consume with `each`/`readChunk`.
    ws: WebSocket
    kind*: WsMessageKind
    first: string
    hasFirst: bool
    done: bool
  WsWriter* = ref object
    ## A message being sent incrementally; `write` each fragment.
    ws: WebSocket
    binary: bool
    started: bool

proc readDataFrame(ws: WebSocket): Future[Frame] {.async.} =
  ## Next non-control frame (data/continuation/close), answering pings; keepalive
  ## applies via `kaRecv`. A transport EOF yields a close frame.
  while true:
    var f: Frame
    while not ws.dec.next(f):
      let chunk = await ws.kaRecv()
      if chunk.len == 0:
        ws.open = false
        await ws.closeRaw()                    # tear down on EOF (see receive)
        return Frame(fin: true, opcode: opClose, payload: "")
      ws.dec.feed(chunk)
    case f.opcode
    of opPing: await ws.sendRaw(encodeFrame(opPong, f.payload)); continue
    of opPong: continue
    else: return f

proc closeOnFrame(ws: WebSocket, f: Frame): Future[void] {.async.} =
  if ws.open:
    try: await ws.sendRaw(encodeFrame(opClose, f.payload))
    except CatchableError: discard
    ws.open = false
    await ws.closeRaw()

proc openStreamReader(ws: WebSocket): Future[WsReader] {.async.} =
  result = WsReader(ws: ws)
  let f = await ws.readDataFrame()
  case f.opcode
  of opText, opBinary:
    result.kind = if f.opcode == opText: wmText else: wmBinary
    result.first = f.payload
    result.hasFirst = true
    result.done = f.fin
  of opClose:
    result.kind = wmClose
    result.done = true
    await ws.closeOnFrame(f)
  else:
    raise newException(IOError, "navi: WebSocket message started with a continuation frame")

proc readChunk*(r: WsReader): Future[string] {.async.} =
  ## The next chunk of the streamed message (one frame's payload), or "" at its end.
  if r.hasFirst:
    r.hasFirst = false
    return r.first
  if r.done: return ""
  let f = await r.ws.readDataFrame()
  case f.opcode
  of opContinuation:
    r.done = f.fin
    return f.payload
  of opClose:
    r.done = true
    await r.ws.closeOnFrame(f)
    return ""
  else:
    raise newException(IOError, "navi: expected a continuation frame mid-message")

proc drain*(r: WsReader, sink: BodySink): Future[void] {.async.} =
  ## Deliver the message's chunks to `sink`; on a sink error close the connection (a
  ## half-read message cannot be resumed) and re-raise. Prefer the `each` template.
  try:
    while true:
      let chunk = await r.readChunk()
      if chunk.len == 0: break
      await sink(chunk)
  except CatchableError:
    r.ws.open = false
    try: await r.ws.closeRaw()
    except CatchableError: discard
    raise

template each*(r: WsReader; chunk, body: untyped): untyped =
  ## Run `body` for each chunk of the streamed message. `body` runs as a proc, so
  ## `break`/`continue`/`return` cannot escape it; raise to stop early. The `await`
  ## is baked in, so there is none on the `each` line.
  await r.drain(proc(chunk: string): Future[void] {.async.} = body)

template stream*(ws: WebSocket): untyped =
  ## Begin receiving the next message as a stream of chunks (one per frame). `kind`
  ## is set from the first frame. Consume with `each`/`readChunk`. `maxMessageBytes`
  ## does not apply -- you bound your own sink. Returns a `Future[WsReader]`, so:
  ##   let reader = await ws.stream()
  openStreamReader(ws)

proc write*(w: WsWriter, data: string): Future[void] {.async.} =
  ## Append a fragment to the message being streamed out.
  if not w.started:
    await w.ws.sendRaw(encodeFrame(if w.binary: opBinary else: opText, data, fin = false))
    w.started = true
  else:
    await w.ws.sendRaw(encodeFrame(opContinuation, data, fin = false))

proc finishWrite(w: WsWriter): Future[void] {.async.} =
  if not w.started:
    await w.ws.sendRaw(encodeFrame(if w.binary: opBinary else: opText, "", fin = true))
  else:
    await w.ws.sendRaw(encodeFrame(opContinuation, "", fin = true))

template streamOut(ws: WebSocket; writer: untyped; isBinary: bool; body: untyped) =
  block:
    var writer {.inject.} = WsWriter(ws: ws, binary: isBinary)
    try:
      body
      await writer.finishWrite()
    except CatchableError:
      ws.open = false
      try: await ws.closeRaw()
      except CatchableError: discard
      raise

template stream*(ws: WebSocket; writer, body: untyped): untyped =
  ## Send the next message as a stream of text fragments: `await writer.write(chunk)`
  ## inside the block; the final (fin) frame is sent on block exit. On an exception
  ## the partial message cannot be completed, so the connection is closed.
  streamOut(ws, writer, false, body)

template streamBinary*(ws: WebSocket; writer, body: untyped): untyped =
  ## Like `stream(writer)`, but the message is binary.
  streamOut(ws, writer, true, body)
