# The native chronos implementation, `include`d by navi/chronos.nim on non-js
# targets. Kept separate so the entry can fall back to navi/js under `nim js`
# without pulling in the chronos package (which has no JavaScript backend).

import navi/private/[entryguard, streamguard]
import navi/core/public
import navi/core/[engine, pool, session, proxy, redirect, cookies, digest, cancel]
import navi/core/decompress   # StreamDecoder, for the readChunk decode state
import navi/proto/h1
import navi/proto/ws
import navi/backend/chronos
from std/strutils import startsWith, find, splitLines, contains

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
    config: NaviConfig
    pool*: Pool[PooledConn[Conn]]
    jar*: CookieJar

proc initNaviConfig*(): NaviConfig =
  ## The only way to build a config (`NaviConfig` requires every field). Sets the
  ## safe defaults; override the fields you want, then pass it to `newNavi`.
  NaviConfig(
    prefixUrl: "", headers: initHeaders(), http: {H1, H2}, tls: defaultTls(),
    decompress: true, throwHttpErrors: true, maxRedirects: 20,
    retry: defaultRetryPolicy(), maxResponseBytes: 0,
    auth: Auth(), proxy: "", timeout: 0, timeouts: Timeouts(), middleware: @[])

proc newNavi*(config = initNaviConfig()): Navi =
  var cfg = config
  if cfg.tls.sessionCache.isNil: cfg.tls.sessionCache = newTlsStore(cfg.tls)
  Navi(config: cfg, pool: newPool[PooledConn[Conn]](), jar: newCookieJar())

proc config*(client: Navi): lent NaviConfig = client.config
  ## Read-only view of the client's config. Config is fixed at construction;
  ## build a fresh client (or `extend`) to change it rather than mutating a live
  ## one, so its pooled connections stay consistent with its settings.

proc extend*(client: Navi, config: NaviConfig): Navi =
  var merged = mergeBase(client.config, config)
  merged.middleware = client.config.middleware & config.middleware
  merged.tls.sessionCache = newTlsStore(merged.tls)  # its own cache, not the parent's
  Navi(config: merged, pool: newPool[PooledConn[Conn]](), jar: newCookieJar())

proc close*(client: Navi): Future[void] {.async.} =
  ## Close all idle pooled connections. Optional but recommended when done with
  ## the client (a later request opens fresh connections).
  for pc in client.pool.drain():
    await close(pc.transport)
  closeTlsStore(client.config.tls.sessionCache)

proc transport(client: Navi, req: Request, sink: BodySink): Future[Response] {.async.} =
  ## Pool-based transport (http/1.1; chronos has no h2).
  result = poolTransport(client, req, sink)

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
  StreamResponseObj = object
    ## The response of a streaming request: status/headers are available
    ## immediately while the body is drained on demand. Holds the checked-out
    ## http/1.1 connection (removed from the pool) until `drain` returns it or
    ## `close` disposes it. chronos is http/1.1 only (BearSSL has no client ALPN),
    ## so there is no h2 variant.
    resp: Response             ## header snapshot (status/headers; empty body)
    client: Navi
    key: string                ## origin key, for returning the connection to the pool
    transport: Conn            ## the checked-out http/1.1 connection
    parser: H1Parser
    decompress: bool
    cap: int
    cancel: CancelToken
    drained: bool              ## body fully read; connection returned/closed
    closed: bool               ## disposed without draining
    guard: StreamGuard         ## closes the connection if the handle is dropped
                               ## before drain/close (see navi/private/streamguard)
    dec: StreamDecoder         ## decode + size-cap state carried across readChunk
    decReady: bool             ## calls (chosen once the response headers are in)
    seen: int
  StreamResponse* = ref StreamResponseObj

proc close*(sr: StreamResponse): Future[void] {.async.} =
  ## Dispose a streaming handle whose body will not be fully drained: closes the
  ## connection (a partially-read response cannot be pooled). Idempotent; a no-op
  ## once the body has been drained.
  if sr.drained or sr.closed: return
  sr.closed = true
  disarm(sr.guard)                    # we do the awaitable teardown ourselves
  await close(sr.transport)

proc status*(sr: StreamResponse): int = sr.resp.status
  ## The response status code, available before the body is drained.
proc reason*(sr: StreamResponse): string = sr.resp.reason
proc httpVersion*(sr: StreamResponse): string = sr.resp.httpVersion
proc headers*(sr: StreamResponse): Headers = sr.resp.headers
proc ok*(sr: StreamResponse): bool = sr.resp.ok
  ## Whether the status is 2xx (checked by the caller; the pull API never throws).

proc openStreamConn(client: Navi, req: Request): Future[StreamResponse] {.async.} =
  ## Send one request and read its headers, returning a handle with the body
  ## pending over a pooled http/1.1 connection. A stale pooled connection is
  ## retried once on a fresh one. Does not throw on non-2xx.
  let origin = originKey(req.url)
  let decompress = client.config.wantsDecompress
  let cap = client.config.maxResponseBytes

  var (found, pc) = popIdle(client.pool, origin)
  if found:
    try:
      let parser = h1SendAndReadHeaders(pc.transport, req, true)
      return StreamResponse(transport: pc.transport, parser: parser,
        resp: parser.toResponse(), client: client, key: origin,
        decompress: decompress, cap: cap)
    except CatchableError:
      await close(pc.transport)     # pooled connection was stale; open a fresh one

  var rq = req
  let proxyTarget = resolveProxy(client.config, rq.url)
  rq.absoluteForm = proxyTarget.isSet and not rq.url.isTls
  let conn = await connect(rq.url.host, rq.url.port, rq.url.isTls,
                           client.config.tls, proxyTarget, @[],
                           client.config.connectMs, client.config.readMs)
  let parser = h1SendAndReadHeaders(conn, rq, true)
  return StreamResponse(transport: conn, parser: parser,
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
    # Arm the leak-guard for the synchronous fallback teardown if the handle is
    # dropped without drain/close. Captures only `transport` (not `handle`, cycle).
    let t = handle.transport
    handle.guard = newStreamGuard(proc() {.gcsafe, raises: [].} =
      {.cast(gcsafe).}:
        try: t.closeSync()
        except Exception: discard)     # best-effort finalizer: never propagate
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
  ## Pull the next decoded body chunk, or "" once the body is fully read. Like
  ## `drain`, the terminal returns the connection to the pool or closes it and
  ## disarms the guard; a cap breach closes and reraises. Call until it returns "".
  ## The guard stays armed across the incremental reads, so a handle dropped before
  ## EOF is still closed by it.
  if sr.drained or sr.closed: return ""
  try:
    result = h1ReadChunk(sr.transport, sr.parser,
                         sr.dec, sr.decReady, sr.seen, sr.decompress, sr.cap)
    if result.len == 0:                        # end of body: we own the teardown now
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

proc drain*(sr: StreamResponse, sink: BodySink): Future[void] {.async.} =
  ## Deliver the response body to `sink` as it arrives (decoded and size-capped),
  ## awaiting it per chunk so a slow sink backpressures the peer. Then return the
  ## connection to the pool (or close it if it cannot be reused). Consumes the
  ## handle: call once. On error the connection is closed and the error re-raised.
  ## Prefer the `each` template for the common case.
  if sr.drained or sr.closed:
    raise newException(IOError, "navi: stream already drained or closed")
  throwIfCancelled(sr.cancel)
  disarm(sr.guard)                         # from here `drain` owns the teardown
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
