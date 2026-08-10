# The native asyncdispatch implementation, `include`d by navi/asyncdispatch.nim
# on non-js targets. Kept separate so the entry can fall back to navi/js under
# `nim js` without pulling in std/asyncdispatch (which has no JS backend).

import std/tables
import navi/private/[entryguard, streamguard]
import navi/core/public
import navi/core/[engine, pool, session, proxy, h2glue]
import navi/core/[redirect, cookies, digest, cancel]
import navi/proto/h1
import navi/proto/h2/conn
import navi/proto/ws
import navi/backend/[asyncdispatch, h2mux]
from std/strutils import startsWith, find, splitLines, contains

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
    config: NaviConfig
    pool*: Pool[PooledConn[Conn]]
    jar*: CookieJar
    muxes: TableRef[string, H2Mux]              ## live shared h2 connections
    pendingMux: TableRef[string, Future[H2Mux]] ## in-flight connects (coalescing)

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
  Navi(config: cfg,
       pool: newPool[PooledConn[Conn]](), jar: newCookieJar(),
       muxes: newTable[string, H2Mux](),
       pendingMux: newTable[string, Future[H2Mux]]())

proc config*(client: Navi): lent NaviConfig = client.config
  ## Read-only view of the client's config. Config is fixed at construction;
  ## build a fresh client (or `extend`) to change it rather than mutating a live
  ## one, so its pooled connections stay consistent with its settings.

proc extend*(client: Navi, config: NaviConfig): Navi =
  var merged = mergeBase(client.config, config)
  merged.middleware = client.config.middleware & config.middleware
  merged.tls.sessionCache = newTlsStore(merged.tls)  # its own cache, not the parent's
  Navi(config: merged,
       pool: newPool[PooledConn[Conn]](), jar: newCookieJar(),
       muxes: newTable[string, H2Mux](),
       pendingMux: newTable[string, Future[H2Mux]]())

proc close*(client: Navi): Future[void] {.async.} =
  ## Close all pooled connections and shared h2 connections, freeing their TLS
  ## contexts. Any in-flight request on a shared connection fails with IOError.
  ## Optional but recommended when done with the client.
  for pc in client.pool.drain():
    await close(pc.transport)
  for mux in client.muxes.values:
    await mux.close()
  client.muxes.clear()
  closeTlsStore(client.config.tls.sessionCache)

proc muxRequest(client: Navi, mux: H2Mux, req: Request,
                sink: BodySink): Future[Response] {.async.} =
  # The mux delivers a streaming request's body to `sink` incrementally (decoding
  # content-encoding as it arrives), so the returned response's body is empty.
  # A non-streaming request (sink == nil) still buffers into r.body as before.
  result = toResponse(await mux.request(h2HeaderList(req), req.body, req.bodyStream, sink))

proc h1OnConn(client: Navi, conn: Conn, origin: string, req: Request,
              sink: BodySink): Future[Response] {.async.} =
  var keep = false
  result = h1Exchange(conn, req, sink, keep,
                      client.config.wantsDecompress, client.config.maxResponseBytes)
  let pc = PooledConn[Conn](transport: conn)
  if not (keep and pushIdle(client.pool, origin, pc)):
    await close(conn)

proc transport(client: Navi, req: Request, sink: BodySink): Future[Response] {.async.} =
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
  var (found, pc) = popIdle(client.pool, origin)
  if found:
    try:
      var keep = false
      result = h1Exchange(pc.transport, req, sink, keep,
                          client.config.wantsDecompress, client.config.maxResponseBytes)
      if not (keep and pushIdle(client.pool, origin, pc)): await close(pc.transport)
      return
    except CatchableError:
      await close(pc.transport)  # stale; fall through

  # 3. Open a fresh connection.
  var rq = req
  let proxyTarget = resolveProxy(client.config, rq.url)
  rq.absoluteForm = proxyTarget.isSet and not rq.url.isTls
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
      pending.fail(e)
      raise

  let conn = await connect(rq.url.host, rq.url.port, rq.url.isTls,
                           client.config.tls, proxyTarget, alpn,
                           client.config.connectMs, client.config.readMs)
  result = await client.h1OnConn(conn, origin, rq, sink)

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
              cancel: CancelToken = nil): Future[Response] {.async.} =
  ## Perform a request; configured middleware wraps the whole call. `params` are
  ## appended to the URL query; `cancel` aborts the in-flight request.
  let req = buildRequest(client.config, verb, target, headers, body, json,
                         form, multipart, bodyStream, params)
  if client.config.middleware.len == 0:
    return await client.guard(doRequest(client, req), cancel)
  let ctx = NaviContext(req: req, clientv: client)
  return await client.guard(runChain(ctx), cancel)

# --- Streaming downloads (pull-based handle) ---

type
  StreamKind = enum skH1, skH2
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
    case kind: StreamKind
    of skH1:
      transport: Conn          ## the checked-out http/1.1 connection
      parser: H1Parser
    of skH2:
      mux: H2Mux               ## the shared connection (stays live for reuse)
      sid: uint32              ## our stream on it
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
  rq.absoluteForm = proxyTarget.isSet and not rq.url.isTls
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
      pending.fail(e)
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
      discard await sr.mux.drainDownload(sr.sid, sink)
      sr.drained = true                 # drainDownload's finally freed the stream
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

proc websocket*(client: Navi, url: string,
                headers = initHeaders()): Future[WebSocket] {.async.} =
  ## Open a WebSocket connection (RFC 6455). Accepts `ws://` / `wss://` (or
  ## http/https); `wss` uses TLS. Does the HTTP/1.1 Upgrade over the transport and
  ## validates `Sec-WebSocket-Accept`. Use `send`, `receive`, and `close`.
  let u = toWsUrl(url)
  let conn = await connect(u.host, u.port, u.isTls, client.config.tls,
                           resolveProxy(client.config, u), @[],
                           client.config.connectMs)
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
    result = WebSocket(conn: conn, open: true)
    if buf.len > headEnd:
      result.dec.feed(buf[headEnd .. ^1])
  except CatchableError:
    await conn.close()
    raise

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
