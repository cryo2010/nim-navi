## navi — asyncdispatch entry point.
##
##   import navi/asyncdispatch
##   let api = newNavi()
##   let res = await api.get("http://example.com")

import std/tables
import navi/private/entryguard
import navi/core/public
import navi/core/[engine, pool, session, proxy, h2glue]
import navi/backend/[asyncdispatch, h2mux]

claimEntry("navi/asyncdispatch")
export public, asyncdispatch

type
  Hook* = proc(ctx: HookCtx): Future[void] {.closure.}
    ## Lifecycle callback; may be async. Mutate `ctx.request` (beforeRequest),
    ## read/mutate `ctx.response` (afterResponse), or read `ctx.attempt`.
  Hooks* = object
    beforeRequest*: seq[Hook]
    afterResponse*: seq[Hook]
    beforeRetry*: seq[Hook]

  NaviOptions* = object of NaviOptionsBase
    hooks*: Hooks   ## lifecycle callbacks (may be async)

  Navi* = object
    options*: NaviOptions
    pool*: Pool[PooledConn[Conn]]
    jar*: CookieJar
    muxes: TableRef[string, H2Mux]              ## live shared h2 connections
    pendingMux: TableRef[string, Future[H2Mux]] ## in-flight connects (coalescing)

proc defaultOptions*(): NaviOptions =
  result.http = {H1, H2}
  result.tls = defaultTls()

proc mergeHooks(base, add: Hooks): Hooks =
  Hooks(beforeRequest: base.beforeRequest & add.beforeRequest,
        afterResponse: base.afterResponse & add.afterResponse,
        beforeRetry: base.beforeRetry & add.beforeRetry)

proc runHook(hook: Hook, ctx: HookCtx): Future[void] =
  # Calling the hook returns its Future without raising (errors ride the Future
  # and surface on `await`); cast away gcsafe/raises so the shared engine's
  # `await runHook(...)` stays within chronos's strict effect tracking.
  {.cast(gcsafe).}:
    {.cast(raises: []).}:
      result = hook(ctx)

proc newNavi*(options = defaultOptions()): Navi =
  Navi(options: options,
       pool: newPool[PooledConn[Conn]](), jar: newCookieJar(),
       muxes: newTable[string, H2Mux](),
       pendingMux: newTable[string, Future[H2Mux]]())

proc extend*(client: Navi, options: NaviOptions): Navi =
  var merged = mergeBase(client.options, options)
  merged.hooks = mergeHooks(client.options.hooks, options.hooks)
  Navi(options: merged,
       pool: newPool[PooledConn[Conn]](), jar: newCookieJar(),
       muxes: newTable[string, H2Mux](),
       pendingMux: newTable[string, Future[H2Mux]]())

proc muxRequest(client: Navi, mux: H2Mux, req: Request,
                sink: BodySink): Future[Response] {.async.} =
  var r = toResponse(await mux.request(h2HeaderList(req), req.body))
  applySink(r, sink)
  result = r

proc h1OnConn(client: Navi, conn: Conn, origin: string, req: Request,
              sink: BodySink): Future[Response] {.async.} =
  var keep = false
  result = h1Exchange(conn, req, sink, keep)
  let pc = PooledConn[Conn](transport: conn)
  if not (keep and pushIdle(client.pool, origin, pc)):
    await close(conn)

proc transport(client: Navi, req: Request, sink: BodySink): Future[Response] {.async.} =
  ## Multiplex over a shared h2 connection when available/negotiable; otherwise
  ## pool http/1.1. Concurrent connects to the same new origin are coalesced so a
  ## cold burst still ends up on one h2 connection.
  let origin = originKey(req.url)
  let wantH2 = client.options.wantsH2 and req.url.isTls

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
      result = h1Exchange(pc.transport, req, sink, keep)
      if not (keep and pushIdle(client.pool, origin, pc)): await close(pc.transport)
      return
    except CatchableError:
      await close(pc.transport)  # stale; fall through

  # 3. Open a fresh connection.
  var rq = req
  let proxyTarget = resolveProxy(client.options, rq.url)
  rq.absoluteForm = proxyTarget.isSet and not rq.url.isTls
  let alpn = if wantH2: @["h2", "http/1.1"] else: @[]

  if wantH2:
    let pending = newFuture[H2Mux]("navi.pendingMux")
    client.pendingMux[origin] = pending
    try:
      let conn = await connect(rq.url.host, rq.url.port, rq.url.isTls,
                               client.options.tls, proxyTarget, alpn)
      if conn.protocol == "h2":
        let mux = await newH2Mux(conn)
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
                           client.options.tls, proxyTarget, alpn)
  result = await client.h1OnConn(conn, origin, rq, sink)

proc doRequest(client: Navi, req: Request): Future[Response] {.async.} =
  result = performRequest(client, req)

proc doStream(client: Navi, req: Request, sink: BodySink): Future[Response] {.async.} =
  result = performStream(client, req, sink)

proc withDeadline(client: Navi, fut: Future[Response]): Future[Response] {.async.} =
  ## Bound the whole request (all attempts) by `timeout`. On expiry, raise
  ## TimeoutError; the abandoned future runs to completion in the background.
  let ms = client.options.timeoutMs
  if ms <= 0:
    return await fut
  if await withTimeout(fut, ms):
    return fut.read
  raise newException(TimeoutError, "navi: request timed out after " & $ms & " ms")

proc request*(client: Navi, verb: HttpVerb, target: string,
              headers = initHeaders(), body = "", json: JsonNode = nil,
              form: seq[(string, string)] = @[], multipart: Multipart = @[],
              bodyStream: BodyProducer = nil): Future[Response] {.async.} =
  let req = buildRequest(client.options, verb, target, headers, body, json,
                         form, multipart, bodyStream)
  result = await client.withDeadline(doRequest(client, req))

proc stream*(client: Navi, verb: HttpVerb, target: string, sink: BodySink,
             headers = initHeaders()): Future[Response] {.async.} =
  ## Deliver the response body to `sink` as it arrives; Response.body is empty.
  let req = buildRequest(client.options, verb, target, headers)
  result = await client.withDeadline(doStream(client, req, sink))

include navi/private/verbs
