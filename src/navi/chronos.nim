## navi — chronos entry point.
##
##   import navi/chronos
##   let api = newNavi()
##   let res = await api.get("http://example.com")
##
## Requires the `chronos` package. Only compiled when this module is imported,
## so sync/asyncdispatch users never pull chronos in.

import navi/private/entryguard
import navi/core/public
import navi/core/[engine, pool, session]
import navi/backend/chronos

claimEntry("navi/chronos")
export public, chronos

type
  Hook* = proc(ctx: HookCtx): Future[void] {.closure.}
    ## Lifecycle callback; may be async. Mutate `ctx.request` (beforeRequest),
    ## read/mutate `ctx.response` (afterResponse), or read `ctx.attempt`.
  Hooks* = object
    beforeRequest*: seq[Hook]
    afterResponse*: seq[Hook]
    beforeRetry*: seq[Hook]

  Navi* = object
    options*: NaviOptions
    hooks*: Hooks
    pool*: Pool[PooledConn[Conn]]
    jar*: CookieJar

proc mergeHooks(base, add: Hooks): Hooks =
  Hooks(beforeRequest: base.beforeRequest & add.beforeRequest,
        afterResponse: base.afterResponse & add.afterResponse,
        beforeRetry: base.beforeRetry & add.beforeRetry)

proc runHook(hook: Hook, ctx: HookCtx): Future[void] =
  # See the asyncdispatch entry: the call returns a Future without raising.
  {.cast(gcsafe).}:
    {.cast(raises: []).}:
      result = hook(ctx)

proc newNavi*(options = defaultOptions(), hooks = Hooks()): Navi =
  Navi(options: options, hooks: hooks,
       pool: newPool[PooledConn[Conn]](), jar: newCookieJar())

proc extend*(client: Navi, options: NaviOptions, hooks = Hooks()): Navi =
  Navi(options: mergeOptions(client.options, options),
       hooks: mergeHooks(client.hooks, hooks),
       pool: newPool[PooledConn[Conn]](), jar: newCookieJar())

proc transport(client: Navi, req: Request, sink: BodySink): Future[Response] {.async.} =
  ## Pool-based transport (http/1.1; chronos has no h2).
  result = poolTransport(client, req, sink)

proc request*(client: Navi, verb: HttpVerb, target: string,
              headers = initHeaders(), body = "", json: JsonNode = nil,
              form: seq[(string, string)] = @[],
              bodyStream: BodyProducer = nil): Future[Response] {.async.} =
  let req = buildRequest(client.options, verb, target, headers, body, json,
                         form, bodyStream)
  result = performRequest(client, req)

proc stream*(client: Navi, verb: HttpVerb, target: string, sink: BodySink,
             headers = initHeaders()): Future[Response] {.async.} =
  ## Deliver the response body to `sink` as it arrives; Response.body is empty.
  let req = buildRequest(client.options, verb, target, headers)
  result = performStream(client, req, sink)

include navi/private/verbs
