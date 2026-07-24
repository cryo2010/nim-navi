## navi — JavaScript (fetch) entry point.
##
##   import navi/js
##
##   proc main() {.async.} =
##     let api = newNavi()
##     let res = await api.get("https://example.com")
##     echo res.status, " ", res.data
##   discard main()
##
## Runs only on the JavaScript backend (`nim js`). `fetch` handles TLS, HTTP
## version negotiation, redirects, and body decoding; navi layers on request
## building, hooks, retries, and throw-on-non-2xx. There is no connection pool
## (the runtime owns connections). Cookies persist automatically: navi keeps a
## jar wherever there is no browser cookie store (Node, Deno, Bun, Workers), and
## leaves it to the browser otherwise. Nothing to configure.

when not defined(js):
  {.error: "navi/js requires the JavaScript backend; compile with `nim js` " &
           "(import `navi` for the native sync client).".}

import std/asyncjs
from std/strutils import startsWith
import navi/private/entryguard
import navi/core/public
import navi/core/retry
import navi/backend/js
import navi/backend/jsws

claimEntry("navi/js")
export public, asyncjs
export jsws.WebSocket, jsws.WsMessage, jsws.WsMessageKind,
       jsws.send, jsws.receive, jsws.close, jsws.closeNormal, jsws.closeGoingAway

type
  NaviContext* = ref object
    ## Carried through the middleware chain. A middleware reads and mutates it,
    ## then `await ctx.next()` runs the rest of the chain (which fills `res`).
    req*: Request            ## the outgoing request; modify it before `next`
    res*: Response           ## the response; set by `next`, adjust it after
    clientp: ptr Navi        ## the owning client (see `client`); valid for the call
    sink: BodySink           ## non-nil for a streaming request
    cancel: CancelToken      ## caller's cancellation token, or nil
    idx: int                 ## index of the next middleware to run
  NaviMiddleware* = proc(ctx: NaviContext): Future[void] {.closure.}
    ## A middleware step; may be async. A closure, so it can capture: read/modify
    ## `ctx.req`, `await ctx.next()` to proceed -- or skip it to short-circuit --
    ## then read/modify `ctx.res`. Write a factory `proc bearer(token): NaviMiddleware`
    ## to close over per-instance config.

  NaviConfig* {.requiresInit.} = object of NaviConfigBase
    ## `requiresInit`: build it with `newNaviConfig()`, not a bare `NaviConfig(...)`.
    middleware*: seq[NaviMiddleware]

  Navi* = object
    config: NaviConfig   ## the runtime owns connections
    jar: CookieJar          ## kept off-browser; nil in a browser (its store owns cookies)

proc newNaviConfig*(): NaviConfig =
  ## The only way to build a config (`NaviConfig` requires every field). Safe
  ## defaults, minus decompression: the browser decodes bodies and forbids the
  ## Accept-Encoding request header, so navi does not add it. TLS and HTTP version
  ## negotiation are the runtime's (so `http` is unused here).
  NaviConfig(
    prefixUrl: "", headers: initHeaders(), http: {}, tls: defaultTls(),
    decompress: false, throwHttpErrors: true, maxRedirects: 20,
    retry: defaultRetryPolicy(), maxResponseBytes: 0,
    auth: Auth(), proxy: "", timeout: 0, middleware: @[])

# A browser owns the cookie store (and hides Set-Cookie from fetch); Node, Deno,
# Bun, and Workers do not, so navi keeps the jar there. `document` exists only in
# a browser document context.
proc inBrowser(): bool {.importjs: "(typeof document !== 'undefined')".}

proc newNavi*(config = newNaviConfig()): Navi =
  result = Navi(config: config)
  if not inBrowser(): result.jar = newCookieJar()

proc config*(client: Navi): lent NaviConfig = client.config
  ## Read-only view of the client's config. Config is fixed at construction;
  ## build a fresh client (or `extend`) to change it rather than mutating a live
  ## one. The runtime owns connections, so there is nothing to reconcile, but the
  ## contract matches the native backends.

proc extend*(client: Navi, config: NaviConfig): Navi =
  var merged = mergeBase(client.config, config)
  merged.middleware = client.config.middleware & config.middleware
  result = Navi(config: merged)
  if not inBrowser(): result.jar = newCookieJar()

proc close*(client: Navi) =
  ## No-op: the browser/runtime owns connections. Present for API symmetry with
  ## the native backends.
  discard

# --- request core (wrapped by middleware in request/stream) ---
proc maybeThrow(client: Navi, req: Request, resp: Response) =
  if client.config.wantsThrow and not resp.ok:
    raise (ref HttpError)(
      msg: $req.verb & " " & $req.url & " -> " & $resp.status & " " & resp.reason,
      response: resp)

proc runCore(client: Navi, req0: Request, cancel: CancelToken): Future[Response] {.async.} =
  ## The innermost `next`: buffered request with the policy navi owns here (cookie
  ## jar, retries with backoff, size cap, throw-on-non-2xx). Redirects and decoding
  ## are the runtime's.
  var req = req0
  var resp: Response
  var attempt = 0
  let policy = client.config.retry
  while true:
    throwIfCancelled(cancel)
    var failed = false
    if not client.jar.isNil: applyCookies(client.jar, req)
    try:
      resp = await fetchExchange(req, nil, client.config.timeoutMs, cancel)
    except CatchableError:
      throwIfCancelled(cancel)   # a cancel is not a retryable failure
      if not (attempt < policy.limit and isRetryableVerb(req.verb, policy)):
        raise   # not retryable: propagate the fetch error
      failed = true
    if not failed and not client.jar.isNil:
      storeCookies(client.jar, req.url, resp)
    if not failed and
       not (attempt < policy.limit and isRetryableVerb(req.verb, policy) and
            isRetryableStatus(resp.status, policy)):
      break
    inc attempt
    await sleep(backoffMs(attempt, resp, policy))
  enforceMaxResponse(resp, client.config.maxResponseBytes)
  client.maybeThrow(req, resp)
  result = resp

proc runCoreStream(client: Navi, req: Request, sink: BodySink,
                   cancel: CancelToken): Future[Response] {.async.} =
  throwIfCancelled(cancel)
  var rq = req
  if not client.jar.isNil: applyCookies(client.jar, rq)
  let limited = limitedSink(sink, client.config.maxResponseBytes)
  var resp = await fetchExchange(rq, limited, client.config.timeoutMs, cancel)
  if not client.jar.isNil: storeCookies(client.jar, rq.url, resp)
  client.maybeThrow(rq, resp)
  result = resp

proc client*(ctx: NaviContext): Navi = ctx.clientp[]
  ## The client handling this request (e.g. to read `ctx.client.config`).

proc next*(ctx: NaviContext): Future[void] {.async.} =
  ## Run the rest of the chain: the next middleware, or -- once they are
  ## exhausted -- the request itself. The outcome lands in `ctx.res`.
  let mws = ctx.clientp[].config.middleware
  if ctx.idx >= mws.len:
    if ctx.sink.isNil:
      ctx.res = await runCore(ctx.clientp[], ctx.req, ctx.cancel)
    else:
      ctx.res = await runCoreStream(ctx.clientp[], ctx.req, ctx.sink, ctx.cancel)
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
              params: seq[(string, string)] = @[],
              cancel: CancelToken = nil): Future[Response] {.async.} =
  ## Perform a request; configured middleware wraps the whole call. `params` are
  ## appended to the URL query; `cancel` aborts the fetch.
  let req = buildRequest(client.config, verb, target, headers, body, json, form,
                         multipart, nil, params)
  if client.config.middleware.len == 0: return await runCore(client, req, cancel)
  let ctx = NaviContext(req: req, clientp: unsafeAddr client, cancel: cancel)
  return await runChain(ctx)

proc stream*(client: Navi, verb: HttpVerb, target: string, sink: BodySink,
             headers = initHeaders(), params: seq[(string, string)] = @[],
             cancel: CancelToken = nil): Future[Response] {.async.} =
  ## Deliver the response body to `sink` as it arrives; `Response.body` stays
  ## empty. Not retried (the stream is consumed as it is read).
  let req = buildRequest(client.config, verb, target, headers, params = params)
  if client.config.middleware.len == 0: return await runCoreStream(client, req, sink, cancel)
  let ctx = NaviContext(req: req, clientp: unsafeAddr client, sink: sink, cancel: cancel)
  return await runChain(ctx)

proc websocket*(client: Navi, url: string,
                headers = initHeaders()): Future[WebSocket] =
  ## Open a WebSocket over the runtime's native `WebSocket`. Accepts `ws://` /
  ## `wss://` (or http/https, which are mapped to ws/wss). Use `send`, `receive`,
  ## and `close`. `headers` is ignored: a browser WebSocket cannot set custom
  ## handshake headers, and the runtime handles ping/pong.
  var u = url
  if u.startsWith("http://"): u = "ws://" & u["http://".len .. ^1]
  elif u.startsWith("https://"): u = "wss://" & u["https://".len .. ^1]
  openWebSocket(u)

include navi/private/verbs
