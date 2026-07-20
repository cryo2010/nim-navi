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
## (the runtime owns connections). Cookies are the browser store's by default,
## but `NaviOptions(cookieJar: some(true))` opts into a navi-managed jar for
## runtimes without one (Node/undici); see `newNavi`.

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
  Hook* = proc(ctx: HookCtx): Future[void] {.closure.}
    ## Lifecycle callback; may be async and `await` inside. Mutate `ctx.request`
    ## (beforeRequest), read/mutate `ctx.response` (afterResponse), read
    ## `ctx.attempt` (beforeRetry).
  Hooks* = object
    beforeRequest*: seq[Hook]
    afterResponse*: seq[Hook]
    beforeRetry*: seq[Hook]

  NaviOptions* = object of NaviOptionsBase
    hooks*: Hooks   ## lifecycle callbacks (may be async)
    cookieJar*: Option[bool]
      ## opt into a navi-managed cookie jar. Off by default: in a browser the
      ## store owns cookies (and this is inert -- Set-Cookie is hidden and the
      ## Cookie header is forbidden). Turn it on for runtimes with no cookie
      ## store, e.g. Node/undici, so cookies persist across requests.

  Navi* = object
    options*: NaviOptions   ## the runtime owns connections; cookies too unless cookieJar is on
    jar: CookieJar          ## nil unless cookieJar was opted into

proc defaultOptions*(): NaviOptions =
  # The browser decodes bodies and forbids the Accept-Encoding request header, so
  # keep navi from adding it; TLS and HTTP-version negotiation are the runtime's.
  result.decompress = some(false)

proc mergeHooks(base, add: Hooks): Hooks =
  Hooks(beforeRequest: base.beforeRequest & add.beforeRequest,
        afterResponse: base.afterResponse & add.afterResponse,
        beforeRetry: base.beforeRetry & add.beforeRetry)

proc runHook(hook: Hook, ctx: HookCtx): Future[void] = hook(ctx)

proc newNavi*(options = defaultOptions()): Navi =
  result = Navi(options: options)
  if options.cookieJar.get(false): result.jar = newCookieJar()

proc extend*(client: Navi, options: NaviOptions): Navi =
  var merged = mergeBase(client.options, options)
  merged.hooks = mergeHooks(client.options.hooks, options.hooks)
  merged.cookieJar = some(client.options.cookieJar.get(false) or
                          options.cookieJar.get(false))
  result = Navi(options: merged)
  if merged.cookieJar.get(false): result.jar = newCookieJar()

proc close*(client: Navi) =
  ## No-op: the browser/runtime owns connections. Present for API symmetry with
  ## the native backends.
  discard

# --- pipeline steps (shared by request and stream) ---
proc runBefore(client: Navi, req: Request): Future[Request] {.async.} =
  let ctx = HookCtx(request: req)
  for hook in client.options.hooks.beforeRequest: await runHook(hook, ctx)
  result = ctx.request

proc runAfter(client: Navi, req: Request, resp: Response): Future[Response] {.async.} =
  let ctx = HookCtx(request: req, response: resp)
  for hook in client.options.hooks.afterResponse: await runHook(hook, ctx)
  result = ctx.response

proc maybeThrow(client: Navi, req: Request, resp: Response) =
  if client.options.wantsThrow and not resp.ok:
    raise (ref HttpError)(
      msg: $req.verb & " " & $req.url & " -> " & $resp.status & " " & resp.reason,
      response: resp)

proc perform(client: Navi, req0: Request): Future[Response] {.async.} =
  ## Buffered request with the policy layer navi still owns: beforeRequest hooks,
  ## retries with backoff on transient failures, afterResponse hooks, and
  ## throw-on-non-2xx. Redirects, cookies, and decoding are the browser's.
  var req = await client.runBefore(req0)
  var resp: Response
  var attempt = 0
  let maxRetries = client.options.retryLimit
  while true:
    var failed = false
    if not client.jar.isNil: applyCookies(client.jar, req)
    try:
      resp = await fetchExchange(req, nil, client.options.timeoutMs)
    except CatchableError:
      if not (attempt < maxRetries and isRetryableVerb(req.verb)):
        raise   # not retryable: propagate the fetch error
      failed = true
    if not failed and not client.jar.isNil:
      storeCookies(client.jar, req.url, resp)
    if not failed and
       not (attempt < maxRetries and isRetryableVerb(req.verb) and
            isRetryableStatus(resp.status)):
      break
    inc attempt
    block:
      let ctx = HookCtx(request: req, attempt: attempt)
      for hook in client.options.hooks.beforeRetry: await runHook(hook, ctx)
      req = ctx.request
    await sleep(backoffMs(attempt, resp))
  resp = await client.runAfter(req, resp)
  client.maybeThrow(req, resp)
  result = resp

proc request*(client: Navi, verb: HttpVerb, target: string,
              headers = initHeaders(), body = "", json: JsonNode = nil,
              form: seq[(string, string)] = @[],
              multipart: Multipart = @[]): Future[Response] {.async.} =
  result = await client.perform(
    buildRequest(client.options, verb, target, headers, body, json, form,
                 multipart, nil))

proc stream*(client: Navi, verb: HttpVerb, target: string, sink: BodySink,
             headers = initHeaders()): Future[Response] {.async.} =
  ## Deliver the response body to `sink` as it arrives; `Response.body` stays
  ## empty. Not retried (the stream is consumed as it is read).
  var req = await client.runBefore(buildRequest(client.options, verb, target, headers))
  if not client.jar.isNil: applyCookies(client.jar, req)
  var resp = await fetchExchange(req, sink, client.options.timeoutMs)
  if not client.jar.isNil: storeCookies(client.jar, req.url, resp)
  resp = await client.runAfter(req, resp)
  client.maybeThrow(req, resp)
  result = resp

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
