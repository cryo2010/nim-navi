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
  Next* = proc(req: Request): Future[Response] {.closure.}
    ## Runs the rest of the chain (inner middleware, then the request itself) and
    ## returns its response. `await` it.
  Middleware* = proc(req: Request, next: Next): Future[Response] {.closure.}
    ## Wraps a request; may be async. Modify `req`, `await next(req)` to proceed
    ## (or skip it to short-circuit), then inspect or replace the response.

  NaviOptions* = object of NaviOptionsBase
    middleware*: seq[Middleware]   ## run in order; index 0 is the outermost wrap

  Navi* = object
    options*: NaviOptions   ## the runtime owns connections
    jar: CookieJar          ## kept off-browser; nil in a browser (its store owns cookies)

proc defaultOptions*(): NaviOptions =
  # The browser decodes bodies and forbids the Accept-Encoding request header, so
  # keep navi from adding it; TLS and HTTP-version negotiation are the runtime's.
  result.decompress = some(false)

proc wrap(m: Middleware, nxt: Next): Next =
  ## `m` and `nxt` are parameters (not loop locals) so each layer captures its
  ## own `nxt` -- capturing a loop variable would make every layer share one.
  proc(req: Request): Future[Response] =
    {.cast(gcsafe).}: m(req, nxt)

proc compose(mws: seq[Middleware], base: Next): Next =
  ## Nest the middleware around `base` so mws[0] is outermost and each `next`
  ## calls the layer beneath it.
  result = base
  for i in countdown(mws.high, 0):
    result = wrap(mws[i], result)

# A browser owns the cookie store (and hides Set-Cookie from fetch); Node, Deno,
# Bun, and Workers do not, so navi keeps the jar there. `document` exists only in
# a browser document context.
proc inBrowser(): bool {.importjs: "(typeof document !== 'undefined')".}

proc newNavi*(options = defaultOptions()): Navi =
  result = Navi(options: options)
  if not inBrowser(): result.jar = newCookieJar()

proc extend*(client: Navi, options: NaviOptions): Navi =
  var merged = mergeBase(client.options, options)
  merged.middleware = client.options.middleware & options.middleware
  result = Navi(options: merged)
  if not inBrowser(): result.jar = newCookieJar()

proc close*(client: Navi) =
  ## No-op: the browser/runtime owns connections. Present for API symmetry with
  ## the native backends.
  discard

# --- request core (wrapped by middleware in request/stream) ---
proc maybeThrow(client: Navi, req: Request, resp: Response) =
  if client.options.wantsThrow and not resp.ok:
    raise (ref HttpError)(
      msg: $req.verb & " " & $req.url & " -> " & $resp.status & " " & resp.reason,
      response: resp)

proc runCore(client: Navi, req0: Request): Future[Response] {.async.} =
  ## The innermost `next`: buffered request with the policy navi owns here (cookie
  ## jar, retries with backoff, throw-on-non-2xx). Redirects and decoding are the
  ## runtime's.
  var req = req0
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
    await sleep(backoffMs(attempt, resp))
  client.maybeThrow(req, resp)
  result = resp

proc runCoreStream(client: Navi, req: Request, sink: BodySink): Future[Response] {.async.} =
  var rq = req
  if not client.jar.isNil: applyCookies(client.jar, rq)
  var resp = await fetchExchange(rq, sink, client.options.timeoutMs)
  if not client.jar.isNil: storeCookies(client.jar, rq.url, resp)
  client.maybeThrow(rq, resp)
  result = resp

proc request*(client: Navi, verb: HttpVerb, target: string,
              headers = initHeaders(), body = "", json: JsonNode = nil,
              form: seq[(string, string)] = @[],
              multipart: Multipart = @[]): Future[Response] {.async.} =
  ## Perform a request; configured middleware wraps the whole call.
  let req = buildRequest(client.options, verb, target, headers, body, json, form,
                         multipart, nil)
  let base: Next = proc(r: Request): Future[Response] = runCore(client, r)
  result = await compose(client.options.middleware, base)(req)

proc stream*(client: Navi, verb: HttpVerb, target: string, sink: BodySink,
             headers = initHeaders()): Future[Response] {.async.} =
  ## Deliver the response body to `sink` as it arrives; `Response.body` stays
  ## empty. Not retried (the stream is consumed as it is read).
  let req = buildRequest(client.options, verb, target, headers)
  let base: Next = proc(r: Request): Future[Response] = runCoreStream(client, r, sink)
  result = await compose(client.options.middleware, base)(req)

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
