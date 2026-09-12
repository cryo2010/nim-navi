## navi — synchronous entry point.
##
##   import navi
##   let api = newNavi()
##   let res = api.get("http://example.com")
##   echo res.status, " ", res.body
##
## For async, import `navi/asyncdispatch` or `navi/chronos` instead (exactly
## one entry module per program).

import std/[tables, options]
import navi/private/[entryguard, streamguard]
import navi/proto/sse
import navi/core/public
import navi/core/[engine, pool, session, decompress, redirect, retry, proxy, h2glue]
import navi/core/[cookies, digest, cancel, url]
import navi/proto/h1
import navi/proto/h2/conn
import navi/proto/ws
import navi/backend/sync
from std/strutils import startsWith, find, splitLines, strip, cmpIgnoreCase,
                         contains, toLowerAscii
when defined(naviHttp3):
  import navi/core/altsvc
  import navi/backend/quic
export sse.SseEvent

claimEntry("navi")
export public

type
  NaviContext* = ref object
    ## Carried through the middleware chain. A middleware reads and mutates it,
    ## then calls `next` to run the rest of the chain (which fills `res`).
    req*: Request            ## the outgoing request; modify it before `next`
    res*: Response           ## the response; set by `next`, adjust it after
    clientv: Navi            ## the owning client (see `client`)
    cancel: CancelToken      ## caller's cancellation token, or nil
    idx: int                 ## index of the next middleware to run
  NaviMiddleware* = proc(ctx: NaviContext) {.closure.}
    ## A middleware step: read/modify `ctx.req`, call `ctx.next()` to proceed --
    ## or skip it to short-circuit -- then read/modify `ctx.res`. Run in order;
    ## index 0 is the outermost. A closure, so it can capture: write a factory
    ## `proc bearer(token: string): NaviMiddleware` that returns a step closing over
    ## `token`.

  NaviConfig* {.requiresInit.} = object of NaviConfigBase
    ## `requiresInit`, so it cannot be built with a bare/partial `NaviConfig(...)`
    ## (which would leave fields zeroed, e.g. verify off). Build it with
    ## `initNaviConfig()`.
    middleware*: seq[NaviMiddleware]

  NaviObj = object
    config*: NaviConfig
      ## The client's live configuration. Mutate it to reconfigure between
      ## requests, e.g. `client.config.headers["authorization"] = "Bearer " & tok`;
      ## the change applies from the next request on (the request path reads these
      ## fields live). Exceptions: `tls`, `http`, and `proxy` are bound when
      ## connections are opened, so change those by building a new client (or
      ## `extend`), not in place.
    pool*: Pool[PooledConn[Conn]]
    jar*: CookieJar
    when defined(naviHttp3):
      altSvc: AltSvcCache      ## per-origin h3 discovery cache (HTTP/3 builds)
  Navi* = ref NaviObj

proc closeIdle(pool: Pool[PooledConn[Conn]]) =
  ## Close every idle pooled connection, freeing each one's OpenSSL context.
  ## Shared by `close` and the destructor leak-guard; safe to call twice, since
  ## `drain` empties the pool. Guarded so it never raises out of a destructor.
  for pc in pool.drain():
    try: pc.transport.close()
    except CatchableError: discard

proc `=destroy`(o: var NaviObj) =
  ## Leak-guard: a client collected without an explicit `close` still gets its idle
  ## pooled connections closed here (each holds an ~85 KB OpenSSL context, so they
  ## add up under connection churn). Best-effort and synchronous, so it does not
  ## touch the shared TLS session store (freeing that belongs to the deterministic
  ## `close`, and doing it here could double-free). No-op after `close`, which has
  ## already drained the pool. `close` remains the recommended shutdown. Declared
  ## before `newNavi` so it binds before NaviObj is first constructed.
  if o.pool != nil: closeIdle(o.pool)
  # A custom `=destroy` suppresses the compiler's field destruction, so destroy the
  # managed fields explicitly or they leak. Keep in sync with NaviObj's fields.
  `=destroy`(o.config)
  `=destroy`(o.pool)
  `=destroy`(o.jar)
  when defined(naviHttp3):
    `=destroy`(o.altSvc)

proc initNaviConfig*(): NaviConfig =
  ## The only way to build a config: `NaviConfig` requires every field. Sets the
  ## safe defaults (verify on, decompress on, 2 retries, 20 redirects); override
  ## the fields you want, then pass it to `newNavi`.
  NaviConfig(
    prefixUrl: "", headers: initHeaders(), http: defaultHttpVersions, tls: defaultTls(),
    decompress: true, throwHttpErrors: true, maxRedirects: 20,
    retry: defaultRetryPolicy(), maxResponseBytes: 0,
    auth: Auth(), proxy: "", unixSocket: "",
    maxIdleConns: 0, maxIdleConnsPerHost: 0, idleConnTimeout: 0,
    timeouts: Timeouts(), middleware: @[])

proc newNavi*(config = initNaviConfig()): Navi =
  ## Create a client. `config` supplies defaults (prefixUrl, headers, TLS,
  ## middleware, …).
  when not defined(naviHttp3):
    # H3 in `http` is a silent no-op without -d:naviHttp3 (h1/h2 only); warn once so
    # the common "I asked for h3 but never got it" misconfiguration is not silent.
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
    jar: newCookieJar())
  when defined(naviHttp3): result.altSvc = newAltSvcCache()

proc extend*(client: Navi, config: NaviConfig): Navi =
  ## Derive a new client, layering `config` over this client's (middleware is
  ## appended). The derived client gets its own connection pool and cookie jar.
  var merged = mergeBase(client.config, config)
  merged.middleware = client.config.middleware & config.middleware
  merged.tls.sessionCache = newTlsStore(merged.tls)  # its own cache, not the parent's
  merged.tls.contextStore = newTlsCtxStore(merged.tls)  # its own contexts too
  result = Navi(config: merged,
    pool: newPool[PooledConn[Conn]](merged.idlePerHost, merged.idleGlobal, merged.idleTimeoutMs),
    jar: newCookieJar())
  when defined(naviHttp3): result.altSvc = newAltSvcCache()

proc close*(client: Navi) =
  ## Close all idle pooled connections, freeing their TLS contexts and cached
  ## sessions. Optional but recommended when done with a client: a later request
  ## just opens fresh connections. Without it, pooled connections are reclaimed
  ## only at process exit (and their OpenSSL contexts leak until then).
  closeIdle(client.pool)
  closeTlsStore(client.config.tls.sessionCache)
  closeTlsCtxStore(client.config.tls.contextStore)

when defined(naviHttp3):
  proc h3Transport(client: Navi, req: Request, ep: AltSvcEndpoint): Response =
    ## Send `req` (any verb with a buffered body) over HTTP/3 to a discovered
    ## endpoint and build a navi Response, so the caller's policy layer (cookies,
    ## redirects, retries, throw-on-non-2xx) is reused unchanged. Raises
    ## `QuicError` on failure, which `transport` catches to fall back to h2/h1.
    var fwd: seq[(string, string)]
    for k, v in req.headers:
      let lk = k.toLowerAscii
      if lk notin h3SkipHeaders: fwd.add((lk, v))
    var fwdTrl: seq[(string, string)]
    for k, v in req.trailers:
      let lk = k.toLowerAscii
      if lk.len > 0 and lk[0] != ':' and lk notin h3SkipHeaders and lk notin ["te", "trailer"]:
        fwdTrl.add((lk, v))
    let conn = h3Open(ep.host, ep.port, sni = req.url.host,
                      caFile = client.config.tls.caFile,
                      verify = client.config.tls.wantsVerify,
                      maxBody = uint64(max(0, client.config.maxResponseBytes)))
    try:
      let r = conn.request($req.verb, req.url.requestTarget, fwd, req.body,
                           req.bodyStream, fwdTrl)
      result = initResponse(r.status, "", "HTTP/3", initHeaders(r.headers), r.body)
      result.trailers = initHeaders(r.trailers)
    finally:
      conn.close()

proc transport(client: Navi, req: Request, sink: BodySink): Response =
  ## Pool-based transport (one request per connection at a time). In a
  ## `-d:naviHttp3` build, a GET to an origin that has advertised h3 (Alt-Svc) is
  ## sent over HTTP/3; any QUIC failure falls back to h2/h1. The h3 endpoint is
  ## learned from the `alt-svc` header captured on prior h2/h1 responses.
  when defined(naviHttp3):
    # Any verb may use h3, whether its body is buffered or streamed (bodyStream is
    # pulled over the h3 request stream, just like h2).
    if client.config.wantsH3 and req.url.isTls:
      let ep = client.altSvc.h3Endpoint("https", req.url.host, req.url.port)
      if ep.isSome:
        try: return h3Transport(client, req, ep.get)
        except QuicError: discard   # fall back to the h2/h1 transport below
  result = poolTransport(client, req, sink)
  when defined(naviHttp3):
    let alt = result.headers.get("alt-svc")
    if alt.len > 0:
      client.altSvc.record("https", req.url.host, req.url.port, alt)

proc runCore(client: Navi, req: Request, cancel: CancelToken): Response =
  ## The innermost `next`: the full policy layer for one buffered request.
  performRequest(client, req, cancel)

proc client*(ctx: NaviContext): Navi = ctx.clientv
  ## The client handling this request (e.g. to read `ctx.client.config`).

proc next*(ctx: NaviContext) =
  ## Run the rest of the chain: the next middleware, or -- once they are
  ## exhausted -- the request itself. The outcome lands in `ctx.res`.
  let mws = ctx.clientv.config.middleware
  if ctx.idx >= mws.len:
    ctx.res = runCore(ctx.clientv, ctx.req, ctx.cancel)
  else:
    let m = mws[ctx.idx]
    inc ctx.idx
    m(ctx)

proc request*(client: Navi, verb: HttpVerb, target: string,
              headers = initHeaders(), body = "", json: JsonNode = nil,
              form: seq[(string, string)] = @[], multipart: Multipart = @[],
              bodyStream: BodyProducer = nil,
              params: seq[(string, string)] = @[],
              cancel: CancelToken = nil,
              trailers = initHeaders()): Response =
  ## Perform a request and return the response. `json`/`form`/`multipart` encode
  ## the body; `bodyStream` uploads a chunked body from a pull-based producer.
  ## `params` are appended to the URL query; `cancel` aborts the request.
  ## `trailers` are sent after the body (chunked on h1, a trailing HEADERS block on
  ## h2/h3). Configured middleware wraps the whole call.
  let req = buildRequest(client.config, verb, target, headers, body, json,
                         form, multipart, bodyStream, params, trailers)
  if client.config.middleware.len == 0: return runCore(client, req, cancel)
  let ctx = NaviContext(req: req, clientv: client, cancel: cancel)
  ctx.next()
  ctx.res

include navi/private/stream_download
include navi/private/sse_stream
include navi/private/verbs
include navi/private/batch
include navi/private/websocket
