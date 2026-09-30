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
import navi/core/[cookies, digest, cancel, url, sinkgate]
import navi/proto/h1
import navi/proto/h2/conn
import navi/proto/ws
import navi/backend/sync
from std/strutils import startsWith, find, splitLines, strip, cmpIgnoreCase,
                         contains, toLowerAscii
when defined(naviHttp3):
  import navi/core/altsvc
  import navi/backend/quic
  from std/times import epochTime

  type H3Cached = object
    ## One pooled HTTP/3 connection, keyed by origin (see `NaviObj.h3conns`).
    conn: QuicConn
    lastUse: float          ## epochTime() when its last request finished

  const h3MaxIdleSec = 20.0
    ## Reuse a pooled h3 connection only if it went idle less than this long ago.
    ## The sync backend runs no background pump, so an idle connection sends no
    ## keep-alive PINGs and the peer silently reclaims it once its idle timer fires
    ## (the client advertises 30 s, the effective timeout is the minimum of the two).
    ## Reusing it past that point would submit a request onto a connection that is
    ## already gone -- an indeterminate, non-replayable failure for a POST. Below the
    ## threshold, re-open pre-emptively instead: a cold connect is provably
    ## pre-submit, so every method stays safe.
export sse.SseEvent, sse.defaultSseMinRetryMs

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
    userSink: BodySink       ## wrapped gated response sink (nil for a buffered call);
                             ## forwarded to the request core once the chain is spent
    gate: SinkGate           ## the sink's delivery gate (nil for a buffered call)
  NaviMiddleware* = proc(ctx: NaviContext) {.closure.}
    ## A middleware step: read/modify `ctx.req`, call `ctx.next()` to proceed --
    ## or skip it to short-circuit -- then read/modify `ctx.res`. Run in order;
    ## index 0 is the outermost. A closure, so it can capture: write a factory
    ## `proc bearer(token: string): NaviMiddleware` that returns a step closing over
    ## `token`.

  NaviConfig* {.requiresInit.} = object of NaviConfigBase
    ## `requiresInit`, so it cannot be built with a bare/partial `NaviConfig(...)`
    ## (which would leave fields zeroed, e.g. no retries and no redirects).
    ## Build it with `initNaviConfig()`.
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
      h3conns: Table[string, H3Cached]
        ## Live per-origin HTTP/3 connections, the QUIC twin of `pool`. Requests to
        ## the same origin reuse one connection (new h3 stream each) instead of
        ## paying a fresh QUIC handshake -- and, far worse, leaving the server a
        ## dead connection to time out -- per request. Unsynchronised, like `pool`:
        ## one sync client is used by one thread at a time.
  Navi* = ref NaviObj

proc closeIdle(pool: Pool[PooledConn[Conn]]) =
  ## Close every idle pooled connection, freeing each one's OpenSSL context.
  ## Shared by `close` and the destructor leak-guard; safe to call twice, since
  ## `drain` empties the pool. Guarded so it never raises out of a destructor.
  for pc in pool.drain():
    try: pc.transport.close()
    except CatchableError: discard

when defined(naviHttp3):
  proc closeH3Conns(conns: var Table[string, H3Cached]) =
    ## Close and forget every pooled h3 connection (each owns a UDP socket plus
    ## ngtcp2/nghttp3/OpenSSL state, and its `close` sends the peer a
    ## CONNECTION_CLOSE so the server drops its half immediately). Shared by
    ## `close` and the destructor leak-guard; safe to call twice, and never raises
    ## out of a destructor.
    for e in conns.mvalues:
      try: e.conn.close()
      except CatchableError: discard
    conns.clear()

proc `=destroy`(o: var NaviObj) =
  ## Leak-guard: a client collected without an explicit `close` still gets its idle
  ## pooled connections closed here (each holds an ~85 KB OpenSSL context, so they
  ## add up under connection churn). Best-effort and synchronous, so it does not
  ## touch the shared TLS session store (freeing that belongs to the deterministic
  ## `close`, and doing it here could double-free). No-op after `close`, which has
  ## already drained the pool. `close` remains the recommended shutdown. Declared
  ## before `newNavi` so it binds before NaviObj is first constructed.
  if o.pool != nil: closeIdle(o.pool)
  when defined(naviHttp3): closeH3Conns(o.h3conns)
  # A custom `=destroy` suppresses the compiler's field destruction, so destroy the
  # managed fields explicitly or they leak. Keep in sync with NaviObj's fields.
  `=destroy`(o.config)
  `=destroy`(o.pool)
  `=destroy`(o.jar)
  when defined(naviHttp3):
    `=destroy`(o.altSvc)
    `=destroy`(o.h3conns)

proc initNaviConfig*(): NaviConfig =
  ## The only way to build a config: `NaviConfig` requires every field. Sets the
  ## safe defaults (verify on, decompress on, 2 retries, 20 redirects); override
  ## the fields you want, then pass it to `newNavi`.
  NaviConfig(
    prefixUrl: "", headers: initHeaders(), http: defaultHttpVersions, tls: defaultTls(),
    decompress: true, throwHttpErrors: true, maxRedirects: 20,
    retry: defaultRetryPolicy(), maxResponseBytes: 0, expectContinueMs: 0,
    auth: Auth(), proxy: "", unixSocket: "",
    maxIdleConns: 0, maxIdleConnsPerHost: 0, idleConnTimeout: 0,
    timeouts: Timeouts(), resolvedProxy: nil, middleware: @[])

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
  cfg.resolvedProxy = buildResolvedProxy(cfg)    # resolve env/proxy/NO_PROXY once (#361)
  result = Navi(config: cfg,
    pool: newPool[PooledConn[Conn]](cfg.idlePerHost, cfg.idleGlobal, cfg.idleTimeoutMs),
    jar: newCookieJar())
  when defined(naviHttp3):
    result.altSvc = newAltSvcCache()
    result.h3conns = initTable[string, H3Cached]()

proc extend*(client: Navi, config: NaviConfig): Navi =
  ## Derive a new client, layering `config` over this client's (middleware is
  ## appended). The derived client gets its own connection pool and cookie jar.
  var merged = mergeBase(client.config, config)
  merged.middleware = client.config.middleware & config.middleware
  merged.tls.sessionCache = newTlsStore(merged.tls)  # its own cache, not the parent's
  merged.tls.contextStore = newTlsCtxStore(merged.tls)  # its own contexts too
  merged.resolvedProxy = buildResolvedProxy(merged)  # its own resolved proxy (#361):
                                                     # an extended client with a
                                                     # different proxy must not inherit
                                                     # the parent's cache
  result = Navi(config: merged,
    pool: newPool[PooledConn[Conn]](merged.idlePerHost, merged.idleGlobal, merged.idleTimeoutMs),
    jar: newCookieJar())
  when defined(naviHttp3):
    result.altSvc = newAltSvcCache()
    result.h3conns = initTable[string, H3Cached]()

proc close*(client: Navi) =
  ## Close all idle pooled connections, freeing their TLS contexts and cached
  ## sessions. Optional but recommended when done with a client: a later request
  ## just opens fresh connections. Without it, pooled connections are reclaimed
  ## only at process exit (and their OpenSSL contexts leak until then).
  closeIdle(client.pool)
  when defined(naviHttp3): closeH3Conns(client.h3conns)
  closeTlsStore(client.config.tls.sessionCache)
  closeTlsCtxStore(client.config.tls.contextStore)
  # The h3 driver caches its own SSL_CTX per policy (#454); this is that cache's
  # half of closing the context store, and follows the h3 connections being closed.
  when defined(naviHttp3): h3ReleaseTlsContexts(client.config.tls.contextStore)

when defined(naviHttp3):
  proc evictH3(client: Navi, origin: string, conn: QuicConn) =
    ## Drop `conn` from the pool (if it is still the entry for `origin`) and close it,
    ## which also sends the peer a CONNECTION_CLOSE so it releases its state at once.
    client.h3conns.withValue(origin, e):
      if e.conn == conn: client.h3conns.del(origin)
    try: conn.close()
    except CatchableError: discard

  proc liveH3Conn(client: Navi, origin: string): QuicConn =
    ## The pooled connection for `origin` when it is still usable, else nil (having
    ## closed and dropped the unusable one). "Usable" is `alive` -- the handle is open
    ## and the peer is not draining -- plus recently used: see `h3MaxIdleSec` for why
    ## a long-idle connection is retired rather than probed.
    client.h3conns.withValue(origin, e):
      if e.conn.alive and epochTime() - e.lastUse < h3MaxIdleSec:
        return e.conn
      let dead = e.conn
      client.h3conns.del(origin)
      try: dead.close()
      except CatchableError: discard
    nil

  proc h3Transport(client: Navi, req: Request, ep: AltSvcEndpoint): Response =
    ## Send `req` (any verb with a buffered body) over HTTP/3 to a discovered
    ## endpoint and build a navi Response, so the caller's policy layer (cookies,
    ## redirects, retries, throw-on-non-2xx) is reused unchanged. Raises
    ## `QuicError` on transport failure, which `transport` catches to fall back to
    ## h2/h1 -- or `TimeoutError` when the client's own budget (attempt/read/total)
    ## expires mid-request, which propagates like any other navi timeout (no
    ## fallback: a timed-out request must not silently burn a second budget
    ## re-running over TCP).
    var fwd: seq[(string, string)]
    for k, v in req.headers:
      let lk = k.toLowerAscii
      if lk notin h3SkipHeaders: fwd.add((lk, v))
    var fwdTrl: seq[(string, string)]
    for k, v in req.trailers:
      let lk = k.toLowerAscii
      if not isForbiddenTrailer(lk):     # the shared h1/h2/h3 trailer filter (#296)
        fwdTrl.add((lk, v))
    # Bound the blocking drive loop with the client's own budget: attempt (the
    # per-attempt wall clock) when set, else read, else total. Without a cap the
    # sync h3 leg has NO timeout at all -- pump waits on the ngtcp2 timer/socket,
    # so a stalling or drip-feeding h3 server wedges the calling thread forever,
    # beyond the reach of every configured timeout (the h1/h2 sync path at least
    # has socket-level read timeouts). 0 (no timeouts configured) preserves the
    # historical unbounded behavior.
    let capMs = if client.config.attemptMs > 0: client.config.attemptMs
                elif client.config.readMs > 0: client.config.readMs
                else: client.config.totalMs
    let origin = originKey(req.url)
    var retried = false
    while true:
      # Reuse this origin's pooled connection, or open (and pool) a fresh one. Opening
      # raises `QuicError` before anything is submitted, so `transport` may fall back
      # to h2/h1 for any method.
      var reused = true
      var conn = client.liveH3Conn(origin)
      if conn == nil:
        reused = false
        # RFC 7838 2.4: an alternative that fails to connect is marked broken, so
        # the next request goes straight to TCP instead of paying the same stalled
        # QUIC handshake again until the advertisement's `ma` expires (#432). The
        # mark-broken/mark-working pair is shared with the other h3 openers (#453).
        conn = client.altSvc.openH3Tracked(req.url.host, req.url.port,
          h3Open(ep.host, ep.port, sni = req.url.host,
                 tls = client.config.tls,
                 maxBody = uint64(max(0, client.config.maxResponseBytes)),
                 connectMs = client.config.connectMs,
                 totalMs = client.config.totalMs))
        client.h3conns[origin] = H3Cached(conn: conn, lastUse: epochTime())
      try:
        let r = conn.request($req.verb, req.url.requestTarget, fwd, req.body,
                             req.bodyStream, fwdTrl, deadlineMs = capMs)
        client.h3conns.withValue(origin, e):
          if e.conn == conn: e.lastUse = epochTime()
        result = initResponse(r.status, "", "HTTP/3", initHeaders(r.headers), r.body)
        result.trailers = initHeaders(r.trailers)
        return
      except QuicError as e:
        # The connection state is unknown after any transport failure (a half-read
        # stream, a peer that is still sending), so drop it rather than hand it to the
        # next request. A pooled connection the peer had already reclaimed fails here
        # exactly like a live one that broke, so retry once on a fresh connection when
        # the request may be re-sent -- otherwise this pool would turn a recoverable
        # stale-connection race into a hard failure (the same discipline the h1/h2
        # pool applies to a stale keep-alive socket).
        client.evictH3(origin, conn)
        if reused and not retried and mayFallBackFromH3(req, e of QuicSubmittedError):
          retried = true
          continue
        raise
      except CatchableError:
        # TimeoutError / ResponseTooLargeError / a body-length mismatch: the stream is
        # gone but the peer may still be transmitting on it, so the connection is not
        # safe to reuse either. No retry -- these are the caller's own limits, not a
        # transport race.
        client.evictH3(origin, conn)
        raise

proc transport(client: Navi, req: Request, sink: BodySink,
               asyncStream: BodyProducer = nil,
               userSink: BodySink = nil, gate: SinkGate = nil): Response =
  ## Pool-based transport (one request per connection at a time). In a
  ## `-d:naviHttp3` build, a GET to an origin that has advertised h3 (Alt-Svc) is
  ## sent over HTTP/3; a pre-submit QUIC failure falls back to h2/h1, while one
  ## raised after the request was submitted only falls back when it is safe to
  ## re-send (see `mayFallBackFromH3`). The h3 endpoint is
  ## learned from the `alt-svc` header captured on prior h2/h1 responses.
  ##
  ## `asyncStream` is accepted for signature parity with the async backends' wider
  ## `run` form but is always nil here (the sync backend has no event loop). `userSink`
  ## / `gate` (when set) stream the FINAL response body to the caller's gated sink; the
  ## h3 leg stays buffered (the performRequest fallback delivers its final body).
  if asyncStream != nil: discard   # accepted for parity; never set on the sync path
  when defined(naviHttp3):
    # Any verb may use h3, whether its body is buffered or streamed (bodyStream is
    # pulled over the h3 request stream, just like h2). The h3 body is buffered here;
    # the gated sink is fed by the performRequest fallback, not this leg.
    if client.config.wantsH3 and req.url.isTls and client.config.tls.h3TlsUsable:
      let ep = client.altSvc.h3Endpoint("https", req.url.host, req.url.port)
      if ep.isSome:
        try: return h3Transport(client, req, ep.get)
        except QuicError as e:
          # Same fall-back discipline as the h1/h2 fall-through (#378): a bare
          # `QuicError` is provably pre-submit (nothing reached the server) and may
          # fall back for any method, while a `QuicSubmittedError` (the request was
          # already on the wire) may only be re-sent when the method is idempotent.
          # A streamed body (`bodyStream`) narrows the submitted case further (#293):
          # its producer may already have been pulled, so only a replayable (buffered)
          # body may be handed to h2/h1.
          if not mayFallBackFromH3(req, e of QuicSubmittedError): raise
          # fall through to the h2/h1 transport below
  result = poolTransport(client, req, sink, nil, userSink, gate)
  when defined(naviHttp3):
    let alt = result.headers.get("alt-svc")
    if alt.len > 0:
      client.altSvc.record("https", req.url.host, req.url.port, alt)

proc runCore(client: Navi, req: Request, cancel: CancelToken,
             userSink: BodySink = nil, gate: SinkGate = nil): Response =
  ## The innermost `next`: the full policy layer for one buffered request. `userSink`
  ## / `gate` (when set) stream the final response body to the caller's gated sink.
  performRequest(client, req, cancel, nil, userSink, gate)

proc client*(ctx: NaviContext): Navi = ctx.clientv
  ## The client handling this request (e.g. to read `ctx.client.config`).

proc cookies*(client: Navi): seq[StoredCookie] =
  ## A read-only snapshot of the client's cookie jar, for inspection/debugging:
  ## every cookie currently stored (all origins), across the whole jar rather than
  ## the URL-scoped view a request sees. Expired-but-not-yet-pruned entries are
  ## included; `StoredCookie.expires` reveals staleness. See also `items`/`len`/`$`
  ## on `client.jar`.
  for c in client.jar: result.add c

proc next*(ctx: NaviContext) =
  ## Run the rest of the chain: the next middleware, or -- once they are
  ## exhausted -- the request itself. The outcome lands in `ctx.res`.
  let mws = ctx.clientv.config.middleware
  if ctx.idx >= mws.len:
    ctx.res = runCore(ctx.clientv, ctx.req, ctx.cancel, ctx.userSink, ctx.gate)
  else:
    let m = mws[ctx.idx]
    inc ctx.idx
    m(ctx)

proc wrapSink(s: GatedBodySink, gate: SinkGate): BodySink =
  ## Adapt a caller's gated sink into the internal `BodySink` the engine drains into:
  ## mark the gate as fed (so the replay guards bar a retry) before each delivery, and
  ## turn a `false` return into a `SinkStopSignal` the drain site catches and converts
  ## to a normal early stop. Nil in -> nil out (no sink).
  if s.isNil: return nil
  result = proc(data: string) {.closure, raises: [CatchableError].} =
    gate.fed = true
    if not s(data):
      raise newException(SinkStopSignal, "navi: sink requested early stop")

proc wrapSink(s: BodySink, gate: SinkGate): BodySink =
  ## Adapt a caller's void sink (always continue) into the internal `BodySink`, marking
  ## the gate as fed before each delivery. Nil in -> nil out.
  if s.isNil: return nil
  result = proc(data: string) {.closure, raises: [CatchableError].} =
    gate.fed = true
    s(data)

proc requestResolved(client: Navi, verb: HttpVerb, target: string,
                     headers: Headers, body: ResolvedBody,
                     form: seq[(string, string)],
                     params: seq[(string, string)],
                     cancel: CancelToken,
                     trailers: Headers,
                     userSink: BodySink = nil, gate: SinkGate = nil): Response =
  let req = buildRequest(client.config, verb, target, headers, body,
                         form, params, trailers)
  if client.config.middleware.len == 0:
    return runCore(client, req, cancel, userSink, gate)
  let ctx = NaviContext(req: req, clientv: client, cancel: cancel,
                        userSink: userSink, gate: gate)
  ctx.next()
  ctx.res

proc request*[B](client: Navi, verb: HttpVerb, target: string,
                 headers = initHeaders(), body: B = "",
                 form: seq[(string, string)] = @[],
                 params: seq[(string, string)] = @[],
                 cancel: CancelToken = nil,
                 trailers = initHeaders()): Response =
  ## Perform a request and return the response. `body` is dispatched by type: a
  ## `string` is the raw body, a `JsonNode` is sent as JSON, a `Multipart` as
  ## multipart/form-data, a `BodyProducer` or closure `BodyIterator` streams a
  ## chunked upload, and any other value is serialized to JSON. `form` encodes a
  ## urlencoded body and is outranked by a typed `body`. `params` are appended to
  ## the URL query; `cancel` aborts the request. `trailers` are sent after the body
  ## (chunked on h1, a trailing HEADERS block on h2/h3). Configured middleware wraps
  ## the whole call.
  requestResolved(client, verb, target, headers, toBody(body), form, params,
                  cancel, trailers)

proc request*[B](client: Navi, verb: HttpVerb, target: string,
                 headers: Headers, body: B, sink: GatedBodySink,
                 form: seq[(string, string)] = @[],
                 params: seq[(string, string)] = @[],
                 cancel: CancelToken = nil,
                 trailers = initHeaders()): Response =
  ## Like `request`, but streams the FINAL response body to `sink` instead of
  ## buffering it into `res.body`. The full policy layer still runs (redirects,
  ## retries, digest, middleware, throw-on-non-2xx): only the body of the response
  ## actually surfaced to you reaches the sink; redirect/retry/digest/thrown-error
  ## bodies never do (an `HttpError` still carries its buffered body). `sink` returns
  ## `bool`: `true` keeps going, `false` stops the download early -- the request then
  ## returns normally with `res.body == ""` and `res.bodyTruncated == true`. On a full
  ## drain `res.body` is "" and any trailers are populated; HEAD/204/304 never call it.
  let gate = newSinkGate()
  requestResolved(client, verb, target, headers, toBody(body), form, params,
                  cancel, trailers, wrapSink(sink, gate), gate)

proc request*[B](client: Navi, verb: HttpVerb, target: string,
                 headers: Headers, body: B, sink: BodySink,
                 form: seq[(string, string)] = @[],
                 params: seq[(string, string)] = @[],
                 cancel: CancelToken = nil,
                 trailers = initHeaders()): Response =
  ## Like the gated `request` overload, but `sink` is a void `BodySink` (always
  ## continue): the FINAL response body streams to it and cannot be stopped early, so
  ## `res.bodyTruncated` is never set by it. Convenient when you always want the whole
  ## body pushed to your sink.
  let gate = newSinkGate()
  requestResolved(client, verb, target, headers, toBody(body), form, params,
                  cancel, trailers, wrapSink(sink, gate), gate)

include navi/private/stream_download
include navi/private/stream_verbs
include navi/private/sse_stream
include navi/private/verbs
include navi/private/batch
include navi/private/websocket
