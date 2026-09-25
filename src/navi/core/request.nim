## Request model, client options, and the pure request-building pipeline.
##
## Nothing here performs I/O: `buildRequest` merges instance defaults with
## per-call arguments into a concrete `Request` that any backend can execute.

import std/[options, json, jsonutils, base64, tables, strutils]
from std/uri import encodeQuery
import ./headers, ./url, ./response, ./multipart, ./version
import ../backend/api
export options, multipart

type
  AuthKind* = enum akNone, akBasic, akBearer, akDigest
  Auth* = object
    case kind*: AuthKind
    of akBasic, akDigest:
      user*, pass*: string
    of akBearer:
      token*: string
    of akNone: discard

proc basicAuth*(user, pass: string): Auth =
  Auth(kind: akBasic, user: user, pass: pass)
proc bearerAuth*(token: string): Auth =
  Auth(kind: akBearer, token: token)
proc digestAuth*(user, pass: string): Auth =
  ## HTTP Digest auth. Unlike basic/bearer, the header can only be built after
  ## the server's 401 challenge, so the engine adds it on a one-shot retry.
  Auth(kind: akDigest, user: user, pass: pass)

proc header(a: Auth): string =
  case a.kind
  of akBasic: "Basic " & encode(a.user & ":" & a.pass)
  of akBearer: "Bearer " & a.token
  of akDigest, akNone: ""   # digest is added by the engine after the challenge

type
  HttpVerb* = enum
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"

  HttpVersion* = enum
    H1 = "HTTP/1.1"
    H2 = "HTTP/2"
    H3 = "HTTP/3"   ## Opt-in only. Unlike H1/H2, H3 is never implied by an empty
                    ## `http` set; it is honored solely in a `-d:naviHttp3` build
                    ## and negotiated per origin via Alt-Svc (see README.md for
                    ## the HTTP/3 build flag, requirements, and backend support).

  RetryPolicy* = object
    ## When and how a request is retried. `initNaviConfig` seeds `defaultRetryPolicy`.
    limit*: int                     ## retry attempts, 0 disables (default 2)
    methods*: set[HttpVerb]         ## verbs eligible for retry (idempotent by default)
    statuses*: seq[int]             ## response statuses that trigger a retry
    maxDelay*: int                  ## upper bound on the wait between attempts, ms

  Timeouts* = object
    ## Per-phase deadlines in milliseconds; 0 (default) disables that phase's
    ## limit. `connect` and `read` are enforced on the native backends (sync,
    ## asyncdispatch, chronos); `total` and `attempt` on all four. On `navi/js`
    ## only `total` and `attempt` are enforceable (via `AbortSignal.timeout`), as
    ## `fetch` hides the connect/read phases.
    connect*: int   ## TCP connect + TLS handshake (establishment)
    read*: int      ## stall waiting for a response chunk (per-read idle)
    total*: int     ## whole request, including retries/redirects
    attempt*: int   ## wall-clock bound on a SINGLE attempt (connect + this try's
                    ## reads, including its redirect hops), separate from `total`.
                    ## The effective per-attempt budget is `min(attempt, remaining
                    ## total)`. Unlike a `total` expiry (terminal), an `attempt`
                    ## expiry is retryable under the normal policy, so a slow attempt
                    ## can be abandoned and re-tried against a healthier connection
                    ## while `total` still caps the whole request. 0 disables.
    h2KeepAlive*: int  ## HTTP/2 PING keepalive interval (ms) for a connection with
                       ## active streams: after this long idle, PING the peer; if a
                       ## second interval passes with no inbound frame at all, treat the
                       ## connection as dead and close it (so wedged streams fail over
                       ## instead of hanging). Any inbound frame -- not just a PING ACK --
                       ## proves liveness, matching a live transport rather than a
                       ## responsive application. 0 disables. asyncdispatch and chronos
                       ## only (the sync backend has no background reader to drive it).

  NaviConfigBase* = object of RootObj
    ## Backend-agnostic client defaults, applied to every request and inheritable
    ## via `.extend`. Each entry module derives its own `NaviConfig` from this,
    ## adding a backend-specific `middleware` field. The derived `NaviConfig` has
    ## `{.requiresInit.}`, so it can only be built with `initNaviConfig` (a bare or
    ## partial `NaviConfig(...)` is a compile error), keeping the defaults intact.
    prefixUrl*: string
    headers*: Headers
    http*: set[HttpVersion]
    tls*: TlsConfig
    decompress*: bool               ## decode gzip/deflate bodies (default on)
    throwHttpErrors*: bool          ## raise HttpError on non-2xx (default on)
    maxRedirects*: int              ## redirects to follow, 0 disables (default 20)
    retry*: RetryPolicy             ## retry policy for transient failures
    maxResponseBytes*: int          ## cap on response body size; 0 (default) unlimited
    expectContinueMs*: int          ## opt-in `Expect: 100-continue` gate for HTTP/1.1
                                    ## requests that carry a body: how long to wait for
                                    ## the server's interim 100 before sending the body,
                                    ## in ms. 0 (default) disables it entirely (no
                                    ## `Expect` header is sent). HTTP/1.1 only: h2, h3
                                    ## and `navi/js` never send the header.
    auth*: Auth                     ## Authorization applied to every request
    proxy*: string                  ## proxy URL; "" falls back to env vars
    unixSocket*: string             ## connect over this Unix socket path instead of
                                    ## TCP; the URL host/port are used only for the
                                    ## Host header and TLS SNI. "" (default) uses TCP.
                                    ## Bypasses proxies; POSIX + native backends only.
    maxIdleConns*: int              ## global cap on idle pooled connections; 0 = unlimited
    maxIdleConnsPerHost*: int       ## idle pooled connections per origin; 0 = default (8)
    idleConnTimeout*: int           ## ms an idle pooled connection may live before it is
                                    ## evicted and closed; 0 (default) = no timeout
    timeouts*: Timeouts             ## per-phase deadlines (connect/read/total)
    resolvedProxy*: ResolvedProxy   ## proxy config resolved once at construction
                                    ## (env reads + URL parse + NO_PROXY split);
                                    ## nil until `newNavi`/`extend` build it. Set by
                                    ## the client, not the caller; see core/proxy.nim.

  BodyProducer* = proc(): string {.closure, raises: [CatchableError].}
    ## Pull-based upload source: returns the next chunk, or "" at end of body.
    ##
    ## The download sink type (`BodySink`) is defined per backend, since it is
    ## awaitable on the async backends (`proc(data: string): Future[void]`) and a
    ## plain `proc(data: string)` on the sync backend -- both take navi's native
    ## body type, so each chunk is moved to the sink with no copy. The js backend
    ## takes `seq[byte]` instead (its bytes come from a JS Uint8Array).

  BodyIterator* = iterator (): string {.closure, raises: [CatchableError].}
    ## Closure-iterator upload source. Unlike `BodyProducer`, end of body is
    ## `finished(it)` (not a "" yield), so an empty chunk in the middle of the
    ## stream cannot truncate the upload. `toBody` wraps it into a `BodyProducer`
    ## (see the `BodyIterator` overload).

  Request* = object
    verb*: HttpVerb
    url*: Url
    headers*: Headers
    trailers*: Headers          ## trailing fields sent after the body. Requires a
                                ## body framed to carry them: chunked transfer-encoding
                                ## (h1) or a trailing HEADERS block (h2/h3). Empty by
                                ## default. Not available on navi/js (fetch cannot send
                                ## request trailers).
    body*: string
    bodyStream*: BodyProducer  ## when set, the body is streamed chunked
    hasStreamedBody*: bool      ## true when the body is streamed and non-replayable:
                                ## a `bodyStream` producer, or an async producer
                                ## threaded outside the request (its type lives at the
                                ## backend layer, so it cannot be a field; see
                                ## `AsyncBodyProducer` per async backend). The
                                ## retry/redirect/digest guards read this instead of
                                ## `bodyStream` alone, so a non-rewindable async upload
                                ## is treated as non-replayable too. Set by the request
                                ## builder / async `requestResolved`.
    absoluteForm*: bool         ## use absolute-URI on the request line (http proxy)
    expectContinueMs*: int      ## copied from `NaviConfigBase.expectContinueMs` by
                                ## `buildRequest`, so the h1 send path sees it whichever
                                ## body arm is in play (buffered, `bodyStream`, or an
                                ## async producer). 0 (default) = no `Expect` gate.
    deadlineMs*: int            ## per-attempt connect/total budget override, in ms; 0
                                ## means "use config.timeouts.total". The sync and batch
                                ## retry loops set this to the REMAINING whole-request
                                ## budget before each attempt so a retried attempt does
                                ## not get a fresh `totalMs` (issue #359). The async
                                ## backends ignore it (their outer `guard` bounds the
                                ## whole request); their connect already `discard`s the
                                ## total deadline.

proc carriesBody*(req: Request): bool =
  ## Whether this request will put content on the wire: a buffered body, a sync
  ## `bodyStream` producer, or an async producer (flagged `hasStreamedBody`). The
  ## `Expect: 100-continue` gate consults it, since RFC 9110 10.1.1 only defines the
  ## expectation for a request that actually has content -- sending it on a bodyless
  ## request would make the server wait for a body that never comes.
  req.body.len > 0 or req.bodyStream != nil or req.hasStreamedBody

proc defaultRetryPolicy*(): RetryPolicy =
  ## Retry idempotent methods up to twice on transient statuses, backing off
  ## exponentially up to 10s. `initNaviConfig` uses this; override `config.retry`
  ## (or its fields) to change the count, methods, statuses, or max delay.
  RetryPolicy(
    limit: 2,
    methods: {GET, HEAD, PUT, DELETE, OPTIONS},
    statuses: @[408, 413, 429, 500, 502, 503, 504],
    maxDelay: 10_000)

# Readers take the base by value; a derived NaviConfig slices to it cleanly.
proc wantsDecompress*(opts: NaviConfigBase): bool = opts.decompress
proc wantsThrow*(opts: NaviConfigBase): bool = opts.throwHttpErrors
proc redirectLimit*(opts: NaviConfigBase): int = opts.maxRedirects
proc retryLimit*(opts: NaviConfigBase): int = opts.retry.limit

proc unixSocket*(opts: NaviConfigBase): string = opts.unixSocket
  ## Unix socket path to dial instead of TCP; "" (default) uses TCP.

proc idlePerHost*(opts: NaviConfigBase): int =
  ## Per-origin idle-connection cap; 0 in the config means the default of 8.
  if opts.maxIdleConnsPerHost > 0: opts.maxIdleConnsPerHost else: 8
proc idleGlobal*(opts: NaviConfigBase): int = opts.maxIdleConns
  ## Global idle-connection cap; 0 = unlimited.
proc idleTimeoutMs*(opts: NaviConfigBase): int = opts.idleConnTimeout
  ## Idle-connection lifetime in ms; 0 = no timeout.

proc connectMs*(opts: NaviConfigBase): int = opts.timeouts.connect
  ## Deadline for TCP connect + TLS handshake, in ms; 0 disables.
proc readMs*(opts: NaviConfigBase): int = opts.timeouts.read
  ## Per-read stall deadline while waiting for a response chunk, in ms; 0 disables.
proc totalMs*(opts: NaviConfigBase): int = opts.timeouts.total
  ## Overall request deadline in ms, including retries/redirects; 0 disables.
proc attemptMs*(opts: NaviConfigBase): int = opts.timeouts.attempt
  ## Per-attempt wall-clock deadline in ms (see `Timeouts.attempt`); 0 disables.
proc totalMsFor*(opts: NaviConfigBase, req: Request): int =
  ## The total/connect budget to use for this attempt: the request's per-attempt
  ## `deadlineMs` override when set (the remaining whole-request budget threaded by
  ## the sync/batch retry loop, issue #359), else the config's `total`. Only the
  ## sync backend acts on this at connect; the async backends bound the whole
  ## request with their outer `guard` instead.
  if req.deadlineMs > 0: req.deadlineMs else: opts.timeouts.total
proc expectContinueMs*(opts: NaviConfigBase): int = opts.expectContinueMs
  ## How long to wait for an interim `100 Continue` before sending an HTTP/1.1
  ## request body, in ms; 0 (the default) disables the `Expect: 100-continue` gate.
proc h2KeepAliveMs*(opts: NaviConfigBase): int = opts.timeouts.h2KeepAlive
  ## HTTP/2 PING keepalive interval in ms for a connection with active streams; 0
  ## disables. Detects a dead/wedged connection (no PONG) so its streams fail over.

const defaultH2KeepAliveMs* = 20_000
  ## The keepalive interval the async backends seed into `Timeouts.h2KeepAlive`.
  ## A dead connection is failed over after two to three intervals (a PING goes out
  ## one idle interval after the last frame, and death is declared the next), so
  ## ~40-60s at this default.

const defaultHttpVersions* = when defined(naviHttp3): {H1, H2, H3} else: {H1, H2}
  ## The default `config.http`: every protocol this build can negotiate (h3 only
  ## in a `-d:naviHttp3` build). Because it lists all of them, strict selection
  ## (`enforceProtocol`) never raises for a default client -- it only bites once a
  ## caller narrows the set.

proc wantsH2*(opts: NaviConfigBase): bool =
  ## An unset `http` (empty set) means "negotiate h2 where possible". Also true for
  ## an h3 request that allows no other bootstrap protocol ({H3} alone): h3 is
  ## discovered via Alt-Svc on a prior h1/h2 response, and h2 is the better discovery
  ## leg (more origins advertise `alt-svc` over h2, and it's faster than h1). When h1
  ## is explicitly allowed (e.g. {H1, H3}) the caller opted into h1, so h2 is not
  ## forced for them.
  opts.http.card == 0 or H2 in opts.http or
    (H3 in opts.http and H1 notin opts.http)

proc wantsH3*(opts: NaviConfigBase): bool =
  ## H3 is opt-in: it must be listed explicitly (an empty `http` set does not
  ## imply it, unlike h2) and is only honored in a `-d:naviHttp3` build. The
  ## native transport upgrades to h3 per origin after Alt-Svc discovery.
  H3 in opts.http

proc protocolAllowed*(http: set[HttpVersion], httpVersion: string): bool =
  ## Whether the HTTP version actually used (`httpVersion`, as it appears on
  ## `Response.httpVersion`) is permitted by the requested `http` set. An empty set
  ## allows anything. The one exemption is the h3 Alt-Svc discovery leg: when h3 is
  ## the *only* requested protocol, an h1/h2 bootstrap is required to discover it, so
  ## that leg is permitted (the upgrade to h3 happens on the following requests).
  ## Factored out of `enforceProtocol` so the sink gate can consult the same rule
  ## when deciding whether a response is certain to be surfaced (a protocol-rejected
  ## response is thrown, so its body must not reach the sink).
  if http.card == 0: return true
  let used =
    if httpVersion.startsWith("HTTP/3"): H3
    elif httpVersion.startsWith("HTTP/2"): H2
    else: H1
  if used in http: return true
  if http == {H3} and used in {H1, H2}: return true
  false

proc enforceProtocol*(opts: NaviConfigBase, httpVersion: string) =
  ## Strict protocol selection: the HTTP version actually used must be allowed by
  ## `opts.http`, else raise `ProtocolError`. Delegates the rule to `protocolAllowed`.
  if protocolAllowed(opts.http, httpVersion): return
  let used =
    if httpVersion.startsWith("HTTP/3"): H3
    elif httpVersion.startsWith("HTTP/2"): H2
    else: H1
  raise newException(ProtocolError,
    "navi: negotiated " & $used & ", which config.http (" & $opts.http &
    ") does not allow; widen config.http or accept the downgrade")

proc mergeBase*[T: NaviConfigBase](base, overrides: T): T =
  ## Layer `overrides`' addressing/identity fields over `base` for `.extend`,
  ## preserving `base`'s policy knobs and derived fields (e.g. middleware). The
  ## override is a full `initNaviConfig`; only fields with a natural "unset" value
  ## (prefixUrl, headers, http, auth, proxy) take effect, so its defaults for the
  ## other fields do not clobber `base`.
  result = base
  if overrides.prefixUrl.len > 0: result.prefixUrl = overrides.prefixUrl
  result.headers = merge(base.headers, overrides.headers)
  if overrides.http.card > 0: result.http = overrides.http
  if overrides.auth.kind != akNone: result.auth = overrides.auth
  if overrides.proxy.len > 0: result.proxy = overrides.proxy

proc toQuery*(pairs: openArray[(string, string)]): seq[(string, string)] = @pairs
  ## Query params from a seq or array literal (incl. `@[...]`, `@{...}`, `{...}`).
proc toQuery*(t: OrderedTable[string, string]): seq[(string, string)] =
  ## Query params from an ordered table (insertion order preserved).
  for k, v in t: result.add (k, v)
proc toQuery*(t: Table[string, string]): seq[(string, string)] =
  ## Query params from a table. A `Table`'s iteration order is unspecified, so
  ## use an `OrderedTable` or the pairs / `@{}` form when query order matters.
  for k, v in t: result.add (k, v)

proc validateRequest*(req: Request) =
  ## Reject CR, LF, or NUL in the target host or any header name/value. On
  ## HTTP/1.1 such a character would let an attacker-influenced header value (or a
  ## crafted redirect Location whose host carries CRLF) split the request into
  ## extra headers or a smuggled request; on h2/h3 the field is simply invalid.
  ## Called on every dispatch, so both the initial request and each redirect hop
  ## are checked. The byte test itself is the shared `hasCtlChars` (headers.nim),
  ## which the WebSocket handshake applies to its own field set.
  if hasCtlChars(req.url.host):
    raise newException(ValueError,
      "navi: invalid request host (contains CR, LF, or NUL)")
  # The path/query are written raw onto the HTTP/1.1 request line (h1.serializeHead),
  # so a CR/LF there splits the request line and injects headers just like a header
  # value does. std/uri passes these through verbatim, so guard them here too (#274).
  if hasCtlChars(req.url.requestTarget):
    raise newException(ValueError,
      "navi: invalid request target (path or query contains CR, LF, or NUL)")
  for (k, v) in req.headers.pairs:
    if hasCtlChars(k) or hasCtlChars(v):
      raise newException(ValueError,
        "navi: invalid header '" & k & "' (name or value contains CR, LF, or NUL)")
  for (k, v) in req.trailers.pairs:
    if hasCtlChars(k) or hasCtlChars(v):
      raise newException(ValueError,
        "navi: invalid trailer '" & k & "' (name or value contains CR, LF, or NUL)")

type
  ResolvedBody* = object
    ## A body argument resolved by `toBody` into what the wire needs. Produced by
    ## the `toBody` overloads (one per accepted body type) and consumed by
    ## `resolveBody`/`buildRequest`. The default `ResolvedBody()` is the "no typed
    ## body" case (`typed == false`, empty content): the caller's raw string or
    ## `form` argument governs the body instead.
    typed*: bool           ## a typed body arm matched (json/multipart/stream/
                           ## iterator/catch-all); outranks `form` and raw string
    content*: string       ## the buffered body bytes (empty for a streamed body)
    contentType*: string   ## Content-Type the body implies; "" keeps the caller's
    stream*: BodyProducer  ## set for a streamed (chunked) upload; nil otherwise

proc toBody*(s: string): ResolvedBody =
  ## Raw string body: not a typed body, so `form` still outranks it. Content is the
  ## string verbatim, with no implied Content-Type.
  ResolvedBody(typed: false, content: s)

proc toBody*(json: JsonNode): ResolvedBody =
  ## JSON body: `$json` with an application/json Content-Type. A nil node is "no
  ## body", so it does not outrank `form` (returns the default `ResolvedBody`).
  if json == nil: ResolvedBody()
  else: ResolvedBody(typed: true, content: $json, contentType: "application/json")

proc toBody*(multipart: Multipart): ResolvedBody =
  ## multipart/form-data body. An empty part list is "no body" (returns the default
  ## `ResolvedBody`); otherwise `encodeMultipart` supplies the body and the
  ## boundary-carrying Content-Type.
  if multipart.len == 0: ResolvedBody()
  else:
    let (body, contentType) = encodeMultipart(multipart)
    ResolvedBody(typed: true, content: body, contentType: contentType)

proc toBody*(stream: BodyProducer): ResolvedBody =
  ## Streamed (chunked) upload from a pull-based producer. A nil producer is "no
  ## body" (returns the default `ResolvedBody`); otherwise the producer is streamed
  ## with no implied Content-Type.
  if stream == nil: ResolvedBody()
  else: ResolvedBody(typed: true, stream: stream)

proc toBody*(it: BodyIterator): ResolvedBody =
  ## Streamed upload from a closure iterator. A nil iterator is "no body" (returns
  ## the default `ResolvedBody`). Otherwise the iterator is wrapped into a
  ## `BodyProducer` whose end-of-body is `finished(it)`, not a "" yield: an empty
  ## chunk yielded mid-stream is skipped rather than treated as end (so it cannot
  ## truncate the upload), and once the iterator is finished the producer keeps
  ## returning "".
  if it == nil: return ResolvedBody()
  let producer: BodyProducer = proc(): string {.closure, raises: [CatchableError].} =
    while true:
      let chunk = it()
      if finished(it): return ""    # true end of body; stays "" hereafter
      if chunk.len == 0: continue    # skip an empty mid-stream yield
      return chunk
  ResolvedBody(typed: true, stream: producer)

proc toBody*(body: ResolvedBody): ResolvedBody = body
  ## Pass-through: an already-resolved body is returned unchanged.

proc toBody*[T: not proc](body: T): ResolvedBody =
  ## Catch-all for any other type: serialize with `std/jsonutils.toJson` and send
  ## as application/json. The `not proc` constraint keeps a raw lambda from ranking
  ## into this generic overload (a generic match beats a convertible one) instead
  ## of the `BodyProducer` overload. A bare `nil` literal would also rank here
  ## ahead of the ref/proc overloads; reject it, since its intent is ambiguous
  ## (omit `body` for no body, or pass a typed nil like `JsonNode(nil)`).
  when body is typeof(nil):
    {.error: "body = nil is ambiguous; omit `body` instead".}
  ResolvedBody(typed: true, content: $toJson(body), contentType: "application/json")

proc resolveBody(body: ResolvedBody, form: seq[(string, string)]): ResolvedBody =
  ## Resolve the body arguments into the winning source, normalized: a concrete
  ## body string (or stream) plus the Content-Type it implies ("" means the
  ## caller keeps any existing header). Precedence, highest first: a typed `body`
  ## (json/multipart/stream/iterator/catch-all, as chosen by `toBody`), then
  ## `form`, then the raw string `body`. Single place the body-source precedence
  ## lives, so it stays consistent and testable as body kinds are added. Returns
  ## a `ResolvedBody`, not a tuple: the js codegen cannot represent a nil closure
  ## inside a tuple (it emits `null.bind(null)`), while a nil object field is fine.
  if body.typed:
    body
  elif form.len > 0:
    ResolvedBody(content: encodeQuery(form),
                 contentType: "application/x-www-form-urlencoded")
  else:
    ResolvedBody(content: body.content)

proc buildRequest*(opts: NaviConfigBase, verb: HttpVerb, target: string,
                   headers: Headers = initHeaders(),
                   body: ResolvedBody = ResolvedBody(),
                   form: seq[(string, string)] = @[],
                   params: seq[(string, string)] = @[],
                   trailers: Headers = initHeaders()): Request =
  ## Resolve `target` against the client's prefixUrl, merge headers, and encode
  ## the body. A typed `body` (produced by `toBody` from json/multipart/a producer/
  ## an iterator/any other value) takes precedence over `form`, which takes
  ## precedence over a raw string `body`; the winner sets a matching Content-Type
  ## unless the caller supplied one. `params` are appended to the resolved URL's
  ## query string (url-encoded). `trailers` are sent after the body (chunked on h1,
  ## a trailing HEADERS block on h2/h3); they are per-request and not merged with
  ## the client's default headers.
  result.verb = verb
  result.url = join(opts.prefixUrl, target)
  if params.len > 0:
    result.url = result.url.withQuery(params)
  result.headers = merge(opts.headers, headers)
  result.trailers = trailers
  let resolved = resolveBody(body, form)
  result.body = resolved.content
  result.bodyStream = resolved.stream
  result.expectContinueMs = opts.expectContinueMs
  if resolved.stream != nil:
    result.hasStreamedBody = true   # a sync producer is non-replayable (see isReplayable)
  if resolved.contentType.len > 0 and not result.headers.contains("content-type"):
    result.headers.add("content-type", resolved.contentType)
  # Digest can't be precomputed (it needs the server's nonce), so its header is
  # empty here and added by the engine after the 401 challenge.
  if opts.auth.header.len > 0 and not result.headers.contains("authorization"):
    result.headers.add("authorization", opts.auth.header)
  if opts.wantsDecompress and not result.headers.contains("accept-encoding"):
    result.headers.add("accept-encoding", "gzip, deflate, br, zstd")
  # Identify the client and advertise a catch-all Accept unless the caller set
  # their own. Every mainstream client (Go, curl, axios, httpx) sends both; some
  # servers and WAFs reject or misroute a User-Agent-less request.
  if not result.headers.contains("user-agent"):
    result.headers.add("user-agent", "navi/" & naviVersion)
  if not result.headers.contains("accept"):
    result.headers.add("accept", "*/*")
