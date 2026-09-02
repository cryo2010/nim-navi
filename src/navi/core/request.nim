## Request model, client options, and the pure request-building pipeline.
##
## Nothing here performs I/O: `buildRequest` merges instance defaults with
## per-call arguments into a concrete `Request` that any backend can execute.

import std/[options, json, base64, tables, strutils]
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
                    ## and negotiated per origin via Alt-Svc (see docs/http3.md).

  RetryPolicy* = object
    ## When and how a request is retried. `initNaviConfig` seeds `defaultRetryPolicy`.
    limit*: int                     ## retry attempts, 0 disables (default 2)
    methods*: set[HttpVerb]         ## verbs eligible for retry (idempotent by default)
    statuses*: seq[int]             ## response statuses that trigger a retry
    maxDelay*: int                  ## upper bound on the wait between attempts, ms

  Timeouts* = object
    ## Per-phase deadlines in milliseconds; 0 (default) disables that phase's
    ## limit. `connect` and `read` are enforced on the native backends (sync,
    ## asyncdispatch, chronos); `total` on all four. On `navi/js` only `total` is
    ## enforceable (via `AbortSignal.timeout`), as `fetch` hides the phases.
    connect*: int   ## TCP connect + TLS handshake (establishment)
    read*: int      ## stall waiting for a response chunk (per-read idle)
    total*: int     ## whole request, including retries/redirects

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

  BodyProducer* = proc(): string {.closure, raises: [CatchableError].}
    ## Pull-based upload source: returns the next chunk, or "" at end of body.
    ##
    ## The download sink type (`BodySink`) is defined per backend, since it is
    ## awaitable on the async backends (`proc(data: string): Future[void]`) and a
    ## plain `proc(data: string)` on the sync backend -- both take navi's native
    ## body type, so each chunk is moved to the sink with no copy. The js backend
    ## takes `seq[byte]` instead (its bytes come from a JS Uint8Array).

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
    absoluteForm*: bool         ## use absolute-URI on the request line (http proxy)

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

proc enforceProtocol*(opts: NaviConfigBase, httpVersion: string) =
  ## Strict protocol selection: the HTTP version actually used (`httpVersion`, as
  ## it appears on `Response.httpVersion`) must be allowed by `opts.http`, else
  ## raise `ProtocolError`. An empty `http` set allows anything. The one exemption
  ## is the h3 Alt-Svc discovery leg: when h3 is the *only* requested protocol, an
  ## h1/h2 bootstrap is required to discover it, so that leg is permitted (the
  ## upgrade to h3 happens on the following requests).
  if opts.http.card == 0: return
  let used =
    if httpVersion.startsWith("HTTP/3"): H3
    elif httpVersion.startsWith("HTTP/2"): H2
    else: H1
  if used in opts.http: return
  if opts.http == {H3} and used in {H1, H2}: return
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
  ## are checked.
  proc hasCtl(s: string): bool =
    for c in s:
      if c in {'\r', '\n', '\0'}: return true
    false
  if hasCtl(req.url.host):
    raise newException(ValueError,
      "navi: invalid request host (contains CR, LF, or NUL)")
  for (k, v) in req.headers.pairs:
    if hasCtl(k) or hasCtl(v):
      raise newException(ValueError,
        "navi: invalid header '" & k & "' (name or value contains CR, LF, or NUL)")
  for (k, v) in req.trailers.pairs:
    if hasCtl(k) or hasCtl(v):
      raise newException(ValueError,
        "navi: invalid trailer '" & k & "' (name or value contains CR, LF, or NUL)")

proc buildRequest*(opts: NaviConfigBase, verb: HttpVerb, target: string,
                   headers: Headers = initHeaders(), body = "",
                   json: JsonNode = nil, form: seq[(string, string)] = @[],
                   multipart: Multipart = @[],
                   bodyStream: BodyProducer = nil,
                   params: seq[(string, string)] = @[],
                   trailers: Headers = initHeaders()): Request =
  ## Resolve `target` against the client's prefixUrl, merge headers, and encode
  ## the body. `json`, `form`, and `multipart` take precedence over `body` (in
  ## that order) and set a matching Content-Type unless the caller supplied one.
  ## `params` are appended to the resolved URL's query string (url-encoded).
  ## `trailers` are sent after the body (chunked on h1, a trailing HEADERS block on
  ## h2/h3); they are per-request and not merged with the client's default headers.
  result.verb = verb
  result.url = join(opts.prefixUrl, target)
  if params.len > 0:
    result.url = result.url.withQuery(params)
  result.headers = merge(opts.headers, headers)
  result.trailers = trailers
  result.bodyStream = bodyStream
  if json != nil:
    result.body = $json
    if not result.headers.contains("content-type"):
      result.headers.add("content-type", "application/json")
  elif multipart.len > 0:
    let (body, contentType) = encodeMultipart(multipart)
    result.body = body
    if not result.headers.contains("content-type"):
      result.headers.add("content-type", contentType)
  elif form.len > 0:
    result.body = encodeQuery(form)
    if not result.headers.contains("content-type"):
      result.headers.add("content-type", "application/x-www-form-urlencoded")
  else:
    result.body = body
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
