## Request model, client options, and the pure request-building pipeline.
##
## Nothing here performs I/O: `buildRequest` merges instance defaults with
## per-call arguments into a concrete `Request` that any backend can execute.

import std/[options, json, base64]
from std/uri import encodeQuery
import ./headers, ./url, ./response, ./multipart
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
    H3 = "HTTP/3"

  NaviConfigBase* = object of RootObj
    ## Backend-agnostic client defaults, applied to every request and inheritable
    ## via `.extend`. Each entry module derives its own `NaviConfig` from this,
    ## adding a backend-specific `middleware` field. Build one with `newNaviConfig`
    ## so the safe defaults are set; a bare `NaviConfig()` leaves fields zeroed
    ## (e.g. verification off), which is why `newNavi` defaults to `newNaviConfig()`.
    prefixUrl*: string
    headers*: Headers
    http*: set[HttpVersion]
    tls*: TlsConfig
    decompress*: bool               ## decode gzip/deflate bodies (default on)
    throwHttpErrors*: bool          ## raise HttpError on non-2xx (default on)
    maxRedirects*: int              ## redirects to follow, 0 disables (default 20)
    maxRetries*: int                ## retry attempts for transient failures (default 2)
    auth*: Auth                     ## Authorization applied to every request
    proxy*: string                  ## proxy URL; "" falls back to env vars
    timeout*: int                   ## request timeout in ms; 0 (default) disables

  BodyProducer* = proc(): string {.closure, raises: [CatchableError].}
    ## Pull-based upload source: returns the next chunk, or "" at end of body.
  BodySink* = proc(data: openArray[byte]) {.closure, raises: [CatchableError].}
    ## Download sink: receives response body chunks as they arrive.

  Request* = object
    verb*: HttpVerb
    url*: Url
    headers*: Headers
    body*: string
    bodyStream*: BodyProducer  ## when set, the body is streamed chunked
    absoluteForm*: bool         ## use absolute-URI on the request line (http proxy)

# Readers take the base by value; a derived NaviConfig slices to it cleanly.
proc wantsDecompress*(opts: NaviConfigBase): bool = opts.decompress
proc wantsThrow*(opts: NaviConfigBase): bool = opts.throwHttpErrors
proc redirectLimit*(opts: NaviConfigBase): int = opts.maxRedirects
proc retryLimit*(opts: NaviConfigBase): int = opts.maxRetries
proc timeoutMs*(opts: NaviConfigBase): int = opts.timeout
  ## Request timeout in milliseconds; 0 means no timeout.
proc wantsH2*(opts: NaviConfigBase): bool =
  ## An unset `http` (empty set) means "negotiate h2 where possible".
  opts.http.card == 0 or H2 in opts.http

proc withDefaults*[T: NaviConfigBase](cfg: var T) =
  ## Fill the backend-agnostic defaults on a freshly-constructed config. Each
  ## entry's `newNaviConfig` calls this, then sets its own backend specifics.
  cfg.tls = defaultTls()
  cfg.decompress = true
  cfg.throwHttpErrors = true
  cfg.maxRedirects = 20
  cfg.maxRetries = 2

proc mergeBase*[T: NaviConfigBase](base, overrides: T): T =
  ## Layer `overrides`' addressing/identity fields over `base` for `.extend`,
  ## preserving `base`'s policy knobs and derived fields (e.g. middleware). Only
  ## fields with a natural "unset" value take effect, so a sparse
  ## `NaviConfig(prefixUrl: ...)` override layers cleanly. To change the numeric
  ## or bool policy, build a full config with `newNaviConfig`.
  result = base
  if overrides.prefixUrl.len > 0: result.prefixUrl = overrides.prefixUrl
  result.headers = merge(base.headers, overrides.headers)
  if overrides.http.card > 0: result.http = overrides.http
  if overrides.auth.kind != akNone: result.auth = overrides.auth
  if overrides.proxy.len > 0: result.proxy = overrides.proxy

proc buildRequest*(opts: NaviConfigBase, verb: HttpVerb, target: string,
                   headers: Headers = initHeaders(), body = "",
                   json: JsonNode = nil, form: seq[(string, string)] = @[],
                   multipart: Multipart = @[],
                   bodyStream: BodyProducer = nil): Request =
  ## Resolve `target` against the client's prefixUrl, merge headers, and encode
  ## the body. `json`, `form`, and `multipart` take precedence over `body` (in
  ## that order) and set a matching Content-Type unless the caller supplied one.
  result.verb = verb
  result.url = join(opts.prefixUrl, target)
  result.headers = merge(opts.headers, headers)
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
