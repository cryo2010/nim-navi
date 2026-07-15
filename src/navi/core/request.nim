## Request model, client options, and the pure request-building pipeline.
##
## Nothing here performs I/O: `buildRequest` merges instance defaults with
## per-call arguments into a concrete `Request` that any backend can execute.

import std/[options, json, base64]
from std/uri import encodeQuery
import ./headers, ./url, ./response
import ../backend/api
export options

type
  AuthKind* = enum akNone, akBasic, akBearer
  Auth* = object
    case kind*: AuthKind
    of akBasic:
      user*, pass*: string
    of akBearer:
      token*: string
    of akNone: discard

proc basicAuth*(user, pass: string): Auth =
  Auth(kind: akBasic, user: user, pass: pass)
proc bearerAuth*(token: string): Auth =
  Auth(kind: akBearer, token: token)

proc header(a: Auth): string =
  case a.kind
  of akBasic: "Basic " & encode(a.user & ":" & a.pass)
  of akBearer: "Bearer " & a.token
  of akNone: ""

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

  NaviOptionsBase* = object of RootObj
    ## Backend-agnostic client defaults, applied to every request and inheritable
    ## via `.extend`. Each entry module derives its own `NaviOptions` from this,
    ## adding a backend-specific `hooks` field.
    prefixUrl*: string
    headers*: Headers
    http*: set[HttpVersion]
    tls*: TlsConfig
    decompress*: Option[bool]      ## decode gzip/deflate bodies (default on)
    throwHttpErrors*: Option[bool]  ## raise HttpError on non-2xx (default on)
    maxRedirects*: Option[int]      ## redirects to follow, 0 disables (default 20)
    maxRetries*: Option[int]        ## retry attempts for transient failures (default 2)
    auth*: Auth                     ## Authorization applied to every request
    proxy*: Option[string]          ## proxy URL; none falls back to env vars
    timeout*: Option[int]           ## request timeout in ms; none (default) disables

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

  HookCtx* = ref object
    ## Mutable context passed to lifecycle hooks. A ref so async hooks can hold
    ## it across an `await` and still mutate it (a `var` param cannot be
    ## captured). beforeRequest edits `request`; afterResponse edits `response`;
    ## beforeRetry sees `attempt`.
    request*: Request
    response*: Response
    attempt*: int

# Readers take the base by value; a derived NaviOptions slices to it cleanly.
proc wantsDecompress*(opts: NaviOptionsBase): bool = opts.decompress.get(true)
proc wantsThrow*(opts: NaviOptionsBase): bool = opts.throwHttpErrors.get(true)
proc redirectLimit*(opts: NaviOptionsBase): int = opts.maxRedirects.get(20)
proc retryLimit*(opts: NaviOptionsBase): int = opts.maxRetries.get(2)
proc timeoutMs*(opts: NaviOptionsBase): int = opts.timeout.get(0)
  ## Request timeout in milliseconds; 0 means no timeout.
proc wantsH2*(opts: NaviOptionsBase): bool =
  ## An unset `http` (empty set) means "negotiate h2 where possible".
  opts.http.card == 0 or H2 in opts.http

proc mergeBase*[T: NaviOptionsBase](base, overrides: T): T =
  ## Layer `overrides`' base fields over `base` for `.extend`, preserving any
  ## derived fields (e.g. hooks) from `base`. Generic so it returns the derived
  ## type. Only fields the caller set take effect.
  result = base
  if overrides.prefixUrl.len > 0: result.prefixUrl = overrides.prefixUrl
  result.headers = merge(base.headers, overrides.headers)
  if overrides.http.card > 0: result.http = overrides.http
  if overrides.decompress.isSome: result.decompress = overrides.decompress
  if overrides.throwHttpErrors.isSome:
    result.throwHttpErrors = overrides.throwHttpErrors
  if overrides.maxRedirects.isSome: result.maxRedirects = overrides.maxRedirects
  if overrides.maxRetries.isSome: result.maxRetries = overrides.maxRetries
  if overrides.auth.kind != akNone: result.auth = overrides.auth
  if overrides.proxy.isSome: result.proxy = overrides.proxy

proc buildRequest*(opts: NaviOptionsBase, verb: HttpVerb, target: string,
                   headers: Headers = initHeaders(), body = "",
                   json: JsonNode = nil, form: seq[(string, string)] = @[],
                   bodyStream: BodyProducer = nil): Request =
  ## Resolve `target` against the client's prefixUrl, merge headers, and encode
  ## the body. `json` and `form` take precedence over `body` and set a matching
  ## Content-Type unless the caller supplied one.
  result.verb = verb
  result.url = join(opts.prefixUrl, target)
  result.headers = merge(opts.headers, headers)
  result.bodyStream = bodyStream
  if json != nil:
    result.body = $json
    if not result.headers.contains("content-type"):
      result.headers.add("content-type", "application/json")
  elif form.len > 0:
    result.body = encodeQuery(form)
    if not result.headers.contains("content-type"):
      result.headers.add("content-type", "application/x-www-form-urlencoded")
  else:
    result.body = body
  if opts.auth.kind != akNone and not result.headers.contains("authorization"):
    result.headers.add("authorization", opts.auth.header)
  if opts.wantsDecompress and not result.headers.contains("accept-encoding"):
    result.headers.add("accept-encoding", "gzip, deflate")
