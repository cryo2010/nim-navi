## Request model, client options, and the pure request-building pipeline.
##
## Nothing here performs I/O: `buildRequest` merges instance defaults with
## per-call arguments into a concrete `Request` that any backend can execute.

import std/[options, json, base64]
from std/uri import encodeQuery
import ./headers, ./url
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

  NaviOptions* = object
    ## Client-level defaults, applied to every request and inheritable via
    ## `.extend`.
    prefixUrl*: string
    headers*: Headers
    http*: set[HttpVersion]
    tls*: TlsConfig
    decompress*: Option[bool]      ## decode gzip/deflate bodies (default on)
    throwHttpErrors*: Option[bool]  ## raise HttpError on non-2xx (default on)
    maxRedirects*: Option[int]      ## redirects to follow, 0 disables (default 20)
    auth*: Auth                     ## Authorization applied to every request

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

proc defaultOptions*(): NaviOptions =
  result.http = {H1} # protocol negotiation lands with ALPN in phase 4
  result.tls = defaultTls()

proc wantsDecompress*(opts: NaviOptions): bool = opts.decompress.get(true)
proc wantsThrow*(opts: NaviOptions): bool = opts.throwHttpErrors.get(true)
proc redirectLimit*(opts: NaviOptions): int = opts.maxRedirects.get(20)

proc mergeOptions*(base, overrides: NaviOptions): NaviOptions =
  ## Layer `overrides` over `base` for `.extend`. Only fields the caller set
  ## take effect; the rest are inherited.
  result = base
  if overrides.prefixUrl.len > 0: result.prefixUrl = overrides.prefixUrl
  result.headers = merge(base.headers, overrides.headers)
  if overrides.http.card > 0: result.http = overrides.http
  if overrides.decompress.isSome: result.decompress = overrides.decompress
  if overrides.throwHttpErrors.isSome:
    result.throwHttpErrors = overrides.throwHttpErrors
  if overrides.maxRedirects.isSome: result.maxRedirects = overrides.maxRedirects
  if overrides.auth.kind != akNone: result.auth = overrides.auth

proc buildRequest*(opts: NaviOptions, verb: HttpVerb, target: string,
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
