## Request model, client options, and the pure request-building pipeline.
##
## Nothing here performs I/O: `buildRequest` merges instance defaults with
## per-call arguments into a concrete `Request` that any backend can execute.

import std/options
import ./headers, ./url
import ../backend/api
export options

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

proc buildRequest*(opts: NaviOptions, verb: HttpVerb, target: string,
                   headers: Headers = initHeaders(), body = "",
                   bodyStream: BodyProducer = nil): Request =
  ## Resolve `target` against the client's prefixUrl and merge headers.
  result.verb = verb
  result.url = join(opts.prefixUrl, target)
  result.headers = merge(opts.headers, headers)
  result.body = body
  result.bodyStream = bodyStream
  if opts.wantsDecompress and not result.headers.contains("accept-encoding"):
    result.headers.add("accept-encoding", "gzip, deflate")
