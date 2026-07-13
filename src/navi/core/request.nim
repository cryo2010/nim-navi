## Request model, client options, and the pure request-building pipeline.
##
## Nothing here performs I/O: `buildRequest` merges instance defaults with
## per-call arguments into a concrete `Request` that any backend can execute.

import ./headers, ./url
import ../backend/api

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

  Request* = object
    verb*: HttpVerb
    url*: Url
    headers*: Headers
    body*: string

proc defaultOptions*(): NaviOptions =
  result.http = {H1} # protocol negotiation lands with ALPN in phase 4
  result.tls = defaultTls()

proc buildRequest*(opts: NaviOptions, verb: HttpVerb, target: string,
                   headers: Headers = initHeaders(), body = ""): Request =
  ## Resolve `target` against the client's prefixUrl and merge headers.
  result.verb = verb
  result.url = join(opts.prefixUrl, target)
  result.headers = merge(opts.headers, headers)
  result.body = body
