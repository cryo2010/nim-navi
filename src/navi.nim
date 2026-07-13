## navi — synchronous entry point.
##
##   import navi
##   let api = newNavi()
##   let res = api.get("http://example.com")
##   echo res.status, " ", res.text()
##
## For async, import `navi/asyncdispatch` or `navi/chronos` instead (exactly
## one entry module per program).

import navi/private/entryguard
import navi/core/public
import navi/core/[engine, pool]
import navi/backend/sync

claimEntry("navi")
export public

type
  Navi* = object
    options*: NaviOptions
    pool*: Pool[Conn]

proc newNavi*(options = defaultOptions()): Navi =
  ## Create a client. `options` supplies defaults (prefixUrl, headers, TLS, …).
  Navi(options: options, pool: newPool[Conn]())

proc extend*(client: Navi, options: NaviOptions): Navi =
  ## Derive a new client, layering `options` over this client's defaults.
  Navi(options: mergeOptions(client.options, options), pool: newPool[Conn]())

proc request*(client: Navi, verb: HttpVerb, target: string,
              headers = initHeaders(), body = "",
              bodyStream: BodyProducer = nil): Response =
  ## Perform a request and return the response. Pass `bodyStream` to upload a
  ## chunked body from a pull-based producer.
  let req = buildRequest(client.options, verb, target, headers, body, bodyStream)
  performRequest(client, req)

proc stream*(client: Navi, verb: HttpVerb, target: string, sink: BodySink,
             headers = initHeaders()): Response =
  ## Perform a request and deliver the response body to `sink` as it arrives.
  ## The returned Response carries status and headers but an empty body.
  let req = buildRequest(client.options, verb, target, headers)
  performStream(client, req, sink)

proc get*(client: Navi, target: string, headers = initHeaders()): Response =
  client.request(GET, target, headers)
proc post*(client: Navi, target: string, body = "", headers = initHeaders()): Response =
  client.request(POST, target, headers, body)
proc put*(client: Navi, target: string, body = "", headers = initHeaders()): Response =
  client.request(PUT, target, headers, body)
proc patch*(client: Navi, target: string, body = "", headers = initHeaders()): Response =
  client.request(PATCH, target, headers, body)
proc delete*(client: Navi, target: string, headers = initHeaders()): Response =
  client.request(DELETE, target, headers)
proc head*(client: Navi, target: string, headers = initHeaders()): Response =
  client.request(HEAD, target, headers)
