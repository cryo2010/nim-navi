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
    jar*: CookieJar

proc newNavi*(options = defaultOptions()): Navi =
  ## Create a client. `options` supplies defaults (prefixUrl, headers, TLS, …).
  Navi(options: options, pool: newPool[Conn](), jar: newCookieJar())

proc extend*(client: Navi, options: NaviOptions): Navi =
  ## Derive a new client, layering `options` over this client's defaults.
  ## The derived client gets its own connection pool and cookie jar.
  Navi(options: mergeOptions(client.options, options),
       pool: newPool[Conn](), jar: newCookieJar())

proc request*(client: Navi, verb: HttpVerb, target: string,
              headers = initHeaders(), body = "", json: JsonNode = nil,
              form: seq[(string, string)] = @[],
              bodyStream: BodyProducer = nil): Response =
  ## Perform a request and return the response. `json`/`form` encode the body;
  ## `bodyStream` uploads a chunked body from a pull-based producer.
  let req = buildRequest(client.options, verb, target, headers, body, json,
                         form, bodyStream)
  performRequest(client, req)

proc stream*(client: Navi, verb: HttpVerb, target: string, sink: BodySink,
             headers = initHeaders()): Response =
  ## Perform a request and deliver the response body to `sink` as it arrives.
  ## The returned Response carries status and headers but an empty body.
  let req = buildRequest(client.options, verb, target, headers)
  performStream(client, req, sink)

include navi/private/verbs
