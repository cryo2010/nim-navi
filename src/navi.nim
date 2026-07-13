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
import navi/core/engine
import navi/backend/sync

claimEntry("navi")
export public

type
  Navi* = object
    options*: NaviOptions

proc newNavi*(options = defaultOptions()): Navi =
  ## Create a client. `options` supplies defaults (prefixUrl, headers, TLS, …).
  Navi(options: options)

proc extend*(client: Navi, options: NaviOptions): Navi =
  ## Derive a new client, layering `options` over this client's defaults.
  var merged = client.options
  if options.prefixUrl.len > 0: merged.prefixUrl = options.prefixUrl
  merged.headers = merge(client.options.headers, options.headers)
  if options.http.card > 0: merged.http = options.http
  Navi(options: merged)

proc request*(client: Navi, verb: HttpVerb, target: string,
              headers = initHeaders(), body = ""): Response =
  ## Perform a request and return the response.
  let req = buildRequest(client.options, verb, target, headers, body)
  performRequest(client, req)

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
