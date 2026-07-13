## navi — chronos entry point.
##
##   import navi/chronos
##   let api = newNavi()
##   let res = await api.get("http://example.com")
##
## Requires the `chronos` package. Only compiled when this module is imported,
## so sync/asyncdispatch users never pull chronos in.

import pkg/chronos, pkg/chronos/transports/stream
import navi/private/entryguard
import navi/core/public
import navi/proto/h1

claimEntry("navi/chronos")
export public, chronos

type
  Navi* = object
    options*: NaviOptions

proc newNavi*(options = defaultOptions()): Navi =
  Navi(options: options)

proc extend*(client: Navi, options: NaviOptions): Navi =
  var merged = client.options
  if options.prefixUrl.len > 0: merged.prefixUrl = options.prefixUrl
  merged.headers = merge(client.options.headers, options.headers)
  if options.http.card > 0: merged.http = options.http
  Navi(options: merged)

proc request*(client: Navi, verb: HttpVerb, target: string,
              headers = initHeaders(), body = ""): Future[Response] {.async.} =
  let req = buildRequest(client.options, verb, target, headers, body)
  let addresses = resolveTAddress(req.url.host, Port(req.url.port))
  let tr = await connect(addresses[0])
  try:
    discard await tr.write(serializeRequest(req))
    var parser = initH1Parser()
    var buf = newString(4096)
    while not parser.finished:
      let n = await tr.readOnce(addr buf[0], buf.len)
      if n == 0:
        parser.eof()
        break
      parser.feed(buf.toOpenArray(0, n - 1))
    result = parser.toResponse()
  finally:
    await tr.closeWait()

proc get*(client: Navi, target: string, headers = initHeaders()): Future[Response] =
  client.request(GET, target, headers)
proc post*(client: Navi, target: string, body = "", headers = initHeaders()): Future[Response] =
  client.request(POST, target, headers, body)
proc put*(client: Navi, target: string, body = "", headers = initHeaders()): Future[Response] =
  client.request(PUT, target, headers, body)
proc patch*(client: Navi, target: string, body = "", headers = initHeaders()): Future[Response] =
  client.request(PATCH, target, headers, body)
proc delete*(client: Navi, target: string, headers = initHeaders()): Future[Response] =
  client.request(DELETE, target, headers)
proc head*(client: Navi, target: string, headers = initHeaders()): Future[Response] =
  client.request(HEAD, target, headers)
