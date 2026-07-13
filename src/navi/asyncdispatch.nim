## navi — asyncdispatch entry point.
##
##   import navi/asyncdispatch
##   let api = newNavi()
##   let res = await api.get("http://example.com")

import std/[asyncdispatch, asyncnet]
import navi/private/entryguard
import navi/core/public
import navi/proto/h1

claimEntry("navi/asyncdispatch")
export public, asyncdispatch

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
  let sock = await asyncnet.dial(req.url.host, Port(req.url.port))
  defer: sock.close()
  await sock.send(serializeRequest(req))
  var parser = initH1Parser()
  while not parser.finished:
    let chunk = await sock.recv(4096)
    if chunk.len == 0:
      parser.eof()
      break
    parser.feed(chunk)
  return parser.toResponse()

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
