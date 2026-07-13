## navi — asyncdispatch entry point.
##
##   import navi/asyncdispatch
##   let api = newNavi()
##   let res = await api.get("http://example.com")

import navi/private/entryguard
import navi/core/public
import navi/core/[engine, pool]
import navi/backend/asyncdispatch

claimEntry("navi/asyncdispatch")
export public, asyncdispatch

type
  Navi* = object
    options*: NaviOptions
    pool*: Pool[Conn]

proc newNavi*(options = defaultOptions()): Navi =
  Navi(options: options, pool: newPool[Conn]())

proc extend*(client: Navi, options: NaviOptions): Navi =
  Navi(options: mergeOptions(client.options, options), pool: newPool[Conn]())

proc request*(client: Navi, verb: HttpVerb, target: string,
              headers = initHeaders(), body = "",
              bodyStream: BodyProducer = nil): Future[Response] {.async.} =
  let req = buildRequest(client.options, verb, target, headers, body, bodyStream)
  result = performRequest(client, req)

proc stream*(client: Navi, verb: HttpVerb, target: string, sink: BodySink,
             headers = initHeaders()): Future[Response] {.async.} =
  ## Deliver the response body to `sink` as it arrives; Response.body is empty.
  let req = buildRequest(client.options, verb, target, headers)
  result = performStream(client, req, sink)

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
