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
    jar*: CookieJar

proc newNavi*(options = defaultOptions()): Navi =
  Navi(options: options, pool: newPool[Conn](), jar: newCookieJar())

proc extend*(client: Navi, options: NaviOptions): Navi =
  Navi(options: mergeOptions(client.options, options),
       pool: newPool[Conn](), jar: newCookieJar())

proc request*(client: Navi, verb: HttpVerb, target: string,
              headers = initHeaders(), body = "", json: JsonNode = nil,
              form: seq[(string, string)] = @[],
              bodyStream: BodyProducer = nil): Future[Response] {.async.} =
  let req = buildRequest(client.options, verb, target, headers, body, json,
                         form, bodyStream)
  result = performRequest(client, req)

proc stream*(client: Navi, verb: HttpVerb, target: string, sink: BodySink,
             headers = initHeaders()): Future[Response] {.async.} =
  ## Deliver the response body to `sink` as it arrives; Response.body is empty.
  let req = buildRequest(client.options, verb, target, headers)
  result = performStream(client, req, sink)

include navi/private/verbs
