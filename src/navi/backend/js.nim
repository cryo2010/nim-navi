## JavaScript transport: HTTP via the runtime's `fetch`.
##
## `fetch` already performs TLS, HTTP-version negotiation, redirect following,
## cookie storage (the browser jar), and content-decoding, so navi does none of
## that here. This module only marshals a navi `Request` into a `fetch` call and
## the `Response` back. JavaScript-only: compiled solely through `import navi/js`
## under `nim js`.

when not defined(js):
  {.error: "navi/backend/js is JavaScript-only; compile with `nim js` via `import navi/js`.".}

import std/[asyncjs, jsffi]
import ../core/[headers, url, request, response]

# --- fetch / DOM bindings ---
proc fetch(url: cstring, init: JsObject): Future[JsObject] {.importjs: "fetch(#, #)".}
proc newHeaders(): JsObject {.importjs: "new Headers()".}
proc append(h: JsObject, name, value: cstring) {.importjs: "#.append(#, #)".}
proc jsText(res: JsObject): Future[cstring] {.importjs: "#.text()".}
proc headerEntries(res: JsObject): JsObject {.importjs: "Array.from(#.headers.entries())".}
proc jsLen(arr: JsObject): int {.importjs: "#.length".}
proc bodyReader(res: JsObject): JsObject {.importjs: "#.body.getReader()".}
proc readChunk(reader: JsObject): Future[JsObject] {.importjs: "#.read()".}
proc setTimeout(cb: proc (), ms: int) {.importjs: "setTimeout(#, #)".}

proc buildInit(req: Request): JsObject =
  result = newJsObject()
  result["method"] = cstring($req.verb)
  let h = newHeaders()
  for (name, value) in req.headers.pairs:
    append(h, cstring(name), cstring(value))
  result["headers"] = h
  if req.body.len > 0:
    result["body"] = cstring(req.body)
  result["redirect"] = cstring("follow")      # the browser follows redirects
  result["credentials"] = cstring("include")  # and owns the cookie jar

proc readHeaders(res: JsObject): Headers =
  result = initHeaders()
  let entries = headerEntries(res)
  for i in 0 ..< jsLen(entries):
    let pair = entries[i]
    result.add($pair[0].to(cstring), $pair[1].to(cstring))

proc toResponse(res: JsObject, body: string): Response =
  initResponse(res["status"].to(int), $res["statusText"].to(cstring),
               "",                    # fetch does not expose the negotiated version
               readHeaders(res), body)

proc drainToSink(res: JsObject, sink: BodySink) {.async.} =
  ## Stream the response body to `sink`, copying each Uint8Array chunk to bytes.
  let reader = bodyReader(res)
  while true:
    let chunk = await readChunk(reader)
    if chunk["done"].to(bool): break
    let arr = chunk["value"]
    var bytes = newSeq[byte](jsLen(arr))
    for i in 0 ..< bytes.len:
      bytes[i] = byte(arr[i].to(int))
    sink(bytes)

proc fetchExchange*(req: Request, sink: BodySink): Future[Response] {.async.} =
  ## One request/response through `fetch`. With a `sink`, the body streams to it
  ## and `Response.body` is left empty; otherwise the body is buffered.
  var res: JsObject
  try:
    res = await fetch(cstring(req.url.absoluteTarget), buildInit(req))
  except:  # noqa: bare — a fetch rejection is a native JS error (no Nim m_type),
           # so a typed `except` would re-raise it. Surface it as a Nim exception
           # the retry loop and user `try/except` can handle like any transport error.
    raise newException(IOError, "navi: fetch failed: " & getCurrentExceptionMsg())
  if sink.isNil:
    result = toResponse(res, $(await jsText(res)))
  else:
    await drainToSink(res, sink)
    result = toResponse(res, "")

proc sleep*(ms: int): Future[void] =
  ## Retry backoff, resolved by the runtime's timer.
  newPromise(proc (resolve: proc ()) = setTimeout(resolve, ms))
