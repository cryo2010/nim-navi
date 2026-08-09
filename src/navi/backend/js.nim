## JavaScript transport: HTTP via the runtime's `fetch`.
##
## `fetch` already performs TLS, HTTP-version negotiation, redirect following,
## and content-decoding, so navi does none of that here. This module marshals a
## navi `Request` into a `fetch` call and the `Response` back, surfacing
## Set-Cookie via getSetCookie() so the entry's opt-in cookie jar can read it.
## JavaScript-only: compiled solely through `import navi/js` under `nim js`.

when not defined(js):
  {.error: "navi/backend/js is JavaScript-only; compile with `nim js` via `import navi/js`.".}

import std/[asyncjs, jsffi]
from std/strutils import cmpIgnoreCase
import ../core/[headers, url, request, response, cancel]

type
  BodySink* = proc(data: seq[byte]): Future[void] {.closure.}
    ## Streaming download sink for the js backend. Awaitable: `drainToSink` `await`s
    ## it per chunk read from the fetch `ReadableStream`, so a slow sink naturally
    ## paces reads from the stream rather than buffering the whole body. Takes an
    ## owned `seq[byte]` (the chunk crosses an `await`).
    ##
    ## Deliberately `seq[byte]`, unlike the native backends' `string` sink: the chunk
    ## originates as a JS `Uint8Array` (marshaled byte-by-byte into Nim, so there is
    ## no owned Nim buffer to move regardless of type), and `seq[byte]` is the
    ## binary-clean representation here -- a Nim js `string` is a JS (UTF-16) string,
    ## so routing bytes through it risks the same lossiness as the buffered `.text()`
    ## path. Portable sinks targeting both js and native must handle both element types.

# --- fetch / DOM bindings ---
proc fetch(url: cstring, init: JsObject): Future[JsObject] {.importjs: "fetch(#, #)".}
proc newHeaders(): JsObject {.importjs: "new Headers()".}
proc append(h: JsObject, name, value: cstring) {.importjs: "#.append(#, #)".}
proc jsText(res: JsObject): Future[cstring] {.importjs: "#.text()".}
proc headerEntries(res: JsObject): JsObject {.importjs: "Array.from(#.headers.entries())".}
proc setCookieList(res: JsObject): JsObject {.importjs: "(#.headers.getSetCookie?.() ?? [])".}
proc jsLen(arr: JsObject): int {.importjs: "#.length".}
proc bodyReader(res: JsObject): JsObject {.importjs: "#.body.getReader()".}
proc readChunk(reader: JsObject): Future[JsObject] {.importjs: "#.read()".}
proc setTimeout(cb: proc (), ms: int) {.importjs: "setTimeout(#, #)".}
proc abortAfter(ms: int): JsObject {.importjs: "AbortSignal.timeout(#)".}
proc newAbortController(): JsObject {.importjs: "new AbortController()".}
proc abort(c: JsObject) {.importjs: "#.abort()".}
proc signalOf(c: JsObject): JsObject {.importjs: "#.signal".}
proc anySignal(a, b: JsObject): JsObject {.importjs: "AbortSignal.any([#, #])".}

proc buildInit(req: Request, signal: JsObject, hasSignal: bool): JsObject =
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
  if hasSignal:
    result["signal"] = signal   # aborts on timeout and/or the caller's cancel

proc readHeaders(res: JsObject): Headers =
  result = initHeaders()
  let entries = headerEntries(res)
  for i in 0 ..< jsLen(entries):
    let pair = entries[i]
    let name = $pair[0].to(cstring)
    # `entries()` folds duplicate headers into one comma-joined value, which is
    # lossy for Set-Cookie (an Expires date contains a comma). Skip it here and
    # re-add each cookie individually from getSetCookie() below. In a browser
    # getSetCookie() returns [] (Set-Cookie is hidden), so this is a no-op there.
    if cmpIgnoreCase(name, "set-cookie") == 0: continue
    result.add(name, $pair[1].to(cstring))
  let cookies = setCookieList(res)
  for i in 0 ..< jsLen(cookies):
    result.add("set-cookie", $cookies[i].to(cstring))

proc toResponse(res: JsObject, body: string): Response =
  initResponse(res["status"].to(int), $res["statusText"].to(cstring),
               "",                    # fetch does not expose the negotiated version
               readHeaders(res), body)

proc drainToSink(res: JsObject, sink: BodySink, cap: int) {.async.} =
  ## Stream the response body to `sink`, copying each Uint8Array chunk to bytes.
  ## `await`ing the sink paces reads from the stream (backpressure). When `cap` is
  ## set, the cumulative bytes read are capped (the browser already decoded the
  ## body, so this counts decoded bytes) and `ResponseTooLargeError` is raised.
  let reader = bodyReader(res)
  var seen = 0
  while true:
    let chunk = await readChunk(reader)
    if chunk["done"].to(bool): break
    let arr = chunk["value"]
    var bytes = newSeq[byte](jsLen(arr))
    for i in 0 ..< bytes.len:
      bytes[i] = byte(arr[i].to(int))
    seen += bytes.len
    if cap > 0 and seen > cap:
      raise newException(ResponseTooLargeError,
        "navi: response exceeded maxResponseBytes")
    await sink(bytes)

proc fetchExchange*(req: Request, sink: BodySink, timeout = 0,
                    cancel: CancelToken = nil, cap = 0): Future[Response] {.async.} =
  ## One request/response through `fetch`. With a `sink`, the body streams to it
  ## and `Response.body` is left empty; otherwise the body is buffered. A nonzero
  ## `timeout` aborts the fetch after that many ms; `cancel` aborts it on demand.
  ## `cap` (when > 0) caps the streamed body size.
  var controller: JsObject
  let wantCancel = cancel != nil
  if wantCancel:
    controller = newAbortController()
    cancel.armHook(proc() {.gcsafe, raises: [].} = controller.abort())
  var signal: JsObject
  if wantCancel and timeout > 0: signal = anySignal(signalOf(controller), abortAfter(timeout))
  elif wantCancel:               signal = signalOf(controller)
  elif timeout > 0:              signal = abortAfter(timeout)
  var res: JsObject
  try:
    res = await fetch(cstring(req.url.absoluteTarget),
                      buildInit(req, signal, wantCancel or timeout > 0))
  except:  # noqa: bare - a fetch rejection is a native JS error (no Nim m_type),
           # so a typed `except` would re-raise it. Surface it as a Nim exception
           # the retry loop and user `try/except` can handle like any transport error.
    if wantCancel and cancel.cancelled:
      raise newException(RequestCancelledError, "navi: request cancelled")
    raise newException(IOError, "navi: fetch failed: " & getCurrentExceptionMsg())
  finally:
    if wantCancel: cancel.disarmHook()
  if sink.isNil:
    result = toResponse(res, $(await jsText(res)))
  else:
    await drainToSink(res, sink, cap)
    result = toResponse(res, "")

proc sleep*(ms: int): Future[void] =
  ## Retry backoff, resolved by the runtime's timer.
  newPromise(proc (resolve: proc ()) = setTimeout(resolve, ms))
