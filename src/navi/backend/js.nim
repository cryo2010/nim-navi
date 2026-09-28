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
import ../core/[headers, url, request, response, cancel, sinkgate]

type
  BodySink* = proc(data: seq[byte]): Future[void] {.closure.}
    ## Streaming download sink for the js backend. Awaitable: `drainToSink` `await`s
    ## it per chunk read from the fetch `ReadableStream`, so a slow sink naturally
    ## paces reads from the stream rather than buffering the whole body. Takes an
    ## owned `seq[byte]` (the chunk crosses an `await`).
    ##
    ## Deliberately `seq[byte]`, unlike the native backends' `string` sink: the chunk
    ## originates as a JS `Uint8Array` (bulk-copied into Nim, so there is no owned
    ## Nim buffer to move regardless of type), and `seq[byte]` is the
    ## binary-clean representation here -- the buffered `.text()` path goes through a
    ## JS (UTF-16) string and a UTF-8 transcode, so routing bytes through it would
    ## risk the same lossiness. Portable sinks targeting both js and native must
    ## handle both element types.

  GatedBodySink* = proc(data: seq[byte]): Future[bool] {.closure.}
    ## A response sink for `request()` that can stop the download early. Like
    ## `BodySink` it receives decoded body chunks of the FINAL surfaced response
    ## (`seq[byte]`, from a JS Uint8Array) and is awaited (backpressure), but returns
    ## `Future[bool]`: `true` keeps the transfer going, `false` stops it cleanly (the
    ## request returns normally with `res.body == ""` and `res.bodyTruncated == true`,
    ## the fetch body aborted). Only the final response's body is delivered;
    ## redirect/retry/thrown-error bodies never reach it.

  AsyncBodyProducer* = proc(): Future[string] {.closure.}
    ## Pull-based upload source for the js backend, accepted by `body` for API parity
    ## with the native async backends. `fetch` cannot stream a request body, so it is
    ## drained (awaited chunk by chunk) into a buffered body before sending, exactly
    ## like the sync `BodyProducer` on js. Its `Future` is `std/asyncjs`'s. Returns
    ## the next chunk, or "" at end of body.

# --- fetch / DOM bindings ---
proc fetch(url: cstring, init: JsObject): Future[JsObject] {.importjs: "fetch(#, #)".}
proc newHeaders(): JsObject {.importjs: "new Headers()".}
proc append(h: JsObject, name, value: cstring) {.importjs: "#.append(#, #)".}
proc jsText(res: JsObject): Future[cstring] {.importjs: "#.text()".}
proc headerEntries(res: JsObject): JsObject {.importjs: "Array.from(#.headers.entries())".}
proc setCookieList(res: JsObject): JsObject {.importjs: "(#.headers.getSetCookie?.() ?? [])".}
proc jsLen(arr: JsObject): int {.importjs: "#.length".}
proc bodyToU8(s: string): JsObject {.importjs: "new Uint8Array(#)".}
  ## A `Uint8Array` of a Nim js `string`'s bytes, byte-exact, in one copy. A Nim js
  ## `string` is a plain JS array of byte values, so `new Uint8Array` on it already
  ## IS its bytes. Request bodies go to `fetch` this way rather than as a `cstring`:
  ## the `cstring` conversion decodes the bytes as UTF-8 into a JS (UTF-16) string
  ## and `fetch` re-encodes them, which is only the identity for a body that is
  ## already valid UTF-8 -- any other byte >= 0x80 went on the wire as U+FFFD (#417).
proc u8ToBytes(arr: JsObject): seq[byte] {.importjs: "Array.prototype.slice.call(#)".}
  ## Bulk-copy a JS `Uint8Array` into a Nim `seq[byte]` in ONE call, instead of a
  ## jsffi property read plus a `.to(int)` conversion per byte (#412: a 50 MB body
  ## cost 50 million of each, and blocked the event loop for the whole chunk).
  ## Exact for arbitrary binary: nim's js backend represents `seq[byte]` as a plain
  ## JS array of byte values, which is precisely what `Array.prototype.slice.call`
  ## produces from a `Uint8Array` (element-wise, no numeric coercion, no arg-count
  ## limit -- unlike `String.fromCharCode.apply`, which blows the stack on a big
  ## chunk and would round-trip through a UTF-16 string anyway).

proc bytesOfBody*(s: string): seq[byte] {.importjs: "Array.prototype.slice.call(#)".}
  ## Bulk-copy a Nim js `string` to `seq[byte]` in one call: on the js backend both
  ## are plain JS arrays of byte values, so this is a native array copy rather than
  ## a per-char loop. For the buffered-body fallback that hands a `.text()` body to
  ## a `seq[byte]` sink.
proc bodyReader*(res: JsObject): JsObject {.importjs: "#.body.getReader()".}
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
    result["body"] = bodyToU8(req.body)   # raw bytes, not a transcoded cstring
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

proc drainToSink*(res: JsObject, sink: BodySink, cap: int) {.async.} =
  ## Stream the response body to `sink`, copying each Uint8Array chunk to bytes
  ## with a single bulk array copy (see `u8ToBytes`).
  ## `await`ing the sink paces reads from the stream (backpressure). When `cap` is
  ## set, the cumulative bytes read are capped (the browser already decoded the
  ## body, so this counts decoded bytes) and `ResponseTooLargeError` is raised.
  let reader = bodyReader(res)
  var seen = 0
  while true:
    let chunk = await readChunk(reader)
    if chunk["done"].to(bool): break
    let bytes = u8ToBytes(chunk["value"])
    seen += bytes.len
    if cap > 0 and seen > cap:
      raise newException(ResponseTooLargeError,
        "navi: response exceeded maxResponseBytes")
    await sink(bytes)

proc newTextDecoder*(): JsObject {.importjs: "new TextDecoder()".}
proc decodeStream(dec: JsObject, arr: JsObject): cstring
  {.importjs: "#.decode(#, {stream: true})".}

proc readTextChunk*(reader: JsObject, dec: JsObject): Future[string] {.async.} =
  ## Read one chunk from a fetch body reader and decode it as UTF-8 text (streaming,
  ## so a multi-byte character split across chunk boundaries is handled correctly),
  ## or "" at end of body. For the SSE reader, which needs text, not raw bytes.
  while true:
    let chunk = await readChunk(reader)
    if chunk["done"].to(bool): return ""
    let s = $decodeStream(dec, chunk["value"])
    if s.len == 0: continue                 # only a partial char so far: read more
    return s

proc readOne*(reader: JsObject): Future[seq[byte]] {.async.} =
  ## Read one non-empty chunk from a fetch body reader as bytes, or @[] at end of
  ## body. The pull equivalent of `drainToSink`; the caller holds the reader across
  ## calls and applies the size cap.
  while true:
    let chunk = await readChunk(reader)
    if chunk["done"].to(bool): return newSeq[byte](0)
    let arr = chunk["value"]
    if jsLen(arr) == 0: continue            # empty chunk mid-stream: read more
    return u8ToBytes(arr)

proc fetchExchange*(req: Request, sink: BodySink, timeout = 0,
                    cancel: CancelToken = nil, cap = 0,
                    userSink: BodySink = nil, gate: SinkGate = nil): Future[Response] {.async.} =
  ## One request/response through `fetch`. With a `sink`, the body streams to it
  ## and `Response.body` is left empty; otherwise the body is buffered. A nonzero
  ## `timeout` aborts the fetch after that many ms; `cancel` aborts it on demand.
  ## `cap` (when > 0) caps the streamed body size.
  ##
  ## `userSink`/`gate` (when set) are the buffered `request()` gated-sink path: after
  ## the headers arrive the gate decides whether this response is surfaced; if so the
  ## body streams to `userSink` (a wrapped sink that raises `SinkStopSignal` on an
  ## early stop, which aborts the fetch body and returns a truncated response); if not
  ## the body is buffered for the policy layer. To make the early-stop abort possible
  ## an AbortController is always created when `userSink` is set (folded into the
  ## signal combination), even without a cancel token or timeout.
  var controller: JsObject
  let wantCancel = cancel != nil
  let wantGated = not userSink.isNil and not gate.isNil
  if wantCancel or wantGated:
    controller = newAbortController()
    if wantCancel:
      cancel.armHook(proc() {.gcsafe, raises: [].} = controller.abort())
  var signal: JsObject
  let haveController = wantCancel or wantGated
  if haveController and timeout > 0: signal = anySignal(signalOf(controller), abortAfter(timeout))
  elif haveController:               signal = signalOf(controller)
  elif timeout > 0:                  signal = abortAfter(timeout)
  var res: JsObject
  try:
    res = await fetch(cstring(req.url.absoluteTarget),
                      buildInit(req, signal, haveController or timeout > 0))
  except:  # noqa: bare - a fetch rejection is a native JS error (no Nim m_type),
           # so a typed `except` would re-raise it. Surface it as a Nim exception
           # the retry loop and user `try/except` can handle like any transport error.
    if wantCancel and cancel.cancelled:
      raise newException(RequestCancelledError, "navi: request cancelled")
    raise newException(IOError, "navi: fetch failed: " & getCurrentExceptionMsg())
  finally:
    if wantCancel: cancel.disarmHook()
  if wantGated:
    # js `fetch` hides the negotiated version, so httpVersion is "" (protocolAllowed
    # passes on js, http == {}); the redirect/digest mirrors are inert (fetch follows
    # redirects itself, js has no digest), so the gate reduces to the retry/throw
    # decisions the js retry mirror fills.
    let snap = toResponse(res, "")
    if gate.wantsDelivery("", snap.status, snap.headers):
      try:
        await drainToSink(res, userSink, cap)
      except SinkStopSignal:
        abort(controller)              # stop the body stream; the request still returns
        var r = toResponse(res, "")
        r.bodyTruncated = true
        return r
      return toResponse(res, "")
    # Not surfaced: buffer for the policy layer (the runCore fallback delivers a
    # surfaced final body via the same userSink).
    result = toResponse(res, $(await jsText(res)))
  elif sink.isNil:
    result = toResponse(res, $(await jsText(res)))
  else:
    await drainToSink(res, sink, cap)
    result = toResponse(res, "")

proc fetchOpen*(req: Request, timeout = 0): Future[(JsObject, JsObject)] {.async.} =
  ## Fetch and resolve the response (status + headers), leaving the body unread for
  ## the pull-based streaming handle. Returns `(response, abortController)`; the
  ## controller aborts the still-open body stream when the caller closes the handle
  ## without draining it. Redirects and decoding are the runtime's, as in
  ## `fetchExchange`. A nonzero `timeout` aborts the fetch after that many ms.
  let controller = newAbortController()
  let signal = if timeout > 0: anySignal(signalOf(controller), abortAfter(timeout))
               else: signalOf(controller)
  var res: JsObject
  try:
    res = await fetch(cstring(req.url.absoluteTarget), buildInit(req, signal, true))
  except:  # noqa: bare - a fetch rejection is a native JS error (see fetchExchange).
    raise newException(IOError, "navi: fetch failed: " & getCurrentExceptionMsg())
  result = (res, controller)

proc headerSnapshot*(res: JsObject): Response = toResponse(res, "")
  ## The status/headers of a fetch response, with an empty body: the snapshot a
  ## streaming handle exposes before its body is drained.

proc abortBody*(controller: JsObject) = controller.abort()
  ## Abort a fetch whose body stream is still open, so the runtime frees the
  ## connection. Used when a streaming handle is closed without being drained.

proc sleep*(ms: int): Future[void] =
  ## Retry backoff, resolved by the runtime's timer.
  newPromise(proc (resolve: proc ()) = setTimeout(resolve, ms))
