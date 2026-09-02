## navi — JavaScript (fetch) entry point.
##
##   import navi/js
##
##   proc main() {.async.} =
##     let api = newNavi()
##     let res = await api.get("https://example.com")
##     echo res.status, " ", res.data
##   discard main()
##
## Runs only on the JavaScript backend (`nim js`). `fetch` handles TLS, HTTP
## version negotiation, redirects, and body decoding; navi layers on request
## building, hooks, retries, and throw-on-non-2xx. There is no connection pool
## (the runtime owns connections). Cookies persist automatically: navi keeps a
## jar wherever there is no browser cookie store (Node, Deno, Bun, Workers), and
## leaves it to the browser otherwise. Nothing to configure.
##
## Limitations vs the native backends: `config.http` is ignored (the runtime picks
## the HTTP version, and may well use h2/h3), and `res.httpVersion` is always `""`
## because `fetch` does not expose the negotiated version -- cross-backend code that
## branches on `res.httpVersion` must treat `""` as "unknown", not as HTTP/1.1.

when not defined(js):
  {.error: "navi/js requires the JavaScript backend; compile with `nim js` " &
           "(import `navi` for the native sync client).".}

import std/[asyncjs, jsffi, options]
from std/strutils import startsWith, toLowerAscii
import navi/private/entryguard
import navi/proto/sse
import navi/core/public
export sse.SseEvent
import navi/core/retry
import navi/backend/js
import navi/backend/jsws

claimEntry("navi/js")
export public, asyncjs
export jsws.WebSocket, jsws.WsMessage, jsws.WsMessageKind,
       jsws.send, jsws.receive, jsws.close, jsws.closeNormal, jsws.closeGoingAway,
       jsws.closeMessageTooBig, jsws.WsMessageTooLarge,
       jsws.WsReader, jsws.WsWriter, jsws.stream, jsws.streamBinary, jsws.each,
       jsws.readChunk, jsws.drain, jsws.write

type
  NaviContext* = ref object
    ## Carried through the middleware chain. A middleware reads and mutates it,
    ## then `await ctx.next()` runs the rest of the chain (which fills `res`).
    req*: Request            ## the outgoing request; modify it before `next`
    res*: Response           ## the response; set by `next`, adjust it after
    clientv: Navi            ## the owning client (see `client`); a shared ref
    cancel: CancelToken      ## caller's cancellation token, or nil
    idx: int                 ## index of the next middleware to run
  NaviMiddleware* = proc(ctx: NaviContext): Future[void] {.closure.}
    ## A middleware step; may be async. A closure, so it can capture: read/modify
    ## `ctx.req`, `await ctx.next()` to proceed -- or skip it to short-circuit --
    ## then read/modify `ctx.res`. Write it as a plain `{.async.}` proc; a factory
    ## `proc bearer(token): NaviMiddleware` closes over per-instance config. (No
    ## `gcsafe` here: JS is single-threaded, and requiring it would reject an
    ## awaiting middleware; the native backends add it where it means something.)

  NaviConfig* {.requiresInit.} = object of NaviConfigBase
    ## `requiresInit`: build it with `initNaviConfig()`, not a bare `NaviConfig(...)`.
    middleware*: seq[NaviMiddleware]

  Navi* = ref object
    config*: NaviConfig
      ## The client's live configuration. Mutate it to reconfigure between
      ## requests, e.g. `client.config.headers["authorization"] = "Bearer " & tok`;
      ## the change applies from the next request on. (The runtime owns
      ## connections, so `tls`/`http`/`proxy` are handled by `fetch`.)
    jar: CookieJar          ## kept off-browser; nil in a browser (its store owns cookies)

proc initNaviConfig*(): NaviConfig =
  ## The only way to build a config (`NaviConfig` requires every field). Safe
  ## defaults, minus decompression: the browser decodes bodies and forbids the
  ## Accept-Encoding request header, so navi does not add it. TLS and HTTP version
  ## negotiation are the runtime's (so `http` is unused here).
  NaviConfig(
    prefixUrl: "", headers: initHeaders(), http: {}, tls: defaultTls(),
    decompress: false, throwHttpErrors: true, maxRedirects: 20,
    retry: defaultRetryPolicy(), maxResponseBytes: 0,
    auth: Auth(), proxy: "", unixSocket: "",
    maxIdleConns: 0, maxIdleConnsPerHost: 0, idleConnTimeout: 0,
    timeouts: Timeouts(), middleware: @[])

# A browser owns the cookie store (and hides Set-Cookie from fetch); Node, Deno,
# Bun, and Workers do not, so navi keeps the jar there. `document` exists only in
# a browser document context.
proc inBrowser(): bool {.importjs: "(typeof document !== 'undefined')".}

proc checkUnsupported(cfg: NaviConfig) =
  ## The js backend uses `fetch`, which cannot dial a Unix socket path.
  if cfg.unixSocket.len > 0:
    raise newException(ValueError,
      "navi: Unix domain sockets are not supported on the js backend")

proc newNavi*(config = initNaviConfig()): Navi =
  checkUnsupported(config)
  result = Navi(config: config)
  if not inBrowser(): result.jar = newCookieJar()

proc extend*(client: Navi, config: NaviConfig): Navi =
  var merged = mergeBase(client.config, config)
  merged.middleware = client.config.middleware & config.middleware
  checkUnsupported(merged)
  result = Navi(config: merged)
  if not inBrowser(): result.jar = newCookieJar()

proc close*(client: Navi) =
  ## No-op: the browser/runtime owns connections. Present for API symmetry with
  ## the native backends.
  discard

# --- request core (wrapped by middleware in request/stream) ---
proc maybeThrow(client: Navi, req: Request, resp: Response) =
  if client.config.wantsThrow and not resp.ok:
    raise (ref HttpError)(
      msg: $req.verb & " " & $req.url & " -> " & $resp.status & " " & resp.reason,
      response: resp)

proc runCore(client: Navi, req0: Request, cancel: CancelToken): Future[Response] {.async.} =
  ## The innermost `next`: buffered request with the policy navi owns here (cookie
  ## jar, retries with backoff, size cap, throw-on-non-2xx). Redirects and decoding
  ## are the runtime's.
  var req = req0
  var resp: Response
  var attempt = 0
  let policy = client.config.retry
  while true:
    throwIfCancelled(cancel)
    var failed = false
    if not client.jar.isNil: applyCookies(client.jar, req)
    try:
      resp = await fetchExchange(req, nil, client.config.totalMs, cancel)
    except CatchableError:
      throwIfCancelled(cancel)   # a cancel is not a retryable failure
      if not (attempt < policy.limit and isRetryableVerb(req.verb, policy)):
        raise   # not retryable: propagate the fetch error
      failed = true
    if not failed and not client.jar.isNil:
      storeCookies(client.jar, req.url, resp)
    if not failed and
       not (attempt < policy.limit and isRetryableVerb(req.verb, policy) and
            isRetryableStatus(resp.status, policy)):
      break
    inc attempt
    await sleep(backoffMs(attempt, resp, policy))
  enforceMaxResponse(resp, client.config.maxResponseBytes)
  client.maybeThrow(req, resp)
  result = resp

proc client*(ctx: NaviContext): Navi = ctx.clientv
  ## The client handling this request (e.g. to read `ctx.client.config`).

proc next*(ctx: NaviContext): Future[void] {.async.} =
  ## Run the rest of the chain: the next middleware, or -- once they are
  ## exhausted -- the request itself. The outcome lands in `ctx.res`.
  let mws = ctx.clientv.config.middleware
  if ctx.idx >= mws.len:
    ctx.res = await runCore(ctx.clientv, ctx.req, ctx.cancel)
  else:
    let m = mws[ctx.idx]
    inc ctx.idx
    await m(ctx)

proc runChain(ctx: NaviContext): Future[Response] {.async.} =
  await ctx.next()
  return ctx.res

proc request*(client: Navi, verb: HttpVerb, target: string,
              headers = initHeaders(), body = "", json: JsonNode = nil,
              form: seq[(string, string)] = @[], multipart: Multipart = @[],
              bodyStream: BodyProducer = nil,
              params: seq[(string, string)] = @[],
              cancel: CancelToken = nil,
              trailers = initHeaders()): Future[Response] {.async.} =
  ## Perform a request; configured middleware wraps the whole call. `params` are
  ## appended to the URL query; `cancel` aborts the fetch.
  ##
  ## `bodyStream` is accepted for parity with the native backends, but the js
  ## backend **buffers** it: `fetch` cannot reliably stream a request body
  ## (`ReadableStream` + `duplex: "half"` support is uneven across runtimes), so
  ## the producer is drained into a full body before sending. `trailers` are
  ## rejected: `fetch` cannot send request trailers.
  if trailers.len > 0:
    raise newException(ValueError,
      "navi: request trailers are not supported on the js backend (fetch cannot send them)")
  var buffered = body
  if bodyStream != nil:
    buffered = ""
    while true:
      let chunk = bodyStream()
      if chunk.len == 0: break
      buffered.add chunk
  let req = buildRequest(client.config, verb, target, headers, buffered, json,
                         form, multipart, nil, params)
  if client.config.middleware.len == 0: return await runCore(client, req, cancel)
  let ctx = NaviContext(req: req, clientv: client, cancel: cancel)
  return await runChain(ctx)

# --- Streaming downloads (pull-based handle) ---

type
  StreamResponseObj = object
    ## The response of a streaming request: status/headers are available
    ## immediately while the body is drained on demand from the fetch
    ## `ReadableStream`. There is no connection pool here (the runtime owns
    ## connections); `close` aborts an undrained body so the runtime frees it.
    resp: Response             ## header snapshot (status/headers; empty body)
    client: Navi
    res: JsObject              ## the fetch Response (body still unread)
    controller: JsObject       ## AbortController for the body stream
    cancel: CancelToken
    cap: int
    drained: bool              ## body fully read
    closed: bool               ## body aborted without draining
    reader: JsObject           ## body ReadableStream reader (lazy, for readChunk)
    readerReady: bool
    seen: int
  StreamResponse* = ref StreamResponseObj

# No `=destroy` on the js backend: Nim's JavaScript backend does not run Nim
# destructors deterministically (the JS engine's GC owns object lifetime), so a
# backstop would not fire. Call `close` explicitly for a stream you open but will
# not fully read; otherwise the runtime reclaims the response on GC.

proc close*(sr: StreamResponse): Future[void] {.async.} =
  ## Dispose a streaming handle whose body will not be drained: aborts the fetch
  ## body stream so the runtime frees the connection. Idempotent; a no-op once the
  ## body has been drained. Async for parity with the native backends' `close`.
  if sr.drained or sr.closed: return
  sr.closed = true
  if not sr.cancel.isNil: sr.cancel.disarmHook()
  abortBody(sr.controller)

proc status*(sr: StreamResponse): int = sr.resp.status
  ## The response status code, available before the body is drained.
proc reason*(sr: StreamResponse): string = sr.resp.reason
proc httpVersion*(sr: StreamResponse): string = sr.resp.httpVersion
proc headers*(sr: StreamResponse): Headers = sr.resp.headers
proc ok*(sr: StreamResponse): bool = sr.resp.ok
  ## Whether the status is 2xx (checked by the caller; the pull API never throws).

proc stream*(client: Navi, verb: HttpVerb, target: string,
             headers = initHeaders(), params: seq[(string, string)] = @[],
             cancel: CancelToken = nil): Future[StreamResponse] {.async.} =
  ## Open a streaming response: fetch and resolve status/headers, returning a
  ## handle whose body streams on demand via `each`/`drain`. Does NOT throw on a
  ## non-2xx status (inspect `status`), and middleware is not applied. Redirects,
  ## body decoding, and the cookie store are the runtime's here, as for `request`.
  throwIfCancelled(cancel)
  var rq = buildRequest(client.config, verb, target, headers, params = params)
  if not client.jar.isNil: applyCookies(client.jar, rq)
  let (res, controller) = await fetchOpen(rq, client.config.totalMs)
  if not cancel.isNil:
    cancel.armHook(proc() {.gcsafe, raises: [].} = abortBody(controller))
  let handle = StreamResponse(resp: headerSnapshot(res), client: client, res: res,
    controller: controller, cancel: cancel, cap: client.config.maxResponseBytes)
  if not client.jar.isNil: storeCookies(client.jar, rq.url, handle.resp)
  return handle

proc readChunk*(sr: StreamResponse): Future[seq[byte]] {.async.} =
  ## Pull the next body chunk as bytes, or an empty seq once the body is fully read.
  ## Marks the handle drained at end; a cap breach aborts the body and reraises. On
  ## js the runtime owns connections, so there is no pool return; drop or `close` an
  ## unfinished handle to free it. Chunks are `seq[byte]` (from a JS Uint8Array), as
  ## with the js sink.
  if sr.drained or sr.closed: return newSeq[byte](0)
  if not sr.readerReady:
    sr.reader = bodyReader(sr.res)
    sr.readerReady = true
  var bytes = await readOne(sr.reader)
  if bytes.len == 0:                      # end of body
    sr.drained = true
    if not sr.cancel.isNil: sr.cancel.disarmHook()
    return bytes
  sr.seen += bytes.len
  if sr.cap > 0 and sr.seen > sr.cap:
    sr.drained = true
    abortBody(sr.controller)
    if not sr.cancel.isNil: sr.cancel.disarmHook()
    raise newException(ResponseTooLargeError,
      "navi: response exceeded maxResponseBytes")
  return bytes

proc drain*(sr: StreamResponse, sink: BodySink): Future[void] {.async.} =
  ## Deliver the response body to `sink` as it arrives (size-capped), awaiting it
  ## per chunk so a slow sink paces reads from the stream. Consumes the handle:
  ## call once. On error the body is left aborted and the error re-raised. Prefer
  ## the `each` template for the common case.
  if sr.drained or sr.closed:
    raise newException(IOError, "navi: stream already drained or closed")
  throwIfCancelled(sr.cancel)
  try:
    await drainToSink(sr.res, sink, sr.cap)
    sr.drained = true
  except CatchableError:
    sr.drained = true
    raise
  finally:
    if not sr.cancel.isNil: sr.cancel.disarmHook()

template each*(sr: StreamResponse; chunk, body: untyped): untyped =
  ## Drain the streaming body, running `body` for each chunk with `chunk` bound to
  ## it. On js `chunk` is a `seq[byte]` (chunks come from a JS `Uint8Array`), unlike
  ## the native backends' `string`. The outer `await` is baked in:
  ##   let res = await api.stream(GET, url)
  ##   res.each(chunk): total += chunk.len
  ##
  ## `body` runs as a proc, so `break`/`continue`/`return` cannot escape the loop
  ## from inside it. To stop early, don't call `each` and `close` the handle.
  await sr.drain(proc(chunk: seq[byte]): Future[void] {.async.} = body)

# --- Server-Sent Events (text/event-stream) ---

type
  SseStreamObj = object
    ## A first-class SSE stream over fetch. Pulls parsed events via `next`/`each`,
    ## reconnecting transparently (Last-Event-ID + the server's retry:) unless
    ## `reconnect` is off. Uses fetch directly (not the byte `StreamResponse`), so
    ## chunks are decoded as UTF-8 text, and supports any verb/headers unlike the
    ## platform EventSource.
    client: Navi
    req: Request
    reconnect: bool
    baseRetryMs: int
    retryMs: int
    maxRetryMs: int
    res: JsObject             ## current fetch Response
    controller: JsObject      ## AbortController for the current body
    reader: JsObject          ## its ReadableStream reader
    decoder: JsObject         ## a streaming TextDecoder
    haveConn: bool
    parser: SseParser
    started: bool
    closed: bool
  SseStream* = ref SseStreamObj

proc openConn(s: SseStream): Future[void] {.async.} =
  ## (Re)open the fetch stream and require a 200 text/event-stream response.
  s.parser.reset()
  var req = s.req
  let lid = s.parser.lastEventId()
  if lid.len > 0: req.headers["last-event-id"] = lid
  let (res, controller) = await fetchOpen(req, 0)   # no total timeout: long-lived
  let snap = headerSnapshot(res)
  if snap.status != 200:
    abortBody(controller)
    raise newException(IOError, "navi: SSE got status " & $snap.status &
      " (expected 200)")
  if not snap.headers.get("content-type").toLowerAscii.startsWith("text/event-stream"):
    let ct = snap.headers.get("content-type")
    abortBody(controller)
    raise newException(IOError,
      "navi: SSE expected Content-Type text/event-stream, got '" & ct & "'")
  s.res = res
  s.controller = controller
  s.reader = bodyReader(res)
  s.decoder = newTextDecoder()
  s.haveConn = true

proc sse*(client: Navi, target: string, verb = GET,
          headers = initHeaders(), body = "",
          params: seq[(string, string)] = @[],
          lastEventId = "", reconnect = true,
          retryMs = 3000, maxRetryMs = 30_000,
          cancel: CancelToken = nil): Future[SseStream] {.async.} =
  ## Open a Server-Sent Events stream over fetch. The initial response is validated
  ## up front (a non-200 or non `text/event-stream` response raises). Consume with
  ## `next` (none at end) or `each` (a real loop, so break/return work). Reconnects
  ## transparently on a drop, resending Last-Event-ID and honoring the server's
  ## retry: (backoff to `maxRetryMs`), unless `reconnect` is false. Redirects,
  ## cookies, and decoding are the runtime's, as elsewhere on js.
  var h = headers
  if not h.contains("accept"): h["accept"] = "text/event-stream"
  if not h.contains("cache-control"): h["cache-control"] = "no-cache"
  let s = SseStream(
    client: client, req: buildRequest(client.config, verb, target, h, body,
                                      params = params),
    reconnect: reconnect, baseRetryMs: retryMs, retryMs: retryMs,
    maxRetryMs: maxRetryMs, parser: initSseParser(lastEventId))
  await s.openConn()
  s.started = true
  return s

proc close*(s: SseStream) =
  ## Stop consuming and abort the body so the runtime frees the connection.
  ## Idempotent. Not awaitable (abort is synchronous).
  if s.closed: return
  s.closed = true
  if s.haveConn:
    abortBody(s.controller)
    s.haveConn = false

proc lastEventId*(s: SseStream): string = s.parser.lastEventId()

proc next*(s: SseStream): Future[Option[SseEvent]] {.async.} =
  ## The next event, or none once the stream ends. Reconnects transparently on a
  ## drop when enabled, resending Last-Event-ID.
  if s.closed: return none(SseEvent)
  while true:
    let ev = s.parser.next()
    if ev.isSome:
      if s.parser.retryMs() >= 0:
        s.baseRetryMs = min(s.parser.retryMs(), s.maxRetryMs)
      return ev
    if not s.haveConn:
      if not s.reconnect: return none(SseEvent)
      await sleep(min(s.retryMs, s.maxRetryMs))
      try:
        await s.openConn()
        s.retryMs = s.baseRetryMs
      except CatchableError:
        if s.closed: return none(SseEvent)
        s.retryMs = min(max(s.retryMs, s.baseRetryMs) * 2, s.maxRetryMs)
        continue
    var chunk = ""
    try:
      chunk = await readTextChunk(s.reader, s.decoder)
    except CatchableError:
      abortBody(s.controller)
      s.haveConn = false
      if not s.reconnect: raise
      continue
    if chunk.len == 0:
      s.haveConn = false
      if not s.reconnect: return none(SseEvent)
      continue
    s.parser.feed(chunk)

template each*(s: SseStream; ev, body: untyped): untyped =
  ## Consume events until the stream ends, binding `ev` to each `SseEvent`. A real
  ## loop (over the awaited `next`), so break/continue/return work.
  while true:
    let evOpt = await s.next()
    if evOpt.isNone: break
    let ev = evOpt.get
    body

proc websocket*(client: Navi, url: string,
                headers = initHeaders(),
                maxMessageBytes = 0, keepAlive = 0): Future[WebSocket] =
  ## Open a WebSocket over the runtime's native `WebSocket`. Accepts `ws://` /
  ## `wss://` (or http/https, which are mapped to ws/wss). Use `send`, `receive`,
  ## and `close`. `headers` is ignored: a browser WebSocket cannot set custom
  ## handshake headers, and the runtime handles ping/pong.
  ##
  ## `maxMessageBytes` (0 = unlimited) rejects an oversized message on delivery
  ## (closing with 1009), for parity with the native backends; the runtime has
  ## already buffered it and enforces its own limit, so this is not a memory guard.
  ## `keepAlive` is accepted for API parity but ignored: the runtime manages its own
  ## WebSocket keepalive and does not expose ping/pong.
  discard keepAlive
  var u = url
  if u.startsWith("http://"): u = "ws://" & u["http://".len .. ^1]
  elif u.startsWith("https://"): u = "wss://" & u["https://".len .. ^1]
  openWebSocket(u, maxMessageBytes)

include navi/private/verbs
