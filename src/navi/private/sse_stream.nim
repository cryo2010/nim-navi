## Server-Sent Events: the SseStream reader with transparent reconnect.
## `include`d by navi.nim (the sync entry); shares its imports, the `Navi`
## type, and the pooled-transport engine. Not a standalone module.

# --- Server-Sent Events (text/event-stream) ---

type
  SseStreamObj = object
    ## A first-class SSE stream. Pulls parsed events via `next`/`each`, reconnecting
    ## transparently (Last-Event-ID + the server's retry:) unless `reconnect` is off.
    client: Navi              ## SSE-tuned client (no size cap, no read/total
                              ## timeout), sharing the caller's cookie jar
    verb: HttpVerb
    target: string
    headers: Headers
    params: seq[(string, string)]
    cancel: CancelToken
    reconnect: bool
    baseRetryMs: int          ## reconnect base delay (the server's retry: overrides)
    retryMs: int              ## current delay (backs off on repeated failures)
    maxRetryMs: int
    handle: StreamResponse    ## current underlying stream, nil between reconnects
    parser: SseParser
    started: bool
    closed: bool
  SseStream* = ref SseStreamObj

proc openConn(s: SseStream) =
  ## (Re)open the underlying stream and require a 200 text/event-stream response.
  s.parser.reset()
  var h = s.headers
  let lid = s.parser.lastEventId()
  if lid.len > 0: h["last-event-id"] = lid
  let handle = s.client.stream(s.verb, s.target, h, s.params, s.cancel)
  if handle.status != 200:
    handle.close()
    raise newException(IOError, "navi: SSE got status " & $handle.status &
      " (expected 200)")
  if not handle.headers.get("content-type").toLowerAscii.startsWith("text/event-stream"):
    let ct = handle.headers.get("content-type")
    handle.close()
    raise newException(IOError,
      "navi: SSE expected Content-Type text/event-stream, got '" & ct & "'")
  s.handle = handle

proc sse*(client: Navi, target: string, verb = GET,
          headers = initHeaders(), body = "",
          params: seq[(string, string)] = @[],
          lastEventId = "", reconnect = true,
          retryMs = 3000, maxRetryMs = 30_000,
          cancel: CancelToken = nil): SseStream =
  ## Open a Server-Sent Events stream. The initial response is validated up front:
  ## a non-200 or non `text/event-stream` response raises. Consume events with
  ## `next` (returns none at end) or `each` (a real loop, so break/return work). The
  ## stream reconnects transparently on a drop -- resending Last-Event-ID and
  ## honoring the server's retry: with exponential backoff up to `maxRetryMs` --
  ## unless `reconnect` is false. `verb`/`body`/headers allow POST-SSE and auth,
  ## which the platform EventSource cannot do. The underlying stream runs with the
  ## size cap and read/total timeouts off (SSE is long-lived) and shares the
  ## client's cookie jar.
  var cfg = client.config
  cfg.maxResponseBytes = 0
  cfg.timeouts.read = 0
  cfg.timeouts.total = 0
  var h = headers
  if not h.contains("accept"): h["accept"] = "text/event-stream"
  if not h.contains("cache-control"): h["cache-control"] = "no-cache"
  # A dedicated client (own pool; via newNavi so it has an Alt-Svc cache and can
  # upgrade to h3 on a reconnect), sharing the caller's cookie jar.
  let sc = newNavi(cfg)
  sc.jar = client.jar
  result = SseStream(
    client: sc,
    verb: verb, target: target, headers: h, params: params, cancel: cancel,
    reconnect: reconnect, baseRetryMs: retryMs, retryMs: retryMs,
    maxRetryMs: maxRetryMs, parser: initSseParser(lastEventId))
  result.openConn()            # eager: validate the initial response, fail fast
  result.started = true

proc close*(s: SseStream) =
  ## Stop consuming and dispose the connection, including the dedicated internal
  ## client (its pool). Idempotent. Call it when done with the stream.
  if s.closed: return
  s.closed = true
  if s.handle != nil:
    s.handle.close()
    s.handle = nil
  s.client.close()

proc lastEventId*(s: SseStream): string = s.parser.lastEventId()
  ## The persistent last event id (what a reconnect resends as Last-Event-ID).

proc httpVersion*(s: SseStream): string =
  ## The HTTP version of the current underlying connection ("HTTP/1.1"|"HTTP/2"|
  ## "HTTP/3"), or "" between reconnects. Note an SSE stream begins on h1/h2 and
  ## upgrades to h3 only on a reconnect (once Alt-Svc has been learned).
  if s.handle != nil: s.handle.httpVersion else: ""

proc next*(s: SseStream): Option[SseEvent] =
  ## The next event, or none once the stream ends (the server closed it and
  ## reconnection is off, or the handle was closed). Reconnects transparently on a
  ## drop when enabled, resending Last-Event-ID.
  if s.closed: return none(SseEvent)
  while true:
    let ev = s.parser.next()
    if ev.isSome:
      if s.parser.retryMs() >= 0:            # server set/updated the reconnect base
        s.baseRetryMs = min(s.parser.retryMs(), s.maxRetryMs)
      return ev
    if s.handle == nil:                      # need a (re)connection
      if not s.reconnect: return none(SseEvent)
      sleep(min(s.retryMs, s.maxRetryMs))
      try:
        s.openConn()
        s.retryMs = s.baseRetryMs            # reset backoff after a good connect
      except CatchableError:
        if s.closed: return none(SseEvent)
        s.retryMs = min(max(s.retryMs, s.baseRetryMs) * 2, s.maxRetryMs)
        continue
    var chunk = ""
    try:
      chunk = s.handle.readChunk()
    except CatchableError:                   # dropped mid-stream
      s.handle = nil
      if not s.reconnect: raise
      continue
    if chunk.len == 0:                        # server closed the stream cleanly
      s.handle = nil
      if not s.reconnect: return none(SseEvent)
      continue
    s.parser.feed(chunk)

template each*(s: SseStream; ev, body: untyped): untyped =
  ## Consume events until the stream ends, binding `ev` to each `SseEvent`. Unlike
  ## the stream() `each`, this is a real loop, so break/continue/return work:
  ##   let s = api.sse(url)
  ##   s.each(ev): echo ev.event, ": ", ev.data
  while true:
    let evOpt = s.next()
    if evOpt.isNone: break
    let ev = evOpt.get
    body
