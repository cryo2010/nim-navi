## Server-Sent Events: the async SseStream reader with reconnect.
## `include`d (transitively, via impl_common) by the asyncdispatch and chronos
## backends; shares their imports, the `Navi`/`Conn`/`H2Mux` types, and `await`.
## Not a standalone module.

# --- Server-Sent Events (text/event-stream) ---

type
  SseStreamObj = object
    ## A first-class SSE stream. Pulls parsed events via `next`/`each`, reconnecting
    ## transparently (Last-Event-ID + the server's retry:) unless `reconnect` is off.
    client: Navi
    verb: HttpVerb
    target: string
    headers: Headers
    params: seq[(string, string)]
    cancel: CancelToken
    reconnect: bool
    baseRetryMs: int
    retryMs: int
    maxRetryMs: int
    idleTimeoutMs: int    ## bound on a single read / (re)open wait; 0 = unbounded
    handle: StreamResponse
    parser: SseParser
    started: bool
    closed: bool
    yieldCtr: int         ## events since the last cooperative yield (see `next`)
    collectCtr: int       ## yield batches since the last flood cycle-collect (see `next`)
  SseStream* = ref SseStreamObj

proc openConn(s: SseStream): Future[void] {.async.} =
  ## (Re)open the underlying stream and require a 200 text/event-stream response.
  s.parser.reset()
  var h = s.headers
  let lid = s.parser.lastEventId()
  if lid.len > 0: h["last-event-id"] = lid
  let handle = await s.client.stream(s.verb, s.target, h, s.params, s.cancel)
  if handle.status != 200:
    await handle.close()
    raise newException(IOError, "navi: SSE got status " & $handle.status &
      " (expected 200)")
  if not handle.headers.get("content-type").toLowerAscii.startsWith("text/event-stream"):
    let ct = handle.headers.get("content-type")
    await handle.close()
    raise newException(IOError,
      "navi: SSE expected Content-Type text/event-stream, got '" & ct & "'")
  s.handle = handle

proc sse*(client: Navi, target: string, verb = GET,
          headers = initHeaders(), body = "",
          params: seq[(string, string)] = @[],
          lastEventId = "", reconnect = true,
          retryMs = 3000, maxRetryMs = 30_000, idleTimeoutMs = 45_000,
          cancel: CancelToken = nil): Future[SseStream] {.async.} =
  ## Open a Server-Sent Events stream. The initial response is validated up front (a
  ## non-200 or non `text/event-stream` response raises). Consume events with `next`
  ## (none at end) or `each` (a real loop, so break/return work). Reconnects
  ## transparently on a drop -- resending Last-Event-ID and honoring the server's
  ## retry: with backoff to `maxRetryMs` -- unless `reconnect` is false. `verb`/
  ## `body`/headers allow POST-SSE and auth. The underlying stream runs with the
  ## size cap and read/total timeouts off and shares the client's cookie jar.
  ##
  ## `idleTimeoutMs` bounds how long a single read or (re)open may block before the
  ## stream is treated as wedged and reconnected (resending Last-Event-ID), so a
  ## parked read cannot hang forever -- the failure mode when all timeouts are off
  ## and reconnect only runs after a read returns (over a stalled HTTP/2 mux). Any
  ## byte -- including a keep-alive `:` comment -- resets it, so a live-but-quiet
  ## stream is not disturbed; set 0 to disable (only for a server known to go silent
  ## for long stretches without sending keep-alives).
  var cfg = client.config
  cfg.maxResponseBytes = 0
  cfg.timeouts.read = 0
  cfg.timeouts.total = 0
  var h = headers
  if not h.contains("accept"): h["accept"] = "text/event-stream"
  if not h.contains("cache-control"): h["cache-control"] = "no-cache"
  let s = SseStream(
    client: newNavi(cfg), verb: verb, target: target, headers: h, params: params,
    cancel: cancel, reconnect: reconnect, baseRetryMs: retryMs, retryMs: retryMs,
    maxRetryMs: maxRetryMs, idleTimeoutMs: idleTimeoutMs, parser: initSseParser(lastEventId))
  s.client.jar = client.jar          # share cookies with the caller
  let openFut = s.openConn()
  if idleTimeoutMs > 0 and not await withTimeout(openFut, msOf(idleTimeoutMs)):
    raise newException(IOError, "navi: SSE connect timed out after " & $idleTimeoutMs & " ms")
  await openFut                      # complete (or surface openConn's own error)
  s.started = true
  return s

proc close*(s: SseStream): Future[void] {.async.} =
  ## Stop consuming and dispose the connection, including the dedicated internal
  ## client (its pool and h2 mux, whose reader is joined). Idempotent. Call it when
  ## done with the stream so the mux does not linger.
  if s.closed: return
  s.closed = true
  if s.handle != nil:
    await s.handle.close()
    s.handle = nil
  await s.client.close()

proc httpVersion*(s: SseStream): string =
  ## HTTP version of the current underlying connection, or "" between reconnects.
  ## An SSE stream starts on h1/h2 and upgrades to h3 only after a reconnect.
  if s.handle != nil: s.handle.httpVersion else: ""

proc lastEventId*(s: SseStream): string = s.parser.lastEventId()

proc next*(s: SseStream): Future[Option[SseEvent]] {.async.} =
  ## The next event, or none once the stream ends. Reconnects transparently on a
  ## drop when enabled, resending Last-Event-ID.
  while true:
    if s.closed: return none(SseEvent)   # also catches a close during a parked read
    let ev = s.parser.next()
    if ev.isSome:
      if s.parser.retryMs() >= 0:
        s.baseRetryMs = min(s.parser.retryMs(), s.maxRetryMs)
      # A server that floods events lets the read complete synchronously every time,
      # so this loop can run the whole soak without the underlying read ever parking.
      # Two things then go wrong -- both only under such a flood; a normally-paced
      # stream parks in the read below long before 128 events and trips neither:
      #   1. On asyncdispatch the loop never re-enters `poll()`, starving its timers
      #      (e.g. a caller's reporter) and other tasks -- so yield cooperatively.
      #   2. The per-read future/closure chain is cyclic, and asyncdispatch relies on
      #      ORC's cycle collector (whose trigger scales with the live heap) to
      #      reclaim it, so a flooded stream floats into the GiBs before auto-
      #      collection fires. Force a collection on a spaced cadence (~1M events):
      #      spacing is what makes it cheap -- a collect only frees futures that have
      #      already died, so a sparse one reclaims a whole batch and amortizes to
      #      well under 1% of runtime, where a frequent one frees little yet still
      #      pays the O(heap) cost. chronos reclaims by refcount and needs neither.
      inc s.yieldCtr
      if s.yieldCtr >= 128:
        s.yieldCtr = 0
        await sleepAsync(msOf(0))
        when not defined(useChronos) and not defined(js):
          inc s.collectCtr
          if s.collectCtr >= 8192:
            s.collectCtr = 0
            GC_fullCollect()
      return ev
    if s.handle == nil:
      if not s.reconnect: return none(SseEvent)
      await sleepAsync(msOf(min(s.retryMs, s.maxRetryMs)))
      if s.closed: return none(SseEvent)    # closed during the backoff: do not reconnect
      try:
        let openFut = s.openConn()
        if s.idleTimeoutMs > 0 and not await withTimeout(openFut, msOf(s.idleTimeoutMs)):
          raise newException(IOError, "navi: SSE reconnect timed out")
        await openFut
        s.retryMs = s.baseRetryMs
      except CatchableError:
        if s.closed: return none(SseEvent)
        s.retryMs = min(max(s.retryMs, s.baseRetryMs) * 2, s.maxRetryMs)
        continue
    var chunk = ""
    try:
      let readFut = s.handle.readChunk()
      # Bound the read so a wedged mux (a parked read that never returns or raises)
      # is caught here and driven back through reconnect+backoff, instead of hanging
      # forever. Any byte, incl. a keep-alive comment, completes readFut and resets
      # the bound, so a live-but-quiet stream is untouched.
      if s.idleTimeoutMs > 0 and not await withTimeout(readFut, msOf(s.idleTimeoutMs)):
        # Idle bound elapsed with the read still parked. Dispose the handle so its h2
        # stream is RST and its concurrency slot freed -- abandoning it with just
        # `s.handle = nil` left the sid in the mux's sinkStreams, the orphaned read
        # acking to nobody, and a stream window + slot leaking per reconnect (#267).
        # Closing wakes the parked read; drain it so the future does not dangle.
        try: await s.handle.close()
        except CatchableError: discard
        try: discard await readFut
        except CatchableError: discard
        s.handle = nil
        if not s.reconnect: return none(SseEvent)
        continue
      chunk = await readFut
    except CatchableError:
      s.handle = nil
      if not s.reconnect: raise
      continue
    if chunk.len == 0:
      s.handle = nil
      if not s.reconnect: return none(SseEvent)
      continue
    s.parser.feed(chunk)

template each*(s: SseStream; ev, body: untyped): untyped =
  ## Consume events until the stream ends, binding `ev` to each `SseEvent`. A real
  ## loop (over the awaited `next`), so break/continue/return work:
  ##   let s = await api.sse(url)
  ##   s.each(ev): await handle(ev)
  while true:
    let evOpt = await s.next()
    if evOpt.isNone: break
    let ev = evOpt.get
    body

