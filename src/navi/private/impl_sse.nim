## Server-Sent Events: the async SseStream reader with reconnect.
## `include`d (transitively, via impl_common) by the asyncdispatch and chronos
## backends; shares their imports, the `Navi`/`Conn`/`H2Mux` types, and `await`.
## Not a standalone module.

# --- Server-Sent Events (text/event-stream) ---

type
  SseStreamObj = object
    ## A first-class SSE stream. Pulls parsed events via `next`/`each`, reconnecting
    ## transparently (Last-Event-ID + the server's retry:) unless `reconnect` is off.
    client: Navi          ## a config-carrying `sharedView` of the caller's client: the
                          ## SSE-tuned config (no size cap, no read/total timeout) over
                          ## the caller's own pool, h3 connections, Alt-Svc cache,
                          ## cookie jar and TLS stores, and over the client's
                          ## SSE-only h2 mux table (#466)
    verb: HttpVerb
    target: string
    headers: Headers
    params: seq[(string, string)]
    cancel: CancelToken
    reconnect: bool
    baseRetryMs: int
    retryMs: int
    minRetryMs: int       ## floor under every delay (a retry: 0 cannot spin us)
    maxRetryMs: int
    sawEvent: bool        ## the current connection delivered at least one event
    idleTimeoutMs: int    ## bound on a single read / (re)open wait; 0 = unbounded
    idleDeadline: Future[void]  ## the stream's ONE re-armed idle bound (see `awaitRead`)
    lastByteAt: MonoTime  ## when the last chunk (any byte) landed; the bound slides off it
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
  s.lastByteAt = getMonoTime()     # a fresh connection gets a full idle window

proc sse*(client: Navi, target: string, verb = GET,
          headers = initHeaders(), body = "",
          params: seq[(string, string)] = @[],
          lastEventId = "", reconnect = true,
          retryMs = 3000, maxRetryMs = 30_000,
          minRetryMs = defaultSseMinRetryMs, idleTimeoutMs = 45_000,
          cancel: CancelToken = nil): Future[SseStream] {.async.} =
  ## Open a Server-Sent Events stream. The initial response is validated up front (a
  ## non-200 or non `text/event-stream` response raises). Consume events with `next`
  ## (none at end) or `each` (a real loop, so break/return work). Reconnects
  ## transparently on a drop -- resending Last-Event-ID and honoring the server's
  ## retry: with backoff to `maxRetryMs` -- unless `reconnect` is false. `verb`/
  ## `body`/headers allow POST-SSE and auth.
  ##
  ## The stream runs ON THE CALLER'S CLIENT: it reuses the client's pooled http/1.1
  ## connections, its shared h2 and HTTP/3 connections, its Alt-Svc cache, its cookie
  ## jar and its TLS session cache, and anything it learns (a cookie, an `Alt-Svc`
  ## advertisement) lands there too. So repeated `sse()` calls cost no extra handshake,
  ## and an origin this client already knows speaks h3 is dialled over HTTP/3 from the
  ## FIRST request, with `reconnect` off as well as on (issue #466). Only three config
  ## values are overridden, all to 0, because an SSE stream is long-lived:
  ## `maxResponseBytes`, `timeouts.read` and `timeouts.total`. `idleTimeoutMs` below is
  ## the stream's only read bound.
  ##
  ## With `timeouts.read` off, the HTTP/2 PING keepalive is the only thing that can
  ## notice a peer that has gone dark on the SSE connection, so a stream ALWAYS runs
  ## one: `timeouts.h2KeepAlive` is taken from the client, except that a client which
  ## disabled it (`0`) gets `defaultH2KeepAliveMs` on its SSE connections. Without
  ## that, such a connection would stay reusable forever and every later `sse()` on
  ## the client would loop open / idle-timeout / reconnect on the same zombie. A live
  ## change to `timeouts.h2KeepAlive` reaches an SSE connection already up, as it does
  ## a request connection.
  ##
  ## On HTTP/2 the streams of one client share an h2 connection per origin with EACH
  ## OTHER but not with the client's requests, which have their own. An h2 connection
  ## carries one read bound for every stream on it (the transport's, from the
  ## `timeouts.read` of whoever opened it, and its expiry fails the connection and all
  ## of its streams), so a stream that must run unbounded and a request that must give
  ## up at `timeouts.read` cannot ride the same connection in either direction. So an
  ## h2 origin costs one extra handshake for the first stream, and nothing after that.
  ## http/1.1 (the pool) and HTTP/3 (one multiplexed connection, bounded only at the
  ## handshake and then per request) are shared with the client's requests as they are
  ## with each other.
  ##
  ## Everything else is the client's live configuration, with these exceptions:
  ##
  ## * `tls`, `http` and `proxy` are bound when a connection is opened, as they are for
  ##   any navi request -- so a stream that REUSES one of the caller's connections gets
  ##   what that connection was opened with.
  ## * `decompress` is likewise fixed per shared h2/h3 connection, so a stream riding
  ##   one another stream opened decodes (or does not) as that connection was opened.
  ## * `timeouts.connect` is NOT overridden; the whole (re)open is instead bounded by
  ##   `idleTimeoutMs` below, which is the smaller bound in practice.
  ##
  ## `idleTimeoutMs` bounds how long a single read or (re)open may block before the
  ## stream is treated as wedged and reconnected (resending Last-Event-ID), so a
  ## parked read cannot hang forever -- the failure mode when all timeouts are off
  ## and reconnect only runs after a read returns (over a stalled HTTP/2 mux). Any
  ## byte -- including a keep-alive `:` comment -- resets it, so a live-but-quiet
  ## stream is not disturbed; set 0 to disable (only for a server known to go silent
  ## for long stretches without sending keep-alives).
  ##
  ## `minRetryMs` floors every reconnect delay, including one the server asked for
  ## with `retry:`, so a `retry: 0` (or a server that answers 200 and closes with no
  ## events) cannot spin the reconnect loop. It is capped by `maxRetryMs`. A connect
  ## that closes without delivering an event also doubles the delay; only a connect
  ## that delivered at least one event resets it to the base.
  var cfg = client.config
  cfg.maxResponseBytes = 0
  cfg.timeouts.read = 0
  cfg.timeouts.total = 0
  # With the read bound off, the PING keepalive is the SSE connection's ONLY
  # dark-peer detector, so a stream always runs one: a client that disabled the
  # keepalive gets the default on its SSE connections rather than a connection with
  # no liveness check at all, which would stay `canReuse` forever and make every
  # later sse() loop open / idle-timeout / reconnect on the same zombie (#466).
  if cfg.timeouts.h2KeepAlive <= 0: cfg.timeouts.h2KeepAlive = defaultH2KeepAliveMs
  var h = headers
  if not h.contains("accept"): h["accept"] = "text/event-stream"
  if not h.contains("cache-control"): h["cache-control"] = "no-cache"
  let s = SseStream(
    client: client.sharedView(cfg), verb: verb, target: target, headers: h,
    params: params, cancel: cancel, reconnect: reconnect,
    baseRetryMs: sseRetryDelay(retryMs, minRetryMs, maxRetryMs),
    retryMs: sseRetryDelay(retryMs, minRetryMs, maxRetryMs),
    minRetryMs: minRetryMs, maxRetryMs: maxRetryMs,
    idleTimeoutMs: idleTimeoutMs, parser: initSseParser(lastEventId))
  let openFut = s.openConn()
  # `withTimeout` (not asyncdispatch's retention-free `withinMs`, issue #468): this
  # body is shared with chronos, whose `withTimeout` cancels the loser, and the bound
  # is armed once per CONNECT rather than per request or per read -- so the timer a
  # won connect leaves behind is one per stream, not one per unit of traffic.
  if idleTimeoutMs > 0 and not await withTimeout(openFut, msOf(idleTimeoutMs)):
    raise newException(IOError, "navi: SSE connect timed out after " & $idleTimeoutMs & " ms")
  await openFut                      # complete (or surface openConn's own error)
  s.started = true
  return s

proc close*(s: SseStream): Future[void] {.async.} =
  ## Stop consuming and dispose THIS STREAM: the handle is closed, which closes an
  ## http/1.1 connection that is still mid-body (one cannot be pooled) or RSTs the
  ## stream on the shared h2/h3 connection, leaving that connection up. Idempotent.
  ## Call it when done with the stream.
  ##
  ## Nothing of the caller's is torn down (issue #466): the client keeps its pool, its
  ## shared h2/h3 connections, its Alt-Svc cache and its TLS session cache, and stays
  ## usable -- including for another `sse()` on the very connection this stream used.
  ## Closing the CLIENT is what disposes those, as for any other request (its `close`
  ## reaps the SSE h2 connections too).
  if s.closed: return
  s.closed = true
  if s.handle != nil:
    await s.handle.close()
    s.handle = nil

proc httpVersion*(s: SseStream): string =
  ## HTTP version of the current underlying connection, or "" between reconnects.
  ## A stream opened on a client that has already learned the origin's
  ## `Alt-Svc: h3` is "HTTP/3" from the first connection; one whose client has not
  ## starts on h1/h2 and upgrades once the advertisement has been learned (on the
  ## next connection, which with `reconnect` off means on the client's next stream).
  if s.handle != nil: s.handle.httpVersion else: ""

proc sharesConnections*(s: SseStream, client: Navi): bool =
  ## Whether this stream runs on `client`'s own connection and discovery state: its
  ## pool, cookie jar, TLS session cache and context store, its SSE h2 mux table (the
  ## one `client.close()` reaps and the client's other streams reuse, which on h2 is
  ## deliberately not the table its REQUESTS use -- see `sharedView`), and -- on an
  ## `-d:naviHttp3` build -- its h3 connections and Alt-Svc cache. True for the client
  ## the stream was opened on. Introspection, for tests and for diagnosing an
  ## unexpected handshake or a stream that did not ride h3 (#466).
  if client == nil or s.client == nil: return false
  result = s.client.owner == client and                    # it is a view OF this client
           s.client.pool == client.pool and s.client.jar == client.jar and
           sameTable(s.client.muxes, client.sseMuxes) and
           sameTable(s.client.pendingMux, client.ssePendingMux) and
           s.client.orphanCloses == client.orphanCloses and
           s.client.config.tls.sessionCache == client.config.tls.sessionCache and
           s.client.config.tls.contextStore == client.config.tls.contextStore
  when defined(naviHttp3):
    result = result and s.client.altSvc == client.altSvc and
             sameTable(s.client.h3conns, client.h3conns) and
             sameTable(s.client.pendingH3, client.pendingH3)

proc sharesH2Connections*(s: SseStream, client: Navi): bool =
  ## Whether this stream's h2 connections are the ones `client`'s own REQUESTS use.
  ## Always false, deliberately: see `sharedView` for why a stream with no read bound
  ## cannot ride a connection whose `timeouts.read` the client's requests depend on
  ## (#466). Present so the distinction is assertable rather than implied by
  ## `sharesConnections`.
  s.client != nil and client != nil and sameTable(s.client.muxes, client.muxes)

proc lastEventId*(s: SseStream): string = s.parser.lastEventId()

proc dropConn(s: SseStream) =
  ## Release the current connection and fold it into the reconnect delay: a connect
  ## that delivered at least one event resets the delay to the base, one that
  ## delivered none steps it up, so a server that accepts and immediately closes
  ## backs off instead of being hammered (#291).
  s.handle = nil
  if s.sawEvent: s.retryMs = s.baseRetryMs
  else: s.retryMs = sseBackoff(s.retryMs, s.baseRetryMs, s.minRetryMs, s.maxRetryMs)
  s.sawEvent = false

proc awaitRead(s: SseStream, readFut: Future[string]): Future[bool] {.async.} =
  ## Park on `readFut` under the stream's sliding idle bound: true once the read
  ## completes, false once `idleTimeoutMs` has gone by with no byte arriving at all.
  ##
  ## The bound is ONE re-armed sleep per stream that every parked read is raced against
  ## (`awaitWithin`, per backend), not a `withTimeout` per read. Neither backend evicts
  ## a `withTimeout`'s losing timer: asyncdispatch leaves the `sleepAsync` future in its
  ## timer heap, and chronos's `clearTimer` only nils the callback while the entry stays
  ## in `loop.timers` until its moment. Under a server that floods events -- where a
  ## read parks constantly -- that was one live idleTimeoutMs-long (default 45 s) timer
  ## per read: a sliding window of them that no collection can reclaim, since they are
  ## reachable from the dispatcher.
  while true:
    let idleMs = int((getMonoTime() - s.lastByteAt).inMilliseconds)
    if idleMs >= s.idleTimeoutMs: return false    # a whole window without a byte
    if s.idleDeadline == nil or s.idleDeadline.finished:
      s.idleDeadline = sleepAsync(msOf(s.idleTimeoutMs - idleMs))
    if await awaitWithin(readFut, s.idleDeadline): return true
    # The deadline elapsed, but it may have been armed before this read started (a
    # window that began at an earlier chunk): re-arm for what is left and keep waiting
    # on the same read. Only a full window with no byte at all returns false above.

proc next*(s: SseStream): Future[Option[SseEvent]] {.async.} =
  ## The next event, or none once the stream ends. Reconnects transparently on a
  ## drop when enabled, resending Last-Event-ID.
  while true:
    if s.closed: return none(SseEvent)   # also catches a close during a parked read
    let ev = s.parser.next()
    if ev.isSome:
      if s.parser.retryMs() >= 0:
        s.baseRetryMs = sseRetryDelay(s.parser.retryMs(), s.minRetryMs, s.maxRetryMs)
      s.sawEvent = true                  # this connect earned a base-delay reset
      # A server that floods events lets the read complete synchronously every time,
      # so this loop can run the whole soak without the underlying read ever parking.
      # Two things then go wrong -- both only under such a flood; a normally-paced
      # stream parks in the read below long before 128 events and trips neither:
      #   1. On asyncdispatch the loop never re-enters `poll()`, starving its timers
      #      (e.g. a caller's reporter) and other tasks -- so yield cooperatively.
      #   2. The per-read future/closure chain is cyclic, and both backends rely on
      #      ORC's cycle collector (whose trigger scales with the live heap) to
      #      reclaim it, so a flooded stream floats into the GiBs before auto-
      #      collection fires. Force a collection on a spaced cadence (~1M events):
      #      spacing is what makes it cheap -- a collect only frees futures that have
      #      already died, so a sparse one reclaims a whole batch and amortizes to
      #      well under 1% of runtime, where a frequent one frees little yet still
      #      pays the O(heap) cost. chronos reclaims the plain refcounted chain
      #      eagerly, but a future abandoned mid-await and a Future/cancel-callback-env
      #      pair are cyclic there too, so it gets the same valve (a flooded chronos
      #      h3 SSE soak floated ~14 MiB per two minutes without it). The yield in 1.
      #      is kept on both: chronos re-enters poll() on its own, and a yield it does
      #      not need is only a queue hop.
      inc s.yieldCtr
      if s.yieldCtr >= 128:
        s.yieldCtr = 0
        await sleepAsync(msOf(0))
        when not defined(js):
          inc s.collectCtr
          if s.collectCtr >= 8192:
            s.collectCtr = 0
            GC_fullCollect()
      return ev
    if s.handle == nil:
      if not s.reconnect: return none(SseEvent)
      await sleepAsync(msOf(sseRetryDelay(s.retryMs, s.minRetryMs, s.maxRetryMs)))
      if s.closed: return none(SseEvent)    # closed during the backoff: do not reconnect
      try:
        let openFut = s.openConn()
        if s.idleTimeoutMs > 0 and not await withTimeout(openFut, msOf(s.idleTimeoutMs)):
          raise newException(IOError, "navi: SSE reconnect timed out")
        await openFut
      except CatchableError:
        if s.closed: return none(SseEvent)
        s.retryMs = sseBackoff(s.retryMs, s.baseRetryMs, s.minRetryMs, s.maxRetryMs)
        continue
    var chunk = ""
    try:
      let readFut = s.handle.readChunk()
      # Bound the read so a wedged mux (a parked read that never returns or raises)
      # is caught here and driven back through reconnect+backoff, instead of hanging
      # forever. Any byte, incl. a keep-alive comment, completes readFut and resets
      # the bound (`lastByteAt`), so a live-but-quiet stream is untouched.
      #
      # A read that completed synchronously needs no bound at all -- under a server that
      # floods events that is most of them -- and one that parks is bounded by the
      # stream's single sliding deadline rather than a timer of its own (`awaitRead`).
      if s.idleTimeoutMs > 0 and not readFut.finished and
         not await s.awaitRead(readFut):
        # Idle bound elapsed with the read still parked. Dispose the handle so its h2
        # stream is RST and its concurrency slot freed -- abandoning it with just
        # `s.handle = nil` left the sid in the mux's sinkStreams, the orphaned read
        # acking to nobody, and a stream window + slot leaking per reconnect (#267).
        # Closing wakes the parked read; drain it so the future does not dangle.
        try: await s.handle.close()
        except CatchableError: discard
        try: discard await readFut
        except CatchableError: discard
        s.dropConn()
        if not s.reconnect: return none(SseEvent)
        continue
      chunk = await readFut
      s.lastByteAt = getMonoTime()      # a byte landed: the idle window starts over
    except CatchableError:
      s.dropConn()
      if not s.reconnect: raise
      continue
    if chunk.len == 0:
      s.dropConn()
      if not s.reconnect: return none(SseEvent)
      continue
    # A maxSseEventBytes breach raises straight out of `next`. Dispose the handle
    # first: the parse state is unusable and the caller is left holding a stream it
    # cannot resume, so leaving the connection open leaks a socket (and, on h2, a
    # mux slot). The error is carried out of the `except` rather than awaited inside
    # it, since `await` in an exception handler is not portable across the two async
    # backends.
    var feedErr: ref CatchableError = nil
    try:
      s.parser.feed(chunk)
    except CatchableError as e:
      feedErr = e
    if feedErr != nil:
      if s.handle != nil:
        try: await s.handle.close()
        except CatchableError: discard
        s.handle = nil
      raise feedErr

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

