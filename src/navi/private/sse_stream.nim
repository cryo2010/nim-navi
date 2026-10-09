## Server-Sent Events: the SseStream reader with transparent reconnect.
## `include`d by navi.nim (the sync entry); shares its imports, the `Navi`
## type, and the pooled-transport engine. Not a standalone module.

# --- Server-Sent Events (text/event-stream) ---

type
  SseStreamObj = object
    ## A first-class SSE stream. Pulls parsed events via `next`/`each`, reconnecting
    ## transparently (Last-Event-ID + the server's retry:) unless `reconnect` is off.
    client: Navi              ## a config-carrying `sharedView` of the caller's
                              ## client: the SSE-tuned config (no size cap, no total
                              ## timeout, per-read limit = idleTimeoutMs) over the
                              ## caller's own pool, h3 connections, Alt-Svc cache,
                              ## cookie jar and TLS stores (#466)
    verb: HttpVerb
    target: string
    headers: Headers
    params: seq[(string, string)]
    cancel: CancelToken
    reconnect: bool
    baseRetryMs: int          ## reconnect base delay (the server's retry: overrides)
    retryMs: int              ## current delay (backs off on repeated failures)
    minRetryMs: int           ## floor under every delay (a retry: 0 cannot spin us)
    maxRetryMs: int
    sawEvent: bool            ## the current connection delivered at least one event
    idleTimeoutMs: int        ## bound on a single read / (re)open wait; 0 = unbounded
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
          minRetryMs = defaultSseMinRetryMs, idleTimeoutMs = 45_000,
          cancel: CancelToken = nil): SseStream =
  ## Open a Server-Sent Events stream. The initial response is validated up front:
  ## a non-200 or non `text/event-stream` response raises. Consume events with
  ## `next` (returns none at end) or `each` (a real loop, so break/return work). The
  ## stream reconnects transparently on a drop -- resending Last-Event-ID and
  ## honoring the server's retry: with exponential backoff up to `maxRetryMs` --
  ## unless `reconnect` is false. `verb`/`body`/headers allow POST-SSE and auth,
  ## which the platform EventSource cannot do.
  ##
  ## The stream runs ON THE CALLER'S CLIENT: it reuses the client's pooled h1/h2
  ## connections, its Alt-Svc cache, its cookie jar and its TLS session cache, and
  ## anything it learns (a cookie, an `Alt-Svc` advertisement) lands there too. So
  ## repeated `sse()` calls over h1/h2 cost no extra handshake, and an origin this
  ## client already knows speaks h3 is dialled over HTTP/3 from the FIRST request,
  ## with `reconnect` off as well as on (issue #466). The sync h3 leg is the one
  ## exception to the reuse: a sync streamed h3 read runs on a dedicated QUIC
  ## connection of its own (never the client's h3 table), so each `sse()` and each
  ## reconnect over h3 pays its own QUIC handshake, resumed from the shared TLS
  ## session cache. The config the stream runs with differs from the
  ## caller's in exactly four places: `maxResponseBytes` and `timeouts.total` are off
  ## (an SSE stream is long-lived), `timeouts.read` is set FROM `idleTimeoutMs` (not
  ## off: a blocking read needs a socket-level bound to come back from a wedged
  ## server), and `timeouts.connect` is clamped to `idleTimeoutMs` when it is unset or
  ## larger. The cap being OFF reaches a pooled h2 connection the caller opened WITH a
  ## cap, because the cap is applied per stream, and the caller's own requests on that
  ## connection keep theirs (#466).
  ##
  ## Everything else is the client's live configuration, with these exceptions:
  ##
  ## * `tls`, `http` and `proxy` are bound when a connection is opened, as they are for
  ##   any navi request -- so a stream that REUSES one of the caller's pooled
  ##   connections gets what that connection was opened with.
  ## * an h2 connection IS shared with the caller's own requests here, unlike on the
  ##   async backends: a sync h2 connection is checked out of the pool exclusively for
  ##   one request at a time and every checkout re-arms its read timeout from the
  ##   checking-out config, so the stream's bound and the caller's never apply to the
  ##   same connection at once.
  ## * the sync STREAMED HTTP/3 leg has no read bound of any kind: its body read drives
  ##   the QUIC pump until a chunk lands, so neither `idleTimeoutMs` nor
  ##   `timeouts.read` bounds it and a silent h3 server blocks the calling thread. Use
  ##   an async backend, or h1/h2, for a stream that must come back from that.
  ## * a sync `SseStream` and the client it was opened on share MUTABLE state (the
  ##   connection pool, the cookie jar, and on an `-d:naviHttp3` build the h3
  ##   connection table), so the two are ONE thread-affine unit: do not hand the stream
  ##   to another thread while the client is in use, or the other way round.
  ##
  ## `idleTimeoutMs` bounds how long a single read or (re)open may block before the
  ## stream is treated as wedged and reconnected (resending Last-Event-ID), so a
  ## parked read cannot hang forever -- the failure mode when all timeouts are off
  ## and a server sends headers then goes silent. It is what the stream's own
  ## `timeouts.read` (and, when smaller, `timeouts.connect`) is set to, so any byte --
  ## including a keep-alive `:` comment -- resets it and a live-but-quiet stream is not
  ## disturbed; set 0 to disable (only for a server known to go silent for long
  ## stretches without sending keep-alives). It does not reach the sync streamed h3
  ## leg, which has no read bound (see above).
  ##
  ## `minRetryMs` floors every reconnect delay, including one the server asked for
  ## with `retry:`, so a `retry: 0` (or a server that answers 200 and closes with no
  ## events) cannot spin the reconnect loop. It is capped by `maxRetryMs`. A connect
  ## that closes without delivering an event also doubles the delay; only a connect
  ## that delivered at least one event resets it to the base.
  var cfg = client.config
  cfg.maxResponseBytes = 0
  # Bound each read (and each (re)open's header read / connect) by idleTimeoutMs so a
  # wedged server that goes silent surfaces a TimeoutError that the reconnect loop in
  # `next` drives back through backoff, instead of a read that blocks forever. 0 keeps
  # reads unbounded (only for a server known to go quiet without keep-alives). The
  # total timeout stays off because an SSE stream is long-lived by design.
  cfg.timeouts.read = idleTimeoutMs
  if idleTimeoutMs > 0 and (cfg.timeouts.connect <= 0 or cfg.timeouts.connect > idleTimeoutMs):
    cfg.timeouts.connect = idleTimeoutMs
  cfg.timeouts.total = 0
  var h = headers
  if not h.contains("accept"): h["accept"] = "text/event-stream"
  if not h.contains("cache-control"): h["cache-control"] = "no-cache"
  # A config-carrying VIEW of the caller's client: the SSE-tuned config above over
  # the caller's own pool, Alt-Svc cache, h3 connections, cookie jar and TLS stores,
  # so the stream reuses what the client already has (and reaches h3 on its first
  # connection when the client has already learned the advertisement). NOT newNavi,
  # which would replace the TLS session cache and context store (#466).
  result = SseStream(
    client: client.sharedView(cfg),
    verb: verb, target: target, headers: h, params: params, cancel: cancel,
    reconnect: reconnect,
    baseRetryMs: sseRetryDelay(retryMs, minRetryMs, maxRetryMs),
    retryMs: sseRetryDelay(retryMs, minRetryMs, maxRetryMs),
    minRetryMs: minRetryMs, maxRetryMs: maxRetryMs,
    idleTimeoutMs: idleTimeoutMs, parser: initSseParser(lastEventId))
  result.openConn()            # eager: validate the initial response, fail fast
  result.started = true

proc close*(s: SseStream) =
  ## Stop consuming and dispose THIS STREAM. Idempotent; call it when done with the
  ## stream. What it does to the underlying connection, on THIS (sync) backend:
  ##
  ## * http/1.1 and h2, closed MID-STREAM (the usual case: an event stream is
  ##   open-ended): the whole CONNECTION is closed. A half-read response cannot be
  ##   pooled, so `StreamResponse.close` closes the transport outright -- on a pooled
  ##   h2 connection too, because this backend has no background reader that could
  ##   drain the rest of the stream off a connection it kept. So a sync `sse()` that
  ##   is abandoned costs its connection, unlike a sync `get()`.
  ## * http/1.1 and h2, after the stream ENDED on its own (`next` returned none with
  ##   `reconnect` off): the connection was already returned to the pool by that last
  ##   read, and `close()` is a no-op.
  ## * HTTP/3: the stream's own QUIC connection is closed (the sync streamed h3 leg
  ##   dials one per stream; see `stream`).
  ##
  ## The async backends differ: there `close()` RSTs the h2/h3 stream and the shared
  ## connection stays up for the rest of the client's work.
  ##
  ## Nothing of the caller's is torn down (issue #466): the client keeps its pool, its
  ## Alt-Svc cache and its TLS session cache, and stays usable -- including for another
  ## `sse()` on the very connection this stream used. Closing the CLIENT is what
  ## disposes those, as for any other request.
  if s.closed: return
  s.closed = true
  if s.handle != nil:
    s.handle.close()
    s.handle = nil

proc lastEventId*(s: SseStream): string = s.parser.lastEventId()
  ## The persistent last event id (what a reconnect resends as Last-Event-ID).

proc httpVersion*(s: SseStream): string =
  ## The HTTP version of the current underlying connection ("HTTP/1.1"|"HTTP/2"|
  ## "HTTP/3"), or "" between reconnects. A stream opened on a client that has already
  ## learned the origin's `Alt-Svc: h3` is "HTTP/3" from the first connection; one
  ## whose client has not starts on h1/h2 and upgrades once the advertisement has been
  ## learned (on the next connection, which with `reconnect` off means on the client's
  ## next stream).
  if s.handle != nil: s.handle.httpVersion else: ""

proc sharesConnections*(s: SseStream, client: Navi): bool =
  ## Whether this stream runs on `client`'s own connection and discovery state: its
  ## pool, cookie jar, TLS session cache and context store, and -- on an
  ## `-d:naviHttp3` build -- its h3 connections and Alt-Svc cache. True for the client
  ## the stream was opened on. Introspection, for tests and for diagnosing an
  ## unexpected handshake or a stream that did not ride h3 (#466).
  if client == nil or s.client == nil: return false
  result = s.client.owner == client and                    # it is a view OF this client
           s.client.pool == client.pool and s.client.jar == client.jar and
           s.client.config.tls.sessionCache == client.config.tls.sessionCache and
           s.client.config.tls.contextStore == client.config.tls.contextStore
  when defined(naviHttp3):
    # Reference identity, not `tables`' structural `==`: two unrelated clients' empty
    # h3 tables compare EQUAL under `==`, which would answer true for a stream that
    # shares nothing.
    result = result and s.client.altSvc == client.altSvc and
             cast[pointer](s.client.h3conns) == cast[pointer](client.h3conns)

proc sharesH2Connections*(s: SseStream, client: Navi): bool =
  ## Whether this stream's HTTP/2 connections are the ones `client`'s own requests
  ## use. True on this backend: a sync h2 connection lives in the shared pool and is
  ## checked out exclusively for one request at a time, with its read timeout re-armed
  ## from the checking-out config, so one connection serves both without the two ever
  ## holding conflicting bounds on it. The async backends answer false -- there one h2
  ## connection carries many concurrent streams under a single bound, so an SSE stream
  ## gets its own (#466).
  s.client != nil and client != nil and s.client.pool == client.pool

proc dropConn(s: SseStream) =
  ## Release the current connection and fold it into the reconnect delay: a connect
  ## that delivered at least one event resets the delay to the base, one that
  ## delivered none steps it up, so a server that accepts and immediately closes
  ## backs off instead of being hammered (#291).
  s.handle = nil
  if s.sawEvent: s.retryMs = s.baseRetryMs
  else: s.retryMs = sseBackoff(s.retryMs, s.baseRetryMs, s.minRetryMs, s.maxRetryMs)
  s.sawEvent = false

proc next*(s: SseStream): Option[SseEvent] =
  ## The next event, or none once the stream ends (the server closed it and
  ## reconnection is off, or the handle was closed). Reconnects transparently on a
  ## drop when enabled, resending Last-Event-ID.
  if s.closed: return none(SseEvent)
  while true:
    let ev = s.parser.next()
    if ev.isSome:
      if s.parser.retryMs() >= 0:            # server set/updated the reconnect base
        s.baseRetryMs = sseRetryDelay(s.parser.retryMs(), s.minRetryMs, s.maxRetryMs)
      s.sawEvent = true                      # this connect earned a base-delay reset
      return ev
    if s.handle == nil:                      # need a (re)connection
      if not s.reconnect: return none(SseEvent)
      sleep(sseRetryDelay(s.retryMs, s.minRetryMs, s.maxRetryMs))
      try:
        s.openConn()
      except CatchableError:
        if s.closed: return none(SseEvent)
        s.retryMs = sseBackoff(s.retryMs, s.baseRetryMs, s.minRetryMs, s.maxRetryMs)
        continue
    var chunk = ""
    try:
      chunk = s.handle.readChunk()
    except CatchableError:                   # dropped mid-stream
      s.dropConn()
      if not s.reconnect: raise
      continue
    if chunk.len == 0:                        # server closed the stream cleanly
      s.dropConn()
      if not s.reconnect: return none(SseEvent)
      continue
    try:
      s.parser.feed(chunk)
    except CatchableError:
      # A maxSseEventBytes breach raises straight out of `next`. Dispose the handle
      # first: the parse state is unusable and the caller is left holding a stream
      # it cannot resume, so leaving the connection open just leaks a socket.
      if s.handle != nil:
        try: s.handle.close()
        except CatchableError: discard
        s.handle = nil
      raise

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
