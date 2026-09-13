## Streaming downloads: the async pull-based StreamResponse handle.
## `include`d (transitively, via impl_common) by the asyncdispatch and chronos
## backends; shares their imports, the `Navi`/`Conn`/`H2Mux` types, and `await`.
## Not a standalone module.

# --- Streaming downloads (pull-based handle) ---

type
  StreamKind = enum skH1, skH2, skH3
  StreamResponseObj = object
    ## The response of a streaming request: status/headers are available
    ## immediately while the body is drained on demand. Holds either a checked-out
    ## http/1.1 connection (removed from the pool) or an open stream on the shared
    ## h2 mux, until `drain` finishes it or `close` disposes it.
    resp: Response             ## header snapshot (status/headers; empty body)
    client: Navi
    key: string                ## origin key, for returning an h1 connection to the pool
    decompress: bool
    cap: int
    cancel: CancelToken
    phase: StreamPhase         ## spOpen -> spDrained (body fully read) or spClosed
                               ## (disposed without draining); see StreamPhase
    guard: StreamGuard         ## closes/resets if the handle is dropped before
                               ## drain/close (see navi/private/streamguard)
    capped: CappedDecoder      ## h1/h3 decode + size-cap state carried across
                               ## readChunk calls (h2 keeps its decoder in the mux)
    case kind: StreamKind
    of skH1:
      transport: Conn          ## the checked-out http/1.1 connection
      parser: H1Parser
    of skH2:
      mux: H2Mux               ## the shared connection (stays live for reuse)
      sid: uint32              ## our stream on it
    of skH3:
      when defined(naviHttp3):
        qc: QuicConn      ## the shared h3 connection (stays live for reuse)
        h3sid: int64           ## our QUIC stream on it
      else: discard
  StreamResponse* = ref StreamResponseObj

proc close*(sr: StreamResponse): Future[void] {.async.} =
  ## Dispose a streaming handle whose body will not be fully drained: closes the
  ## http/1.1 connection (a partially-read response cannot be pooled) or resets the
  ## h2 stream (the shared connection stays up). Idempotent; a no-op once drained.
  if sr.phase != spOpen: return
  sr.phase = spClosed
  disarm(sr.guard)                    # we do the awaitable teardown ourselves
  case sr.kind
  of skH1: await close(sr.transport)
  of skH2: await sr.mux.abandon(sr.sid)
  of skH3:
    when defined(naviHttp3): sr.qc.freeStream(sr.h3sid)  # STOP_SENDING; conn stays up
    else: discard

proc status*(sr: StreamResponse): int = sr.resp.status
  ## The response status code, available before the body is drained.
proc reason*(sr: StreamResponse): string = sr.resp.reason
proc httpVersion*(sr: StreamResponse): string = sr.resp.httpVersion
proc headers*(sr: StreamResponse): Headers = sr.resp.headers
proc ok*(sr: StreamResponse): bool = sr.resp.ok
  ## Whether the status is 2xx (checked by the caller; the pull API never throws).

proc openStreamConn(client: Navi, req: Request): Future[StreamResponse] {.async.} =
  ## Send one request and read its headers, returning a handle with the body
  ## pending: multiplexed over a shared h2 connection when available/negotiable,
  ## otherwise a pooled http/1.1 connection. Mirrors `transport`, but stops at the
  ## response headers. Does not throw on non-2xx.
  let origin = originKey(req.url)
  let wantH2 = client.config.wantsH2 and req.url.isTls
  let decompress = client.config.wantsDecompress
  let cap = client.config.maxResponseBytes
  client.pruneDeadMuxes()          # evict muxes that died since the last request (#312)

  when defined(naviHttp3):
    # Stream over HTTP/3 when the origin has advertised h3 (Alt-Svc). Mirrors the
    # buffered h3Transport path: submit on the shared connection, read headers,
    # return a handle whose readChunk pulls the body incrementally. Any QUIC failure
    # falls through to h2/h1.
    if client.config.wantsH3 and req.url.isTls and req.bodyStream == nil:
      let ep = client.altSvc.h3Endpoint("https", req.url.host, req.url.port)
      if ep.isSome:
        try:
          let qc = await client.getH3Conn(origin, ep.get, req)
          var fwd: seq[(string, string)]
          for k, v in req.headers:
            let lk = k.toLowerAscii
            if lk notin h3SkipHeaders: fwd.add((lk, v))
          let sid = qc.submitStream($req.verb, req.url.requestTarget, fwd, req.body)
          if sid >= 0:
            try:
              let (status, hdrs) = await qc.awaitHeaders(sid)
              return StreamResponse(kind: skH3, qc: qc, h3sid: sid,
                resp: initResponse(status, "", "HTTP/3", initHeaders(hdrs), ""),
                client: client, key: origin, decompress: decompress, cap: cap, capped: initCappedDecoder(decompress, cap))
            except CatchableError:                 # header wait failed or was cancelled
              qc.freeStream(sid)                   # (e.g. timeout): free the submitted
              raise                                # stream so it isn't left on the wire
        except QuicError:
          if client.h3conns.getOrDefault(origin, nil) != nil and
             not client.h3conns[origin].alive:
            client.h3conns.del(origin)     # drop a dead connection; fall back below

  if wantH2:
    if client.muxes.hasKey(origin) and client.muxes[origin].canReuse:
      let mux = client.muxes[origin]
      let sid = await mux.sendAndReadHeaders(h2HeaderList(req), req.body, req.bodyStream, h2TrailerList(req))
      return StreamResponse(kind: skH2, mux: mux, sid: sid,
        resp: toResponse(mux.respSnapshot(sid)), client: client, key: origin,
        decompress: decompress, cap: cap, capped: initCappedDecoder(decompress, cap))
    if client.pendingMux.hasKey(origin):
      let mux = await client.pendingMux[origin]
      if mux != nil and mux.canReuse:
        let sid = await mux.sendAndReadHeaders(h2HeaderList(req), req.body, req.bodyStream, h2TrailerList(req))
        return StreamResponse(kind: skH2, mux: mux, sid: sid,
          resp: toResponse(mux.respSnapshot(sid)), client: client, key: origin,
          decompress: decompress, cap: cap, capped: initCappedDecoder(decompress, cap))

  for dead in reapExpired(client.pool):    # close idle connections past idleConnTimeout
    await close(dead.transport)            # (the buffered path reaps too; issue #313)
  var (found, pc) = popIdle(client.pool, origin)
  if found:
    try:
      let parser = h1SendAndReadHeaders(pc.transport, req, true)
      return StreamResponse(kind: skH1, transport: pc.transport, parser: parser,
        resp: parser.toResponse(), client: client, key: origin,
        decompress: decompress, cap: cap, capped: initCappedDecoder(decompress, cap))
    except CatchableError:
      await close(pc.transport)     # pooled connection was stale
      # Only open a fresh connection when replay is safe (mirrors transportInner):
      # the header read failed, so the request was not processed (the classic
      # keep-alive race -- safe to replay any method), but a non-rewindable streamed
      # body cannot be re-sent. Previously the streaming path replayed here
      # unconditionally, unlike the buffered path.
      if not isReplayable(req): raise

  var rq = req
  rq.absoluteForm = usesAbsoluteForm(resolveProxy(client.config, rq.url), rq.url.isTls)
  let (conn, mux) = await client.openFreshConn(rq, origin, wantH2)
  if mux != nil:
    let sid = await mux.sendAndReadHeaders(h2HeaderList(rq), rq.body, rq.bodyStream, h2TrailerList(rq))
    return StreamResponse(kind: skH2, mux: mux, sid: sid,
      resp: toResponse(mux.respSnapshot(sid)), client: client, key: origin,
      decompress: decompress, cap: cap, capped: initCappedDecoder(decompress, cap))
  else:
    let parser = h1SendAndReadHeaders(conn, rq, true)
    return StreamResponse(kind: skH1, transport: conn, parser: parser,
      resp: parser.toResponse(), client: client, key: origin,
      decompress: decompress, cap: cap, capped: initCappedDecoder(decompress, cap))

proc stream*(client: Navi, verb: HttpVerb, target: string,
             headers = initHeaders(), params: seq[(string, string)] = @[],
             cancel: CancelToken = nil): Future[StreamResponse] {.async.} =
  ## Open a streaming response: perform the request, follow redirects and digest
  ## auth to the final response, and return a handle whose status/headers are
  ## available immediately while the body streams on demand via `each`/`drain`.
  ##
  ## Unlike `request`, this does NOT throw on a non-2xx status (inspect `status`),
  ## and middleware is not applied. Redirect/digest hops are closed. Consume the
  ## returned handle with `each`/`drain`, or `close` it to skip the body.
  var rreq = buildRequest(client.config, verb, target, headers, params = params)
  let digestOrigin = originKey(rreq.url)   # digest creds only for this origin
  var hops = 0
  let limit = client.config.redirectLimit
  while true:
    throwIfCancelled(cancel)
    applyCookies(client.jar, rreq)
    let handle = await openStreamConn(client, rreq)
    handle.cancel = cancel
    when defined(naviHttp3):                       # learn h3 from a streamed response
      client.recordAltSvc(rreq, handle.resp)        # too, so SSE/stream can upgrade
    # Arm the leak-guard for the synchronous fallback teardown if the handle is
    # dropped without drain/close. Captures only the connection essentials (never
    # `handle`, which would cycle): the h1 transport, or the mux + stream id.
    case handle.kind
    of skH1:
      let t = handle.transport
      handle.guard = newStreamGuard(proc() {.gcsafe, raises: [].} =
        {.cast(gcsafe).}:
          try: t.closeSync()
          except Exception: discard)     # best-effort finalizer: never propagate
    of skH2:
      let mux = handle.mux
      let sid = handle.sid
      handle.guard = newStreamGuard(proc() {.gcsafe, raises: [].} =
        {.cast(gcsafe).}:
          try: (if mux != nil: mux.dropStream(sid))
          except Exception: discard)
    of skH3:
      when defined(naviHttp3):
        let qc = handle.qc
        let sid = handle.h3sid
        handle.guard = newStreamGuard(proc() {.gcsafe, raises: [].} =
          {.cast(gcsafe).}:
            try: (if qc != nil: qc.freeStream(sid))
            except Exception: discard)
      else: discard
    storeCookies(client.jar, rreq.url, handle.resp)
    # Origin check keeps digest credentials from being answered to a cross-origin
    # redirect target (redirectRequest strips Authorization on a cross-origin hop,
    # so without it the "no authorization header" test would pass and digest would
    # bypass that protection); mirrors the buffered path's maybeDigest.
    if handle.status == 401 and client.config.auth.kind == akDigest and
       originKey(rreq.url) == digestOrigin and
       not rreq.headers.contains("authorization"):
      let chal = bestChallenge(handle.headers.getAll("www-authenticate"))
      if chal.isSome:
        let auth = digestAuthHeader(client.config.auth.user, client.config.auth.pass,
                                    $rreq.verb, rreq.url.requestTarget, chal.get)
        if auth.len > 0:
          await handle.close()
          rreq.headers["authorization"] = auth
          continue
    let location = handle.headers.get("location")
    if shouldFollowRedirect(handle.status, hops, limit, location):
      await handle.close()
      rreq = redirectRequest(rreq, handle.status, location)
      inc hops
    else:
      return handle

proc readChunk*(sr: StreamResponse): Future[string] {.async.} =
  ## Pull the next decoded body chunk, or "" once the body is fully read. At end an
  ## h1 connection is returned to the pool (or closed) and an h2 stream is dropped
  ## on the shared connection, and the guard is disarmed; a cap breach or h2 reset
  ## closes/drops and reraises. The guard stays armed across the incremental reads,
  ## so a handle dropped before EOF is still cleaned up by it.
  if sr.phase != spOpen: return ""
  case sr.kind
  of skH2:
    try:
      result = await sr.mux.readChunk(sr.sid)
      if result.len == 0:                 # stream ended; readChunk dropped it
        sr.phase = spDrained
        disarm(sr.guard)
    except CatchableError:
      if sr.phase == spOpen: sr.phase = spDrained
      disarm(sr.guard)                    # readChunk dropped the stream; mux stays up
      raise
  of skH1:
    try:
      result = h1ReadChunk(sr.transport, sr.parser, sr.capped)
      if result.len == 0:                 # end of body: we own the teardown now
        sr.phase = spDrained
        disarm(sr.guard)
        if not (sr.parser.keepAliveAfter() and
                pushIdle(sr.client.pool, sr.key, PooledConn[Conn](transport: sr.transport))):
          await close(sr.transport)
    except CatchableError:
      if sr.phase == spOpen: sr.phase = spDrained
      disarm(sr.guard)
      await close(sr.transport)
      raise
  of skH3:
    when defined(naviHttp3):
      # h3 body arrives raw; apply the same streamed decode + size-cap as h1, then
      # free the stream at EOF (a reset surfaces as an error). The mux stays live.
      try:
        while true:
          let raw = await sr.qc.readStreamBody(sr.h3sid)
          if raw.len == 0:                # EOF
            sr.phase = spDrained
            disarm(sr.guard)
            let wasReset = sr.qc.streamWasReset(sr.h3sid)
            let tooLarge = sr.qc.streamTooLarge(sr.h3sid)          # before freeStream
            let lengthBad = sr.qc.streamLengthMismatch(sr.h3sid)  # before freeStream
            sr.qc.freeStream(sr.h3sid)
            if tooLarge:
              raise newException(ResponseTooLargeError,
                "navi: response exceeded maxResponseBytes")
            if wasReset: raise newException(IOError, "navi: http/3 stream reset")
            if lengthBad: raise newException(IOError, h3BodyLengthErr)
            if not sr.capped.streamComplete:   # compressed stream cut short mid-decode
              raise newException(IOError, truncatedBodyErr)
            return ""
          let decoded = sr.capped.feed(raw,
            if sr.capped.encodingResolved: "" else: sr.resp.headers.get("content-encoding"))
          if decoded.len == 0: continue   # decoder buffered input; pull more
          return decoded
      except CatchableError:
        if sr.phase == spOpen: sr.phase = spDrained
        disarm(sr.guard)
        sr.qc.freeStream(sr.h3sid)
        raise
    else: discard

proc drain*(sr: StreamResponse, sink: BodySink): Future[void] {.async.} =
  ## Deliver the response body to `sink` as it arrives (decoded and size-capped),
  ## awaiting it per chunk so a slow sink backpressures the peer. Then return an
  ## http/1.1 connection to the pool (or close it if it cannot be reused); an h2
  ## stream just finishes on the shared connection. Consumes the handle: call once.
  ## On error the connection is closed/reset and the error re-raised. Prefer `each`.
  if sr.phase != spOpen:
    raise newException(IOError, "navi: stream already drained or closed")
  throwIfCancelled(sr.cancel)
  disarm(sr.guard)                      # from here `drain` owns the teardown
  case sr.kind
  of skH2:
    try:
      await sr.mux.drainDownload(sr.sid, sink)
      sr.phase = spDrained                 # drainDownload freed the stream
    except CatchableError:
      sr.phase = spDrained                 # ...on error too, so the guard won't double-free
      raise
  of skH1:
    try:
      var keep = false
      h1DrainBody(sr.transport, sr.parser, sink, keep, sr.decompress, sr.cap)
      sr.phase = spDrained
      if not (keep and pushIdle(sr.client.pool, sr.key, PooledConn[Conn](transport: sr.transport))):
        await close(sr.transport)
    except CatchableError:
      if sr.phase == spOpen: sr.phase = spDrained
      await close(sr.transport)
      raise
  of skH3:
    when defined(naviHttp3):
      # Reuse readChunk's decode/cap/free per chunk; it sets `drained` at EOF. The
      # sink is a bare closure (no chronos raises annotation); navi's contract is it
      # raises at most CatchableError -- discharge chronos's strict effects here, as
      # drainDownload does. `readChunk` frees the stream on its own error/EOF, but a
      # SINK error escapes it while the guard is already disarmed (above), so free the
      # stream here too or it leaks on the shared connection (mirrors skH1/skH2).
      try:
        while true:
          let chunk = await sr.readChunk()
          if chunk.len == 0: break
          {.cast(gcsafe).}:
            {.cast(raises: [CatchableError]).}:
              await sink(chunk)
      except CatchableError:
        if sr.phase == spOpen:
          sr.phase = spDrained
          sr.qc.freeStream(sr.h3sid)
        raise
    else: discard

template each*(sr: StreamResponse; chunk, body: untyped): untyped =
  ## Drain the streaming body, running `body` for each decoded chunk with `chunk`
  ## bound to it (an owned `string`, moved from navi's read buffer). The outer
  ## `await` is baked in, so call it inside an async proc without one:
  ##   let res = await api.stream(GET, url)
  ##   res.each(chunk): await outFile.write(chunk)
  ##
  ## `body` runs as a proc, so `break`/`continue`/`return` cannot escape the loop
  ## from inside it. To stop early, don't call `each` and `close` the handle, or
  ## raise from `body` (which closes/resets the connection and propagates).
  await sr.drain(proc(chunk: string): Future[void] {.async.} = body)

