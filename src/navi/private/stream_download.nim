## Streaming downloads: the pull-based StreamResponse handle.
## `include`d by navi.nim (the sync entry); shares its imports, the `Navi`
## type, and the pooled-transport engine. Not a standalone module.

# --- Streaming downloads (pull-based handle) ---

type
  StreamResponseObj = object
    ## The response of a streaming request: status/headers are available
    ## immediately while the body is drained on demand. Holds the checked-out
    ## connection (removed from the pool) until `drain` returns it or `close`
    ## disposes it.
    resp: Response             ## header snapshot (status/headers; empty body)
    client: Navi
    key: string                ## origin key, for returning the connection to the pool
    pc: PooledConn[Conn]       ## the checked-out connection (h2 conn when pc.h2 != nil)
    parser: H1Parser           ## used when pc.h2 == nil (http/1.1)
    sid: uint32                ## used when pc.h2 != nil (http/2 stream id)
    decompress: bool
    cap: int
    cancel: CancelToken
    phase: StreamPhase         ## spOpen -> spDrained (body fully read) or spClosed
                               ## (disposed without draining); see StreamPhase
    guard: StreamGuard         ## closes the connection if the handle is dropped
                               ## before drain/close (see navi/private/streamguard)
    capped: CappedDecoder      ## decode + size-cap state carried across readChunk
                               ## calls (the decoder is chosen on the first chunk)
    when defined(naviHttp3):
      qc: QuicConn             ## h3 connection (non-nil marks an h3 stream; pc unused)
      h3sid: int64             ## its h3 stream id
  StreamResponse* = ref StreamResponseObj

proc close*(sr: StreamResponse) =
  ## Dispose a streaming handle whose body will not be fully drained: closes the
  ## underlying connection (a partially-read response cannot be safely pooled).
  ## Idempotent, and a no-op once the body has been drained.
  if sr.phase != spOpen: return
  sr.phase = spClosed
  closeNow(sr.guard)

proc status*(sr: StreamResponse): int = sr.resp.status
  ## The response status code, available before the body is drained.
proc reason*(sr: StreamResponse): string = sr.resp.reason
proc httpVersion*(sr: StreamResponse): string = sr.resp.httpVersion
proc headers*(sr: StreamResponse): Headers = sr.resp.headers
proc ok*(sr: StreamResponse): bool = sr.resp.ok
  ## Whether the status is 2xx (checked by the caller; the pull API never throws).

proc openStream(client: Navi, req0: Request): StreamResponse =
  ## Send one request and read its headers, returning a handle that holds the
  ## checked-out connection with the body pending. A stale pooled connection is
  ## retried once on a fresh one. Does not throw on non-2xx.
  var rq = req0
  let proxy = resolveProxy(client.config, rq.url)
  rq.absoluteForm = usesAbsoluteForm(proxy, rq.url.isTls)
  let alpn = if client.config.wantsH2 and rq.url.isTls: @["h2", "http/1.1"] else: @[]
  let key = originKey(rq.url)
  let decompress = client.config.wantsDecompress
  let cap = client.config.maxResponseBytes

  when defined(naviHttp3):
    # Stream over HTTP/3 when the origin has advertised h3 (Alt-Svc). Blocking twin
    # of the async openStreamConn skH3 path: open a QUIC connection, submit, read the
    # headers, and return a handle whose readChunk pulls the body. The connection is
    # per-stream (no sync h3 pooling, matching the buffered path) and closed at EOF /
    # by the guard. Any QUIC failure falls through to the h2/h1 pool below.
    if client.config.wantsH3 and rq.url.isTls and client.altSvc != nil:
      let ep = client.altSvc.h3Endpoint("https", rq.url.host, rq.url.port)
      if ep.isSome:
        try:
          let conn = h3Open(ep.get.host, ep.get.port, sni = rq.url.host,
                            caFile = client.config.tls.caFile,
                            verify = client.config.tls.wantsVerify,
                            maxBody = uint64(max(0, client.config.maxResponseBytes)))
          var fwd: seq[(string, string)]
          for k, v in rq.headers:
            let lk = k.toLowerAscii
            if lk notin h3SkipHeaders: fwd.add((lk, v))
          let sid = conn.submitStream($rq.verb, rq.url.requestTarget, fwd)
          if sid < 0:
            conn.close()
          else:
            try:
              let (status, hdrs) = conn.awaitHeaders(sid)
              return StreamResponse(resp: initResponse(status, "", "HTTP/3",
                initHeaders(hdrs), ""), client: client, key: key, qc: conn,
                h3sid: sid, decompress: decompress, cap: cap, capped: initCappedDecoder(decompress, cap))
            except CatchableError:
              conn.freeStream(sid); conn.close(); raise
        except QuicError: discard   # fall back to the h2/h1 transport below

  for dead in reapExpired(client.pool):    # close idle connections past idleConnTimeout
    try: dead.transport.close() except CatchableError: discard
  var (found, pc) = popIdle(client.pool, key)
  if found:
    try:
      if pc.h2 != nil:
        let sid = h2SendAndReadHeaders(pc.transport, pc.h2, rq)
        return StreamResponse(resp: toResponse(pc.h2.respSnapshot(sid)), client: client,
                              key: key, pc: pc, sid: sid, decompress: decompress, cap: cap, capped: initCappedDecoder(decompress, cap))
      else:
        let parser = h1SendAndReadHeaders(pc.transport, rq, true)
        return StreamResponse(resp: parser.toResponse(), client: client, key: key,
                              pc: pc, parser: parser, decompress: decompress, cap: cap, capped: initCappedDecoder(decompress, cap))
    except CatchableError:
      try: pc.transport.close()          # pooled connection was stale; open a fresh one
      except CatchableError: discard

  let transport = connect(rq.url.host, rq.url.port, rq.url.isTls, client.config.tls,
                          proxy, alpn, client.config.connectMs, client.config.readMs,
                          client.config.totalMs)
  var npc = PooledConn[Conn](transport: transport)
  if transport.protocol == "h2":
    npc.h2 = initH2Conn(client.config.maxResponseBytes)
    transport.sendAll(npc.h2.preamble())
    let sid = h2SendAndReadHeaders(transport, npc.h2, rq)
    result = StreamResponse(resp: toResponse(npc.h2.respSnapshot(sid)), client: client,
                            key: key, pc: npc, sid: sid, decompress: decompress, cap: cap, capped: initCappedDecoder(decompress, cap))
  else:
    let parser = h1SendAndReadHeaders(transport, rq, true)
    result = StreamResponse(resp: parser.toResponse(), client: client, key: key,
                            pc: npc, parser: parser, decompress: decompress, cap: cap, capped: initCappedDecoder(decompress, cap))

proc stream*(client: Navi, verb: HttpVerb, target: string,
             headers = initHeaders(), params: seq[(string, string)] = @[],
             cancel: CancelToken = nil): StreamResponse =
  ## Open a streaming response: perform the request, follow redirects and digest
  ## auth to the final response, and return a handle whose status/headers are
  ## available immediately while the body streams on demand via `each`/`drain`.
  ##
  ## Unlike `request`, this does NOT throw on a non-2xx status (inspect `status`),
  ## and middleware is not applied. Redirect/digest hops are opened as streams and
  ## their bodies discarded (their connections closed). Consume the returned handle
  ## with `each`/`drain`, or `close` it if you decide not to read the body.
  var rreq = buildRequest(client.config, verb, target, headers, params = params)
  let digestOrigin = originKey(rreq.url)   # digest creds only for this origin
  var hops = 0
  let limit = client.config.redirectLimit
  while true:
    throwIfCancelled(cancel)
    applyCookies(client.jar, rreq)
    let handle = openStream(client, rreq)
    handle.cancel = cancel
    when defined(naviHttp3):
      # Learn h3 from a streamed response too, so SSE/stream upgrade on a reconnect.
      if client.altSvc != nil:
        let alt = handle.resp.headers.get("alt-svc")
        if alt.len > 0:
          client.altSvc.record("https", rreq.url.host, rreq.url.port, alt)
    # Arm the leak-guard: if the handle is dropped without drain/close, close its
    # connection. Captures only the connection essentials (not `handle`, which cycles).
    var armed = false
    when defined(naviHttp3):
      if handle.qc != nil:
        let qc = handle.qc
        let sid = handle.h3sid
        handle.guard = newStreamGuard(proc() {.gcsafe, raises: [].} =
          {.cast(gcsafe).}:
            try: qc.freeStream(sid); qc.close()
            except Exception: discard)
        armed = true
    if not armed:
      let pc = handle.pc
      handle.guard = newStreamGuard(proc() {.gcsafe, raises: [].} =
        {.cast(gcsafe).}:
          try: pc.transport.close()
          except Exception: discard)   # best-effort finalizer: never propagate
    storeCookies(client.jar, rreq.url, handle.resp)
    # 401 Digest challenge: re-open with an Authorization header (mirrors the
    # buffered path's maybeDigest), discarding the challenge body. The origin
    # check keeps digest credentials from being answered to a cross-origin
    # redirect target: redirectRequest strips Authorization on a cross-origin
    # hop, so without this the "no authorization header" test would pass and
    # digest would bypass that protection.
    if handle.status == 401 and client.config.auth.kind == akDigest and
       originKey(rreq.url) == digestOrigin and
       not rreq.headers.contains("authorization"):
      let chal = bestChallenge(handle.headers.getAll("www-authenticate"))
      if chal.isSome:
        let auth = digestAuthHeader(client.config.auth.user, client.config.auth.pass,
                                    $rreq.verb, rreq.url.requestTarget, chal.get)
        if auth.len > 0:
          handle.close()
          rreq.headers["authorization"] = auth
          continue
    let location = handle.headers.get("location")
    if shouldFollowRedirect(handle.status, hops, limit, location):
      handle.close()
      rreq = redirectRequest(rreq, handle.status, location)
      inc hops
    else:
      return handle

proc readChunk*(sr: StreamResponse): string =
  ## Pull the next decoded body chunk, or "" once the body is fully read. At end the
  ## connection is returned to the pool (or closed) and the guard disarmed, exactly
  ## as `drain` does; a size-cap breach or an h2 reset closes the connection and
  ## reraises. Call it until it returns "". This is the break-friendly pull form
  ## (a `while (let c = sr.readChunk(); c.len > 0)` loop) that the SSE reader builds
  ## on; `drain`/`each` remain the push form.
  if sr.phase != spOpen: return ""
  when defined(naviHttp3):
    if sr.qc != nil:
      # h3 body arrives raw; apply the same streamed decode + size-cap as h1, then
      # close the per-stream connection at EOF (a reset surfaces as an error). The
      # guard does freeStream + close, so closeNow both frees and tears down.
      try:
        while true:
          let raw = sr.qc.readStreamBody(sr.h3sid)
          if raw.len == 0:                       # EOF
            sr.phase = spDrained
            let wasReset = sr.qc.streamWasReset(sr.h3sid)
            let tooLarge = sr.qc.streamTooLarge(sr.h3sid)         # before the guard frees it
            let lengthBad = sr.qc.streamLengthMismatch(sr.h3sid)  # before the guard frees it
            closeNow(sr.guard)
            if tooLarge:
              raise newException(ResponseTooLargeError,
                "navi: response exceeded maxResponseBytes")
            if wasReset: raise newException(IOError, "navi: http/3 stream reset")
            if lengthBad: raise newException(IOError, h3BodyLengthErr)
            if not sr.capped.streamComplete:       # compressed stream cut short mid-decode
              raise newException(IOError, truncatedBodyErr)
            return ""
          let decoded = sr.capped.feed(raw,
            if sr.capped.encodingResolved: "" else: sr.resp.headers.get("content-encoding"))
          if decoded.len == 0: continue          # decoder buffered input; pull more
          return decoded
      except CatchableError:
        if sr.phase == spOpen: sr.phase = spDrained
        closeNow(sr.guard)
        raise
  try:
    if sr.pc.h2 != nil:
      result = h2ReadChunk(sr.pc.transport, sr.pc.h2, sr.sid, sr.capped)
      if result.len == 0:                       # end of stream
        sr.phase = spDrained
        if sr.pc.h2.canReuse and pushIdle(sr.client.pool, sr.key, sr.pc): disarm(sr.guard)
        else: closeNow(sr.guard)
    else:
      result = h1ReadChunk(sr.pc.transport, sr.parser, sr.capped)
      if result.len == 0:                       # end of body
        sr.phase = spDrained
        if sr.parser.keepAliveAfter() and pushIdle(sr.client.pool, sr.key, sr.pc):
          disarm(sr.guard)
        else: closeNow(sr.guard)
  except CatchableError:
    if sr.phase == spOpen: sr.phase = spDrained  # consumed; the guard must not re-close
    closeNow(sr.guard)
    raise

proc drain*(sr: StreamResponse, sink: BodySink) =
  ## Deliver the response body to `sink` as it arrives (decoded and size-capped),
  ## then return the connection to the pool (or close it if it cannot be reused).
  ## Consumes the handle: call once. On error the connection is closed and the
  ## error re-raised. Prefer the `each` template for the common case.
  ##
  ## `readChunk` owns the per-transport decode, size-cap, EOF teardown (pool-return
  ## or close), and error handling for h1/h2/h3 alike, so drain is one loop over it
  ## for every transport (as the h3 arm always was) rather than a per-protocol ladder.
  if sr.phase != spOpen:
    raise newException(IOError, "navi: stream already drained or closed")
  throwIfCancelled(sr.cancel)
  try:
    while true:
      let c = sr.readChunk()
      if c.len == 0: break
      sink(c)
  except CatchableError:
    # readChunk already tears down (phase -> spDrained + closeNow) on its own
    # errors; a sink error escapes it with the connection still open (phase still
    # spOpen), so close it here. Guarding on spOpen avoids a double closeNow.
    if sr.phase == spOpen:
      sr.phase = spDrained
      closeNow(sr.guard)
    raise

template each*(sr: StreamResponse; chunk, body: untyped): untyped =
  ## Drain the streaming body, running `body` for each decoded chunk with `chunk`
  ## bound to it (an owned `string`, moved from navi's read buffer, no copy):
  ##   let res = api.stream(GET, url)
  ##   res.each(chunk): outFile.write(chunk)
  ## Sugar over `drain`; the connection is returned/closed when the body is done.
  ##
  ## `body` runs as a proc, so `break`/`continue`/`return` cannot escape the loop
  ## from inside it. To stop early, either don't call `each` and `close` the handle,
  ## or raise from `body` (which closes the connection and propagates out of `each`).
  sr.drain(proc(chunk: string) {.raises: [CatchableError].} = body)
