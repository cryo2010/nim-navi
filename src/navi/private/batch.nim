## Parallel batch requests over HTTP/2 multiplexing.
## `include`d by navi.nim (the sync entry); shares its imports, the `Navi`
## type, and the pooled-transport engine. Not a standalone module.

# --- Parallel batch requests (HTTP/2 multiplexing) ---

type BatchItem = object
  idx: int          ## position in the caller's target list
  req: Request
  attempt, hops: int
  lastResp: Response  ## the most recent response; returned in place of a retry that
                      ## the total-deadline budget no longer allows (issue #359)

proc transportGroup(client: Navi, items: seq[BatchItem],
                    members: seq[int]): seq[Response] =
  ## Raw transport for one origin's requests (no policy). Multiplexes over a
  ## single h2 connection when negotiated, else runs them sequentially on
  ## reused http/1.1 connections.
  result.setLen(members.len)
  let url0 = items[members[0]].req.url
  let origin = originKey(url0)
  let alpn = if client.config.wantsH2 and url0.isTls: @["h2", "http/1.1"] else: @[]

  var (found, pc) = popIdle(client.pool, origin)
  var transport: Conn
  var h2: H2Conn
  if found:
    transport = pc.transport
    # A pooled connection adopts the CURRENT config timeouts, not the ones it was
    # opened with, honoring navi's live-config contract (issue #360).
    rearm(transport, client.config.readMs,
          totalMsFor(client.config, items[members[0]].req))
    h2 = pc.h2
  else:
    transport = connect(url0.host, url0.port, url0.isTls, client.config.tls,
                        resolveProxy(client.config, url0), alpn,
                        client.config.connectMs, client.config.readMs,
                        totalMsFor(client.config, items[members[0]].req))
    pc = PooledConn[Conn](transport: transport)
    if transport.protocol == "h2":
      h2 = initH2Conn(client.config.maxResponseBytes)
      pc.h2 = h2
      transport.sendAll(h2.preamble())

  if h2 != nil:
    # On a fresh connection the server's SETTINGS (carrying MAX_CONCURRENT_
    # STREAMS) arrive first; read them before opening streams so a large batch
    # honors the cap from the start instead of over-committing and having the
    # excess streams reset. A pooled connection already processed its settings.
    if not found:
      let chunk = transport.recvSome()
      if chunk.len > 0:
        let toSend = h2.feed(chunk)
        if toSend.len > 0: transport.sendAll(toSend)

    var sidK: Table[uint32, int]   ## in-flight stream id -> index into `members`
    var opened = 0                 ## members whose stream has been opened
    var completed = 0

    proc openMore() =
      ## Open as many queued requests as the peer's stream limit allows now. Stop once
      ## the peer sent GOAWAY: a stream above its last-stream-id would be ignored (RFC
      ## 9113 6.8), so leave the rest for a retry on a fresh connection.
      while not h2.goneAway and sidK.len < h2.maxConcurrentStreams and opened < members.len:
        let sid = h2.openStream()
        sidK[sid] = opened
        transport.sendAll(h2.encodeRequest(sid, h2HeaderList(items[members[opened]].req),
                                           items[members[opened]].req.body))
        inc opened

    openMore()
    while completed < members.len:
      let chunk = transport.recvSome()
      if chunk.len == 0: break                 # peer closed mid-batch
      let toSend = h2.feed(chunk)
      if toSend.len > 0: transport.sendAll(toSend)
      var finished: seq[uint32]
      for sid in sidK.keys:
        if h2.streamDone(sid): finished.add sid
      for sid in finished:
        result[sidK[sid]] = toResponse(h2.takeResponse(sid))
        sidK.del(sid)
        inc completed
      openMore()                               # a freed slot admits the next request
    if not (h2.canReuse and pushIdle(client.pool, origin, pc)):
      transport.close()
  else:
    for k in 0 ..< members.len:
      transport.sendAll(serializeRequest(items[members[k]].req))
      var parser = initH1Parser()
      while not parser.finished:
        let chunk = transport.recvSome()
        if chunk.len == 0:
          parser.eof()                       # completes a read-until-close body
          # A length- or chunked-delimited body that isn't `finished` at EOF was cut
          # short by a premature close. Raise rather than return the partial body as a
          # complete response (silent truncation), mirroring h1DrainBody in engine.nim.
          if not parser.finished:
            raise newException(IOError, h1TruncatedErr)
          break
        parser.feed(chunk)
      result[k] = parser.toResponse()
      let keep = parser.keepAliveAfter()
      if k == members.len - 1:
        if not (keep and pushIdle(client.pool, origin, pc)): transport.close()
      elif not keep:
        transport.close()
        transport = connect(url0.host, url0.port, url0.isTls, client.config.tls,
                            resolveProxy(client.config, url0), alpn,
                            client.config.connectMs, client.config.readMs,
                            totalMsFor(client.config, items[members[k]].req))
        pc = PooledConn[Conn](transport: transport)

proc parallel*(client: Navi, targets: openArray[string]): seq[Response] =
  ## Fetch many URLs (GET) concurrently. Same-origin requests are multiplexed
  ## over one HTTP/2 connection when the server supports h2, otherwise run
  ## sequentially, each through the policy layer (cookies, decompression,
  ## redirects, retries). Non-2xx responses are returned (not raised) so every
  ## result is available; inspect `.ok`.
  ##
  ## When middleware is configured it wraps each request individually, which
  ## forgoes the shared-connection multiplexing (every request stands alone).
  result.setLen(targets.len)
  if client.config.middleware.len > 0:
    for i, target in targets:
      let ctx = NaviContext(req: buildRequest(client.config, GET, target),
                        clientv: client)
      try:
        ctx.next()
        result[i] = ctx.res
      except HttpError as e:
        result[i] = e.response          # keep parallel's non-throwing contract
    return

  var pending: seq[BatchItem]
  for i, target in targets:
    pending.add BatchItem(idx: i, req: buildRequest(client.config, GET, target))

  # `config.timeouts.total` bounds the whole batch (every round, its connects, and
  # the backoff sleeps between rounds), the same contract the single-request engine
  # honors via `RetryDeadline` (issue #359). Armed once, before the first round.
  var deadline = initRetryDeadline(client.config.totalMs)
  while pending.len > 0:
    let remaining = deadline.attemptBudgetMs   # this round's connects get what remains
    for pi in 0 ..< pending.len:
      applyCookies(client.jar, pending[pi].req)
      pending[pi].req.deadlineMs = remaining

    var groups: OrderedTable[string, seq[int]]
    for pi in 0 ..< pending.len:
      groups.mgetOrPut(originKey(pending[pi].req.url), @[]).add pi

    var nextRound: seq[BatchItem]
    var retriers: seq[BatchItem]   # items that want another attempt after a backoff
    var backoff = 0
    for origin, members in groups:
      let raw = client.transportGroup(pending, members)
      for k in 0 ..< members.len:
        var item = pending[members[k]]
        var resp = raw[k]
        decodeBody(resp, client.config)
        storeCookies(client.jar, item.req.url, resp)
        let location = resp.headers.get("location")
        # Same redirect gate and retry predicate the single-request engine uses
        # (followRedirects / shouldRetryAfterResponse in core), applied per batch
        # item so the shared connection can be reused for the round that follows.
        if shouldFollowRedirect(resp.status, item.hops, client.config.redirectLimit, location):
          item.req = redirectRequest(item.req, resp.status, location)
          inc item.hops
          nextRound.add item
        elif shouldRetryAfterResponse(item.attempt, resp.status, isReplayable(item.req),
                                      item.req.verb, client.config.retry):
          inc item.attempt
          backoff = max(backoff, backoffMs(item.attempt, resp, client.config.retry))
          item.lastResp = resp   # surface this if the deadline forbids the retry
          retriers.add item
        else:
          result[item.idx] = resp
    # Bound the inter-round backoff by the remaining budget. When it is exhausted (or
    # a backoff sleep would overrun it) the retry candidates give up and their last
    # response is returned, matching the single-request engine's `if gotResp: break`
    # and keeping `parallel`'s non-throwing contract.
    let sleepMs = backoffWithinDeadline(deadline, backoff)
    if sleepMs < 0:
      for item in retriers: result[item.idx] = item.lastResp
    else:
      if sleepMs > 0: sleep(sleepMs)
      for item in retriers: nextRound.add item
    pending = nextRound
