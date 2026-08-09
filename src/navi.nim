## navi — synchronous entry point.
##
##   import navi
##   let api = newNavi()
##   let res = api.get("http://example.com")
##   echo res.status, " ", res.body
##
## For async, import `navi/asyncdispatch` or `navi/chronos` instead (exactly
## one entry module per program).

import std/tables
import navi/private/entryguard
import navi/core/public
import navi/core/[engine, pool, session, decompress, redirect, retry, proxy, h2glue]
import navi/core/[cookies, digest, cancel, url]
import navi/proto/h1
import navi/proto/h2/conn
import navi/proto/ws
import navi/backend/sync
from std/strutils import startsWith, find, splitLines, strip, cmpIgnoreCase, contains

claimEntry("navi")
export public

type
  NaviContext* = ref object
    ## Carried through the middleware chain. A middleware reads and mutates it,
    ## then calls `next` to run the rest of the chain (which fills `res`).
    req*: Request            ## the outgoing request; modify it before `next`
    res*: Response           ## the response; set by `next`, adjust it after
    clientv: Navi            ## the owning client (see `client`)
    sink: BodySink           ## non-nil for a streaming request
    cancel: CancelToken      ## caller's cancellation token, or nil
    idx: int                 ## index of the next middleware to run
  NaviMiddleware* = proc(ctx: NaviContext) {.closure.}
    ## A middleware step: read/modify `ctx.req`, call `ctx.next()` to proceed --
    ## or skip it to short-circuit -- then read/modify `ctx.res`. Run in order;
    ## index 0 is the outermost. A closure, so it can capture: write a factory
    ## `proc bearer(token: string): NaviMiddleware` that returns a step closing over
    ## `token`.

  NaviConfig* {.requiresInit.} = object of NaviConfigBase
    ## `requiresInit`, so it cannot be built with a bare/partial `NaviConfig(...)`
    ## (which would leave fields zeroed, e.g. verify off). Build it with
    ## `initNaviConfig()`.
    middleware*: seq[NaviMiddleware]

  Navi* = ref object
    config: NaviConfig
    pool*: Pool[PooledConn[Conn]]
    jar*: CookieJar

proc initNaviConfig*(): NaviConfig =
  ## The only way to build a config: `NaviConfig` requires every field. Sets the
  ## safe defaults (verify on, decompress on, 2 retries, 20 redirects); override
  ## the fields you want, then pass it to `newNavi`.
  NaviConfig(
    prefixUrl: "", headers: initHeaders(), http: {H1, H2}, tls: defaultTls(),
    decompress: true, throwHttpErrors: true, maxRedirects: 20,
    retry: defaultRetryPolicy(), maxResponseBytes: 0,
    auth: Auth(), proxy: "", timeout: 0, timeouts: Timeouts(), middleware: @[])

proc newNavi*(config = initNaviConfig()): Navi =
  ## Create a client. `config` supplies defaults (prefixUrl, headers, TLS,
  ## middleware, …).
  var cfg = config
  if cfg.tls.sessionCache.isNil: cfg.tls.sessionCache = newTlsStore(cfg.tls)
  Navi(config: cfg, pool: newPool[PooledConn[Conn]](), jar: newCookieJar())

proc config*(client: Navi): lent NaviConfig = client.config
  ## Read-only view of the client's config. Config is fixed at construction;
  ## build a fresh client (or `extend`) to change it rather than mutating a live
  ## one, so its pooled connections stay consistent with its settings.

proc extend*(client: Navi, config: NaviConfig): Navi =
  ## Derive a new client, layering `config` over this client's (middleware is
  ## appended). The derived client gets its own connection pool and cookie jar.
  var merged = mergeBase(client.config, config)
  merged.middleware = client.config.middleware & config.middleware
  merged.tls.sessionCache = newTlsStore(merged.tls)  # its own cache, not the parent's
  Navi(config: merged, pool: newPool[PooledConn[Conn]](), jar: newCookieJar())

proc close*(client: Navi) =
  ## Close all idle pooled connections, freeing their TLS contexts and cached
  ## sessions. Optional but recommended when done with a client: a later request
  ## just opens fresh connections. Without it, pooled connections are reclaimed
  ## only at process exit (and their OpenSSL contexts leak until then).
  for pc in client.pool.drain():
    pc.transport.close()
  closeTlsStore(client.config.tls.sessionCache)

proc transport(client: Navi, req: Request, sink: BodySink): Response =
  ## Pool-based transport (one request per connection at a time).
  poolTransport(client, req, sink)

proc runCore(client: Navi, req: Request, cancel: CancelToken): Response =
  ## The innermost `next`: the full policy layer for one buffered request.
  performRequest(client, req, cancel)

proc runCoreStream(client: Navi, req: Request, sink: BodySink,
                   cancel: CancelToken): Response =
  performStream(client, req, sink, cancel)

proc client*(ctx: NaviContext): Navi = ctx.clientv
  ## The client handling this request (e.g. to read `ctx.client.config`).

proc next*(ctx: NaviContext) =
  ## Run the rest of the chain: the next middleware, or -- once they are
  ## exhausted -- the request itself. The outcome lands in `ctx.res`.
  let mws = ctx.clientv.config.middleware
  if ctx.idx >= mws.len:
    ctx.res =
      if ctx.sink.isNil: runCore(ctx.clientv, ctx.req, ctx.cancel)
      else: runCoreStream(ctx.clientv, ctx.req, ctx.sink, ctx.cancel)
  else:
    let m = mws[ctx.idx]
    inc ctx.idx
    m(ctx)

proc request*(client: Navi, verb: HttpVerb, target: string,
              headers = initHeaders(), body = "", json: JsonNode = nil,
              form: seq[(string, string)] = @[], multipart: Multipart = @[],
              bodyStream: BodyProducer = nil,
              params: seq[(string, string)] = @[],
              cancel: CancelToken = nil): Response =
  ## Perform a request and return the response. `json`/`form`/`multipart` encode
  ## the body; `bodyStream` uploads a chunked body from a pull-based producer.
  ## `params` are appended to the URL query; `cancel` aborts the request.
  ## Configured middleware wraps the whole call.
  let req = buildRequest(client.config, verb, target, headers, body, json,
                         form, multipart, bodyStream, params)
  if client.config.middleware.len == 0: return runCore(client, req, cancel)
  let ctx = NaviContext(req: req, clientv: client, cancel: cancel)
  ctx.next()
  ctx.res

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
    drained: bool              ## body fully read; connection returned or closed
    closed: bool               ## connection disposed without draining
  StreamResponse* = ref StreamResponseObj

proc disposeConn(o: StreamResponseObj) =
  ## Close the underlying connection. A partially-read response cannot be pooled,
  ## so we always close (h2: closing the connection drops the pending stream). Only
  ## call when the handle owns the connection (not drained, not already closed).
  try: o.pc.transport.close()
  except CatchableError: discard

proc `=destroy`(o: StreamResponseObj) =
  ## Backstop: a handle dropped without draining must not leak its connection.
  if not (o.drained or o.closed): disposeConn(o)

proc close*(sr: StreamResponse) =
  ## Dispose a streaming handle whose body will not be fully drained: closes the
  ## underlying connection (a partially-read response cannot be safely pooled).
  ## Idempotent, and a no-op once the body has been drained.
  if sr.drained or sr.closed: return
  sr.closed = true
  disposeConn(sr[])

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
  rq.absoluteForm = proxy.isSet and not rq.url.isTls
  let alpn = if client.config.wantsH2 and rq.url.isTls: @["h2", "http/1.1"] else: @[]
  let key = originKey(rq.url)
  let decompress = client.config.wantsDecompress
  let cap = client.config.maxResponseBytes

  var (found, pc) = popIdle(client.pool, key)
  if found:
    try:
      if pc.h2 != nil:
        let sid = h2SendAndReadHeaders(pc.transport, pc.h2, rq)
        return StreamResponse(resp: toResponse(pc.h2.respSnapshot(sid)), client: client,
                              key: key, pc: pc, sid: sid, decompress: decompress, cap: cap)
      else:
        let parser = h1SendAndReadHeaders(pc.transport, rq, true)
        return StreamResponse(resp: parser.toResponse(), client: client, key: key,
                              pc: pc, parser: parser, decompress: decompress, cap: cap)
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
                            key: key, pc: npc, sid: sid, decompress: decompress, cap: cap)
  else:
    let parser = h1SendAndReadHeaders(transport, rq, true)
    result = StreamResponse(resp: parser.toResponse(), client: client, key: key,
                            pc: npc, parser: parser, decompress: decompress, cap: cap)

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
  var hops = 0
  let limit = client.config.redirectLimit
  while true:
    throwIfCancelled(cancel)
    applyCookies(client.jar, rreq)
    let handle = openStream(client, rreq)
    handle.cancel = cancel
    storeCookies(client.jar, rreq.url, handle.resp)
    # 401 Digest challenge: re-open with an Authorization header (mirrors the
    # buffered path's maybeDigest), discarding the challenge body.
    if handle.status == 401 and client.config.auth.kind == akDigest and
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
    if limit > 0 and hops < limit and isRedirect(handle.status) and location.len > 0:
      handle.close()
      rreq = redirectRequest(rreq, handle.status, location)
      inc hops
    else:
      return handle

proc drain*(sr: StreamResponse, sink: BodySink) =
  ## Deliver the response body to `sink` as it arrives (decoded and size-capped),
  ## then return the connection to the pool (or close it if it cannot be reused).
  ## Consumes the handle: call once. On error the connection is closed and the
  ## error re-raised. Prefer the `each` template for the common case.
  if sr.drained or sr.closed:
    raise newException(IOError, "navi: stream already drained or closed")
  throwIfCancelled(sr.cancel)
  try:
    if sr.pc.h2 != nil:
      h2DrainBody(sr.pc.transport, sr.pc.h2, sr.sid, sink, sr.decompress, sr.cap)
      sr.drained = true
      if not (sr.pc.h2.canReuse and pushIdle(sr.client.pool, sr.key, sr.pc)):
        disposeConn(sr[])
    else:
      var keep = false
      h1DrainBody(sr.pc.transport, sr.parser, sink, keep, sr.decompress, sr.cap)
      sr.drained = true
      if not (keep and pushIdle(sr.client.pool, sr.key, sr.pc)):
        disposeConn(sr[])
  except CatchableError:
    if not sr.drained: sr.drained = true   # mark consumed so =destroy won't double-close
    disposeConn(sr[])
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

include navi/private/verbs

# --- Parallel batch requests (HTTP/2 multiplexing) ---

type BatchItem = object
  idx: int          ## position in the caller's target list
  req: Request
  attempt, hops: int

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
    h2 = pc.h2
  else:
    transport = connect(url0.host, url0.port, url0.isTls, client.config.tls,
                        resolveProxy(client.config, url0), alpn,
                        client.config.connectMs, client.config.readMs, client.config.totalMs)
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
      ## Open as many queued requests as the peer's stream limit allows now.
      while sidK.len < h2.maxConcurrentStreams and opened < members.len:
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
          parser.eof()
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
                            client.config.connectMs, client.config.readMs, client.config.totalMs)
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

  while pending.len > 0:
    for pi in 0 ..< pending.len:
      applyCookies(client.jar, pending[pi].req)

    var groups: OrderedTable[string, seq[int]]
    for pi in 0 ..< pending.len:
      groups.mgetOrPut(originKey(pending[pi].req.url), @[]).add pi

    var nextRound: seq[BatchItem]
    var backoff = 0
    for origin, members in groups:
      let raw = client.transportGroup(pending, members)
      for k in 0 ..< members.len:
        var item = pending[members[k]]
        var resp = raw[k]
        decodeBody(resp, client.config)
        storeCookies(client.jar, item.req.url, resp)
        let location = resp.headers.get("location")
        if client.config.redirectLimit > 0 and item.hops < client.config.redirectLimit and
           isRedirect(resp.status) and location.len > 0:
          item.req = redirectRequest(item.req, resp.status, location)
          inc item.hops
          nextRound.add item
        elif item.attempt < client.config.retry.limit and
             isRetryableVerb(item.req.verb, client.config.retry) and
             isRetryableStatus(resp.status, client.config.retry):
          inc item.attempt
          backoff = max(backoff, backoffMs(item.attempt, resp, client.config.retry))
          nextRound.add item
        else:
          result[item.idx] = resp
    if backoff > 0: sleep(backoff)
    pending = nextRound

# --- WebSocket (RFC 6455) ---

export ws.WsMessage, ws.WsMessageKind, ws.closeNormal, ws.closeGoingAway

type
  WebSocket* = ref object
    conn: Conn
    dec: WsDecoder
    asmb: WsAssembler
    open: bool

proc toWsUrl(url: string): Url =
  var s = url
  if s.startsWith("ws://"): s = "http://" & s["ws://".len .. ^1]
  elif s.startsWith("wss://"): s = "https://" & s["wss://".len .. ^1]
  parseUrl(s)

proc websocket*(client: Navi, url: string, headers = initHeaders()): WebSocket =
  ## Open a WebSocket connection (RFC 6455). Accepts `ws://` / `wss://` (or
  ## http/https); `wss` uses TLS. Does the HTTP/1.1 Upgrade over the transport and
  ## validates `Sec-WebSocket-Accept`. Use `send`, `receive`, and `close`.
  let u = toWsUrl(url)
  let conn = connect(u.host, u.port, u.isTls, client.config.tls,
                     resolveProxy(client.config, u), @[], client.config.connectMs)
  # Close the connection unless the handshake completes -- a send/recv error mid
  # handshake would otherwise leak the socket (and its SSL_CTX for wss).
  var handshakeOk = false
  defer:
    if not handshakeOk:
      try: conn.close()
      except CatchableError: discard
  let key = genKey()
  conn.sendAll(upgradeRequest(u, key, headers))
  var buf = ""
  while "\r\n\r\n" notin buf:
    let chunk = conn.recvSome()
    if chunk.len == 0:
      raise newException(IOError, "navi: websocket handshake closed by peer")
    buf.add chunk
  let headEnd = buf.find("\r\n\r\n") + 4
  if not validate101(buf[0 ..< headEnd], key):
    raise newException(IOError, "navi: websocket upgrade rejected: " &
      buf[0 ..< headEnd].splitLines[0])
  result = WebSocket(conn: conn, open: true)
  if buf.len > headEnd:                 # server frames already buffered
    result.dec.feed(buf[headEnd .. ^1])
  handshakeOk = true

proc send*(ws: WebSocket, data: string, binary = false) =
  ## Send a text (default) or binary message. Client frames are masked.
  ws.conn.sendAll(encodeFrame(if binary: opBinary else: opText, data))

proc ping*(ws: WebSocket, data = "") =
  ws.conn.sendAll(encodeFrame(opPing, data))

proc receive*(ws: WebSocket): WsMessage =
  ## Block until a full message arrives, answering pings and reassembling
  ## fragments. A close returns `wmClose` (and the connection is then closed).
  while true:
    var f: Frame
    while not ws.dec.next(f):
      let chunk = ws.conn.recvSome()
      if chunk.len == 0:
        ws.open = false
        return WsMessage(kind: wmClose, closeCode: closeGoingAway)
      ws.dec.feed(chunk)
    let o = ws.asmb.offer(f)
    case o.reply
    of wrPong:
      ws.conn.sendAll(encodeFrame(opPong, o.replyPayload))
    of wrCloseEcho:
      if ws.open:
        try: ws.conn.sendAll(encodeFrame(opClose, o.replyPayload))
        except CatchableError: discard
        ws.open = false
        ws.conn.close()
    of wrNone: discard
    if o.ready: return o.message

proc close*(ws: WebSocket, code = closeNormal, reason = "") =
  ## Send a close frame and close the transport (freeing its TLS context).
  ## Idempotent: a no-op once the socket is already closed.
  if not ws.open: return
  try: ws.conn.sendAll(encodeFrame(opClose, closePayload(code, reason)))
  except CatchableError: discard
  ws.open = false
  ws.conn.close()
