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
import navi/proto/h1
import navi/proto/h2/conn
import navi/backend/sync

claimEntry("navi")
export public

type
  Hook* = proc(ctx: HookCtx) {.closure.}
    ## Lifecycle callback: mutate `ctx.request` (beforeRequest), read/mutate
    ## `ctx.response` (afterResponse), or read `ctx.attempt` (beforeRetry).
  Hooks* = object
    beforeRequest*: seq[Hook]
    afterResponse*: seq[Hook]
    beforeRetry*: seq[Hook]

  NaviOptions* = object of NaviOptionsBase
    hooks*: Hooks   ## lifecycle callbacks (sync)

  Navi* = object
    options*: NaviOptions
    pool*: Pool[PooledConn[Conn]]
    jar*: CookieJar

proc defaultOptions*(): NaviOptions =
  result.http = {H1, H2} # negotiate h2 over TLS via ALPN, fall back to h1
  result.tls = defaultTls()

proc mergeHooks(base, add: Hooks): Hooks =
  Hooks(beforeRequest: base.beforeRequest & add.beforeRequest,
        afterResponse: base.afterResponse & add.afterResponse,
        beforeRetry: base.beforeRetry & add.beforeRetry)

proc runHook(hook: Hook, ctx: HookCtx) =
  {.cast(gcsafe).}: hook(ctx)

proc newNavi*(options = defaultOptions()): Navi =
  ## Create a client. `options` supplies defaults (prefixUrl, headers, TLS,
  ## hooks, …).
  Navi(options: options, pool: newPool[PooledConn[Conn]](), jar: newCookieJar())

proc extend*(client: Navi, options: NaviOptions): Navi =
  ## Derive a new client, layering `options` over this client's (hooks are
  ## appended). The derived client gets its own connection pool and cookie jar.
  var merged = mergeBase(client.options, options)
  merged.hooks = mergeHooks(client.options.hooks, options.hooks)
  Navi(options: merged, pool: newPool[PooledConn[Conn]](), jar: newCookieJar())

proc transport(client: Navi, req: Request, sink: BodySink): Response =
  ## Pool-based transport (one request per connection at a time).
  poolTransport(client, req, sink)

proc request*(client: Navi, verb: HttpVerb, target: string,
              headers = initHeaders(), body = "", json: JsonNode = nil,
              form: seq[(string, string)] = @[],
              bodyStream: BodyProducer = nil): Response =
  ## Perform a request and return the response. `json`/`form` encode the body;
  ## `bodyStream` uploads a chunked body from a pull-based producer.
  let req = buildRequest(client.options, verb, target, headers, body, json,
                         form, bodyStream)
  performRequest(client, req)

proc stream*(client: Navi, verb: HttpVerb, target: string, sink: BodySink,
             headers = initHeaders()): Response =
  ## Perform a request and deliver the response body to `sink` as it arrives.
  ## The returned Response carries status and headers but an empty body.
  let req = buildRequest(client.options, verb, target, headers)
  performStream(client, req, sink)

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
  let alpn = if client.options.wantsH2 and url0.isTls: @["h2", "http/1.1"] else: @[]

  var (found, pc) = popIdle(client.pool, origin)
  var transport: Conn
  var h2: H2Conn
  if found:
    transport = pc.transport
    h2 = pc.h2
  else:
    transport = connect(url0.host, url0.port, url0.isTls, client.options.tls,
                        resolveProxy(client.options, url0), alpn, client.options.timeoutMs)
    pc = PooledConn[Conn](transport: transport)
    if transport.protocol == "h2":
      h2 = initH2Conn()
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
        transport = connect(url0.host, url0.port, url0.isTls, client.options.tls,
                            resolveProxy(client.options, url0), alpn, client.options.timeoutMs)
        pc = PooledConn[Conn](transport: transport)

proc parallel*(client: Navi, targets: openArray[string]): seq[Response] =
  ## Fetch many URLs (GET) concurrently. Same-origin requests are multiplexed
  ## over one HTTP/2 connection when the server supports h2, otherwise run
  ## sequentially. Each response is still put through the policy layer: cookies,
  ## decompression, redirects, retries, and hooks. Non-2xx responses are
  ## returned (not raised) so every result is available; inspect `.ok`.
  result.setLen(targets.len)
  var pending: seq[BatchItem]
  for i, target in targets:
    let ctx = HookCtx(request: buildRequest(client.options, GET, target))
    for hook in client.options.hooks.beforeRequest: runHook(hook, ctx)
    pending.add BatchItem(idx: i, req: ctx.request)

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
        decodeBody(resp, client.options)
        storeCookies(client.jar, item.req.url, resp)
        block:
          let ctx = HookCtx(request: item.req, response: resp)
          for hook in client.options.hooks.afterResponse: runHook(hook, ctx)
          resp = ctx.response
        let location = resp.headers.get("location")
        if client.options.redirectLimit > 0 and item.hops < client.options.redirectLimit and
           isRedirect(resp.status) and location.len > 0:
          item.req = redirectRequest(item.req, resp.status, location)
          inc item.hops
          nextRound.add item
        elif item.attempt < client.options.retryLimit and
             isRetryableVerb(item.req.verb) and isRetryableStatus(resp.status):
          inc item.attempt
          block:
            let ctx = HookCtx(request: item.req, attempt: item.attempt)
            for hook in client.options.hooks.beforeRetry: runHook(hook, ctx)
          backoff = max(backoff, backoffMs(item.attempt, resp))
          nextRound.add item
        else:
          result[item.idx] = resp
    if backoff > 0: sleep(backoff)
    pending = nextRound
