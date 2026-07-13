## navi — synchronous entry point.
##
##   import navi
##   let api = newNavi()
##   let res = api.get("http://example.com")
##   echo res.status, " ", res.text()
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
  Navi* = object
    options*: NaviOptions
    pool*: Pool[PooledConn[Conn]]
    jar*: CookieJar

proc newNavi*(options = defaultOptions()): Navi =
  ## Create a client. `options` supplies defaults (prefixUrl, headers, TLS, …).
  Navi(options: options, pool: newPool[PooledConn[Conn]](), jar: newCookieJar())

proc extend*(client: Navi, options: NaviOptions): Navi =
  ## Derive a new client, layering `options` over this client's defaults.
  ## The derived client gets its own connection pool and cookie jar.
  Navi(options: mergeOptions(client.options, options),
       pool: newPool[PooledConn[Conn]](), jar: newCookieJar())

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
                        resolveProxy(client.options, url0), alpn)
    pc = PooledConn[Conn](transport: transport)
    if transport.protocol == "h2":
      h2 = initH2Conn()
      pc.h2 = h2
      transport.sendAll(h2.preamble())

  if h2 != nil:
    var streams: seq[uint32]
    for pi in members:
      let sid = h2.openStream()
      streams.add sid
      transport.sendAll(h2.encodeRequest(sid, h2HeaderList(items[pi].req),
                                         items[pi].req.body))
    while true:
      var remaining = 0
      for sid in streams:
        if not h2.streamDone(sid): inc remaining
      if remaining == 0: break
      let chunk = transport.recvSome()
      if chunk.len == 0: break
      let toSend = h2.feed(chunk)
      if toSend.len > 0: transport.sendAll(toSend)
    for k in 0 ..< streams.len:
      result[k] = toResponse(h2.takeResponse(streams[k]))
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
                            resolveProxy(client.options, url0), alpn)
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
    var req = buildRequest(client.options, GET, target)
    for hook in client.options.hooks.beforeRequest: hook(req)
    pending.add BatchItem(idx: i, req: req)

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
        for hook in client.options.hooks.afterResponse: hook(item.req, resp)
        let location = resp.headers.get("location")
        if client.options.redirectLimit > 0 and item.hops < client.options.redirectLimit and
           isRedirect(resp.status) and location.len > 0:
          item.req = redirectRequest(item.req, resp.status, location)
          inc item.hops
          nextRound.add item
        elif item.attempt < client.options.retryLimit and
             isRetryableVerb(item.req.verb) and isRetryableStatus(resp.status):
          inc item.attempt
          for hook in client.options.hooks.beforeRetry: hook(item.req, item.attempt)
          backoff = max(backoff, backoffMs(item.attempt, resp))
          nextRound.add item
        else:
          result[item.idx] = resp
    if backoff > 0: sleep(backoff)
    pending = nextRound
