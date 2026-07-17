## The request algorithm, written once and shared by every engine backend.
##
## `performRequest`/`performStream` are templates so they can expand inside both
## a plain proc (sync) and an `{.async.}` proc (asyncdispatch/chronos). The
## transport ops (`connect`, `sendAll`, `recvSome`, `close`) and `await` are
## resolved at the instantiation site: real await in async backends, an identity
## template in the sync one.
##
## Connections are pooled per origin (keep-alive). A connection taken from the
## pool may have been closed by the server in the meantime, so a failed reused
## attempt is retried once on a fresh connection.

import ./headers, ./url, ./request, ./response, ./pool, ./decompress, ./redirect,
       ./retry, ./cookies, ./proxy, ./session, ./h2glue, ./digest
import ../proto/h1
import ../proto/h2/conn

proc raiseHttpError(req: Request, resp: Response) =
  raise (ref HttpError)(
    msg: $req.verb & " " & $req.url & " -> " & $resp.status & " " & resp.reason,
    response: resp)

template sendRequest(conn, req: typed) =
  ## Write the request, streaming the body as chunked transfer-encoding when a
  ## producer is set, otherwise sending it buffered.
  if req.bodyStream != nil:
    await sendAll(conn, serializeHead(req, chunked = true))
    while true:
      # single-threaded client; the producer need not be gcsafe (see h1.emitBody)
      var chunk: string
      {.cast(gcsafe).}:
        chunk = req.bodyStream()
      if chunk.len == 0: break
      await sendAll(conn, encodeChunk(chunk))
    await sendAll(conn, chunkTerminator)
  else:
    await sendAll(conn, serializeRequest(req))

template h1Exchange*(transport, req, sink, keep, decompress: typed): Response =
  ## One HTTP/1.1 request/response over `transport`. Sets `keep` to whether the
  ## connection may be reused; does not pool or close. When `sink` is set and
  ## `decompress` is on, body chunks are decompressed as they arrive.
  block:
    sendRequest(transport, req)
    var parser =
      if not sink.isNil and decompress:
        initH1Parser(sinkFactory = proc(h: Headers): BodySink =
          decodingSink(h.get("content-encoding"), sink))
      else:
        initH1Parser(sink)
    while not parser.finished:
      let chunk = await recvSome(transport)
      if chunk.len == 0:
        parser.eof()
        break
      parser.feed(chunk)
    keep = parser.keepAliveAfter()
    parser.toResponse()

template h2Stream(transport, h2, req, sink, decompress: typed): Response =
  ## One HTTP/2 request/response on a new stream of the shared connection `h2`.
  block:
    let sid = h2.openStream()
    await sendAll(transport, h2.encodeRequest(sid, h2HeaderList(req), req.body))
    while not h2.streamDone(sid):
      let chunk = await recvSome(transport)
      if chunk.len == 0: break
      let toSend = h2.feed(chunk)
      if toSend.len > 0: await sendAll(transport, toSend)
    let wasReset = h2.streamReset(sid)
    var r = toResponse(h2.takeResponse(sid))
    if wasReset or r.status == 0:  # reset, or gone away before a response
      raise newException(IOError, "navi: http/2 request did not complete")
    if not sink.isNil and r.body.len > 0:
      # The h2 body arrives buffered in the connection, so decode it in one pass
      # here (consistent with the incremental h1 streaming path).
      var payload = r.body
      if decompress:
        let dec = newStreamDecoder(r.headers.get("content-encoding"))
        if dec != nil: payload = dec.update(payload.toOpenArrayByte(0, payload.high))
      {.cast(gcsafe).}: sink(payload.toOpenArrayByte(0, payload.high))
      r.body = ""
    r

template poolTransport*(client, req, sink: typed): Response =
  ## Pool-based transport: reuse a pooled connection (http/1.1 or a persistent
  ## h2 connection) or open a fresh one, negotiating the protocol via ALPN.
  ## One request at a time per connection. Used by the sync and chronos entries.
  mixin connect, sendAll, recvSome, close, await
  block:
    var rq = req
    let proxy = resolveProxy(client.options, rq.url)
    rq.absoluteForm = proxy.isSet and not rq.url.isTls
    let alpn = if client.options.wantsH2 and rq.url.isTls:
                 @["h2", "http/1.1"] else: @[]
    let key = originKey(rq.url)
    var resp: Response
    var served = false

    var (found, pc) = popIdle(client.pool, key)
    if found:
      try:
        if pc.h2 != nil:
          resp = h2Stream(pc.transport, pc.h2, rq, sink, client.options.wantsDecompress)
          if not (pc.h2.canReuse and pushIdle(client.pool, key, pc)):
            await close(pc.transport)
        else:
          var keep = false
          resp = h1Exchange(pc.transport, rq, sink, keep, client.options.wantsDecompress)
          if not (keep and pushIdle(client.pool, key, pc)):
            await close(pc.transport)
        served = true
      except CatchableError:
        await close(pc.transport)  # pooled connection was stale; fall through

    if not served:
      let transport = await connect(rq.url.host, rq.url.port, rq.url.isTls,
                                    client.options.tls, proxy, alpn,
                                    client.options.timeoutMs)
      var npc = PooledConn[typeof(transport)](transport: transport)
      if transport.protocol == "h2":
        npc.h2 = initH2Conn()
        await sendAll(transport, npc.h2.preamble())
        resp = h2Stream(transport, npc.h2, rq, sink, client.options.wantsDecompress)
        if not (npc.h2.canReuse and pushIdle(client.pool, key, npc)):
          await close(transport)
      else:
        var keep = false
        resp = h1Exchange(transport, rq, sink, keep, client.options.wantsDecompress)
        if not (keep and pushIdle(client.pool, key, npc)):
          await close(transport)
    resp

template run(client, req, sink: typed): Response =
  ## Cookie handling around the backend's transport step. `transport` is
  ## resolved per entry: pool-based for sync/chronos, mux-based for asyncdispatch.
  mixin transport, await
  block:
    var rq = req
    applyCookies(client.jar, rq)
    var resp = await transport(client, rq, sink)
    storeCookies(client.jar, rq.url, resp)
    resp

template maybeDigest(client, rreq, resp: typed) =
  ## On a 401 Digest challenge, when digest auth is configured and the request
  ## carries no Authorization yet, compute the response and retry once. Expands
  ## inline so the retry's `await`s run in the caller's async proc.
  if resp.status == 401 and client.options.auth.kind == akDigest and
     not rreq.headers.contains("authorization"):
    let chal = parseChallenge(resp.headers.get("www-authenticate"))
    if chal.isSome:
      let auth = digestAuthHeader(
        client.options.auth.user, client.options.auth.pass,
        $rreq.verb, rreq.url.requestTarget, chal.get)
      if auth.len > 0:                 # "" means the challenge algorithm is unsupported
        rreq.headers["authorization"] = auth
        resp = run(client, rreq, BodySink(nil))

template followRedirects(client, startReq, resp: typed) =
  ## Issue `startReq`, following redirects into `resp`. Expands inline so its
  ## `await`s run in the caller's async proc.
  var rreq = startReq
  var hops = 0
  let limit = client.options.redirectLimit
  while true:
    resp = run(client, rreq, BodySink(nil))
    maybeDigest(client, rreq, resp)
    decodeBody(resp, client.options)
    let location = resp.headers.get("location")
    if limit > 0 and hops < limit and isRedirect(resp.status) and location.len > 0:
      rreq = redirectRequest(rreq, resp.status, location)
      inc hops
    else:
      break

template performRequest*(client, req0: typed): Response =
  ## Buffered request with the full policy layer: beforeRequest hooks, retries
  ## with backoff, redirect following, decompression, afterResponse hooks, and
  ## throw-on-non-2xx.
  mixin sleep, runHook
  block:
    var req = req0
    block:
      let ctx = HookCtx(request: req)
      for hook in client.options.hooks.beforeRequest: await runHook(hook, ctx)
      req = ctx.request
    var resp: Response
    var attempt = 0
    let maxRetries = client.options.retryLimit
    while true:
      var gotResp = false
      try:
        followRedirects(client, req, resp)
        gotResp = true
      except CatchableError:
        if not (attempt < maxRetries and isRetryableVerb(req.verb)):
          raise # not retryable: propagate the transport error
      if gotResp and
         not (attempt < maxRetries and isRetryableVerb(req.verb) and
              isRetryableStatus(resp.status)):
        break
      inc attempt
      block:
        let ctx = HookCtx(request: req, attempt: attempt)
        for hook in client.options.hooks.beforeRetry: await runHook(hook, ctx)
        req = ctx.request
      await sleep(backoffMs(attempt, resp))
    block:
      let ctx = HookCtx(request: req, response: resp)
      for hook in client.options.hooks.afterResponse: await runHook(hook, ctx)
      resp = ctx.response
    if client.options.wantsThrow and not resp.ok:
      raiseHttpError(req, resp)
    resp

template performStream*(client, req, sink: typed): Response =
  ## Streaming request: body chunks are delivered to `sink` as they arrive and
  ## `Response.body` is left empty. Chunks are delivered as received (not
  ## decompressed); a non-2xx still raises HttpError unless disabled.
  block:
    let resp = run(client, req, sink)
    if client.options.wantsThrow and not resp.ok:
      raiseHttpError(req, resp)
    resp
