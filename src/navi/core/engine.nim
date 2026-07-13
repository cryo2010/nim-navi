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

import std/strutils
import ./headers, ./url, ./request, ./response, ./pool, ./decompress, ./redirect,
       ./retry, ./cookies, ./proxy, ./session
import ../proto/h1
import ../proto/h2/[conn, hpack]

proc raiseHttpError(req: Request, resp: Response) =
  raise (ref HttpError)(
    msg: $req.verb & " " & $req.url & " -> " & $resp.status & " " & resp.reason,
    response: resp)

proc h2HeaderList(req: Request): seq[HeaderPair] =
  ## Pseudo-headers first, then regular headers (lowercased, connection-specific
  ## fields dropped, Host replaced by :authority).
  result.add((":method", $req.verb))
  result.add((":scheme", if req.url.isTls: "https" else: "http"))
  result.add((":path", req.url.requestTarget))
  var authority = req.url.host
  let p = req.url.port
  if not ((req.url.isTls and p == 443) or (not req.url.isTls and p == 80)):
    authority.add(":" & $p)
  result.add((":authority", authority))
  for (name, value) in req.headers.pairs:
    let lower = name.toLowerAscii
    if lower in ["host", "connection", "keep-alive", "proxy-connection",
                 "transfer-encoding", "upgrade"]:
      continue
    result.add((lower, value))

proc toResponse(r: H2Response): Response =
  result.status = r.status
  result.httpVersion = "HTTP/2"
  result.body = r.body
  for (name, value) in r.headers:
    result.headers.add(name, value)

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

template h1Exchange(transport, req, sink, keep: typed): Response =
  ## One HTTP/1.1 request/response over `transport`. Sets `keep` to whether the
  ## connection may be reused; does not pool or close.
  block:
    sendRequest(transport, req)
    var parser = initH1Parser(sink)
    while not parser.finished:
      let chunk = await recvSome(transport)
      if chunk.len == 0:
        parser.eof()
        break
      parser.feed(chunk)
    keep = parser.keepAliveAfter()
    parser.toResponse()

template h2Stream(transport, h2, req, sink: typed): Response =
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
      {.cast(gcsafe).}: sink(r.body.toOpenArrayByte(0, r.body.high))
      r.body = ""
    r

template run(client, req, sink: typed): Response =
  mixin connect, sendAll, recvSome, close, await
  block:
    var rq = req
    applyCookies(client.jar, rq)
    let proxy = resolveProxy(client.options, rq.url)
    rq.absoluteForm = proxy.isSet and not rq.url.isTls
    let alpn = if client.options.wantsH2 and rq.url.isTls:
                 @["h2", "http/1.1"] else: @[]
    let key = originKey(rq.url)
    var resp: Response
    var served = false

    # 1. Reuse a pooled connection (http/1.1 or a persistent h2 connection).
    var (found, pc) = popIdle(client.pool, key)
    if found:
      try:
        if pc.h2 != nil:
          resp = h2Stream(pc.transport, pc.h2, rq, sink)
          if not (pc.h2.canReuse and pushIdle(client.pool, key, pc)):
            await close(pc.transport)
        else:
          var keep = false
          resp = h1Exchange(pc.transport, rq, sink, keep)
          if not (keep and pushIdle(client.pool, key, pc)):
            await close(pc.transport)
        served = true
      except CatchableError:
        await close(pc.transport)  # pooled connection was stale; fall through

    # 2. Open a fresh connection, negotiating the protocol via ALPN.
    if not served:
      let transport = await connect(rq.url.host, rq.url.port, rq.url.isTls,
                                    client.options.tls, proxy, alpn)
      var npc = PooledConn[typeof(transport)](transport: transport)
      if transport.protocol == "h2":
        npc.h2 = initH2Conn()
        await sendAll(transport, npc.h2.preamble())
        resp = h2Stream(transport, npc.h2, rq, sink)
        if not (npc.h2.canReuse and pushIdle(client.pool, key, npc)):
          await close(transport)
      else:
        var keep = false
        resp = h1Exchange(transport, rq, sink, keep)
        if not (keep and pushIdle(client.pool, key, npc)):
          await close(transport)

    storeCookies(client.jar, rq.url, resp)
    resp

template followRedirects(client, startReq, resp: typed) =
  ## Issue `startReq`, following redirects into `resp`. Expands inline so its
  ## `await`s run in the caller's async proc.
  var rreq = startReq
  var hops = 0
  let limit = client.options.redirectLimit
  while true:
    resp = run(client, rreq, BodySink(nil))
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
  mixin sleep
  block:
    var req = req0
    for hook in client.options.hooks.beforeRequest:
      {.cast(gcsafe).}: hook(req)
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
      for hook in client.options.hooks.beforeRetry:
        {.cast(gcsafe).}: hook(req, attempt)
      await sleep(backoffMs(attempt, resp))
    for hook in client.options.hooks.afterResponse:
      {.cast(gcsafe).}: hook(req, resp)
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
