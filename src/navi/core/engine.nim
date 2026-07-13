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

import ./url, ./request, ./response, ./pool, ./decompress, ./redirect, ./retry,
       ./cookies, ./proxy
import ../proto/h1

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

template roundTrip(client, req, conn, key, sink: typed): Response =
  ## Send one request over `conn`, read the response (body to `sink` if set,
  ## else buffered), then pool or close the connection. Expands inline so its
  ## `await`s run in the caller's async proc.
  block:
    sendRequest(conn, req)
    var parser = initH1Parser(sink)
    while not parser.finished:
      let chunk = await recvSome(conn)
      if chunk.len == 0:
        parser.eof()
        break
      parser.feed(chunk)
    let resp = parser.toResponse()
    if not (parser.keepAliveAfter() and pushIdle(client.pool, key, conn)):
      await close(conn)
    resp

template run(client, req, sink: typed): Response =
  mixin connect, sendAll, recvSome, close, await
  block:
    var rq = req
    applyCookies(client.jar, rq)
    let proxy = resolveProxy(client.options, rq.url)
    rq.absoluteForm = proxy.isSet and not rq.url.isTls
    let key = originKey(rq.url)
    var resp: Response
    var (reused, conn) = popIdle(client.pool, key)
    var needFresh = not reused
    if reused:
      try:
        resp = roundTrip(client, rq, conn, key, sink)
      except CatchableError:
        await close(conn)
        needFresh = true  # pooled connection was stale; try once more
    if needFresh:
      conn = await connect(rq.url.host, rq.url.port, rq.url.isTls,
                           client.options.tls, proxy)
      resp = roundTrip(client, rq, conn, key, sink)
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
