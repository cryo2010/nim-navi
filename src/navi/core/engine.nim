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

import ./url, ./request, ./response, ./pool, ./decompress
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
    let key = originKey(req.url)
    var resp: Response
    var (reused, conn) = popIdle(client.pool, key)
    var needFresh = not reused
    if reused:
      try:
        resp = roundTrip(client, req, conn, key, sink)
      except CatchableError:
        await close(conn)
        needFresh = true  # pooled connection was stale; try once more
    if needFresh:
      conn = await connect(req.url.host, req.url.port, req.url.isTls,
                           client.options.tls)
      resp = roundTrip(client, req, conn, key, sink)
    resp

template performRequest*(client, req: typed): Response =
  ## Buffered request: body read into `Response.body`, decompressed per
  ## Content-Encoding, then a non-2xx raises HttpError unless disabled.
  block:
    var resp = run(client, req, BodySink(nil))
    decodeBody(resp, client.options)
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
