## The request algorithm, written once and shared by every engine backend.
##
## `performRequest` is a template so it can expand inside both a plain proc
## (sync) and an `{.async.}` proc (asyncdispatch/chronos). The transport ops
## (`connect`, `sendAll`, `recvSome`, `close`) and `await` are resolved at the
## instantiation site: real await in async backends, an identity template in
## the sync one.
##
## Connections are pooled per origin (keep-alive). A connection taken from the
## pool may have been closed by the server in the meantime, so a failed reused
## attempt is retried once on a fresh connection.

import ./url, ./response, ./pool
import ../proto/h1

template roundTrip(client, req, conn, key: typed): Response =
  ## Send one request over `conn`, read the response, then either return the
  ## connection to the pool or close it. Expands inline, so its `await`s run in
  ## the caller's async proc.
  block:
    await sendAll(conn, serializeRequest(req))
    var parser = initH1Parser()
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

template performRequest*(client, req: typed): Response =
  mixin connect, sendAll, recvSome, close, await
  block:
    let key = originKey(req.url)
    var resp: Response
    var (reused, conn) = popIdle(client.pool, key)
    var needFresh = not reused
    if reused:
      try:
        resp = roundTrip(client, req, conn, key)
      except CatchableError:
        await close(conn)
        needFresh = true  # pooled connection was stale; try once more
    if needFresh:
      conn = await connect(req.url.host, req.url.port, req.url.isTls,
                           client.options.tls)
      resp = roundTrip(client, req, conn, key)
    resp
