## The request algorithm, written once and shared by every engine backend.
##
## `performRequest` is a template so it can expand inside both a plain proc
## (sync) and an `{.async.}` proc (asyncdispatch/chronos). The transport ops
## (`connect`, `sendAll`, `recvSome`, `close`) and `await` are resolved at the
## instantiation site: real await in async backends, an identity template in
## the sync one.

import ./url, ./response
import ../proto/h1

template performRequest*(client, req: typed): Response =
  mixin connect, sendAll, recvSome, close, await
  block:
    let conn = await connect(req.url.host, req.url.port, req.url.isTls,
                             client.options.tls)
    var resp: Response
    try:
      await sendAll(conn, serializeRequest(req))
      var parser = initH1Parser()
      while not parser.finished:
        let chunk = await recvSome(conn)
        if chunk.len == 0:
          parser.eof()
          break
        parser.feed(chunk)
      resp = parser.toResponse()
    finally:
      await close(conn)
    resp
