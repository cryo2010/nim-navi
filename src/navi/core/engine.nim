## The request algorithm, written once and shared by every engine backend.
##
## `performRequest` is a template so it can expand inside both a plain proc (sync)
## and an `{.async.}` proc (asyncdispatch/chronos). The transport ops (`connect`,
## `sendAll`, `recvSome`, `close`) and `await` are resolved at the instantiation
## site: real await in async backends, an identity template in the sync one. The
## exchange is also split into header-read (`h1SendAndReadHeaders`/
## `h2SendAndReadHeaders`) and body-drain (`h1DrainBody`/`h2DrainBody`) phases, so
## the pull-based `stream()` handle can return after the headers and drain later.
##
## Connections are pooled per origin (keep-alive). A connection taken from the
## pool may have been closed by the server in the meantime, so a failed reused
## attempt is retried once on a fresh connection.

import ./headers, ./url, ./request, ./response, ./pool, ./decompress, ./redirect,
       ./retry, ./cookies, ./proxy, ./session, ./h2glue, ./digest, ./cancel
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

template h1SendAndReadHeaders*(transport, req, streaming: typed): H1Parser =
  ## Send an HTTP/1.1 request and read up to the end of the response headers,
  ## returning the parser (status/headers available via `toResponse`; body bytes
  ## that arrived alongside the headers stay buffered in the parser for the drain).
  ## The header/body split lets a pull-based caller return a handle here and drain
  ## the body later.
  mixin await, sendAll, recvSome
  block:
    sendRequest(transport, req)
    let noBody = req.verb == HEAD          # a HEAD response never carries a body
    # positional args: `streaming` is a template param, so a named `streaming =`
    # would be hygienically renamed and not match initH1Parser's parameter.
    var parser = initH1Parser(streaming, noBody)
    while not parser.headersReady and not parser.finished:
      let chunk = await recvSome(transport)
      if chunk.len == 0: parser.eof(); break
      parser.feed(chunk)
    parser

template h1DrainBody*(transport, parser, sink, keep, decompress, cap: typed) =
  ## Read and parse the response body over `transport`. When `sink` is set the body
  ## is drained per read, decoded (if `decompress`), size-capped at `cap` decoded
  ## bytes, and `await`ed into the sink -- so a slow sink stalls the read loop
  ## (backpressure) instead of buffering, and the parser never holds the whole body.
  ## With a nil sink the body accumulates in the parser (buffered request). Sets
  ## `keep` to whether the connection may be reused. Body bytes buffered during the
  ## header read are delivered first.
  mixin await, recvSome, BodySink
  block:
    let streaming = not sink.isNil
    var dec: StreamDecoder = nil
    var decReady = false                    # decoder chosen once headers are in
    var seen = 0
    template deliver() =
      if streaming:
        let raw = parser.takeBody()
        if raw.len > 0:
          if not decReady:
            dec = if decompress: newStreamDecoder(parser.contentEncoding()) else: nil
            decReady = true
          let decoded =
            if dec != nil: dec.update(raw.toOpenArrayByte(0, raw.high)) else: raw
          if decoded.len > 0:
            seen += decoded.len
            if cap > 0 and seen > cap:
              raise newException(ResponseTooLargeError,
                "navi: response exceeded maxResponseBytes")
            # single-threaded client; the sink need not be gcsafe (see sendRequest).
            # `decoded` is navi's native body type (`string`), which the sink also
            # takes, so its last use here moves the buffer straight into the sink
            # (into the async env on the async backends) with no copy. The raises
            # cast discharges chronos's strict-raises obligation on the portable
            # (annotation-free) sink type, as the middleware path does.
            {.cast(gcsafe).}:
              {.cast(raises: [CatchableError]).}:
                await sink(decoded)
    deliver()                               # body read alongside the headers
    while not parser.finished:
      let chunk = await recvSome(transport)
      if chunk.len == 0: parser.eof()
      else: parser.feed(chunk)
      deliver()
      if chunk.len == 0: break
    keep = parser.keepAliveAfter()

template h1Exchange*(transport, req, sink, keep, decompress, cap: typed): Response =
  ## One HTTP/1.1 request/response over `transport` (send + read headers + drain
  ## the body), composed from the header/body split above. Sets `keep` to whether
  ## the connection may be reused; does not pool or close.
  block:
    mixin BodySink
    let streaming = not sink.isNil
    var parser = h1SendAndReadHeaders(transport, req, streaming)
    h1DrainBody(transport, parser, sink, keep, decompress, cap)
    parser.toResponse()

template h2Stream(transport, h2, req, sink, decompress, cap: typed): Response =
  ## One HTTP/2 request/response on a new stream of the shared connection `h2`.
  block:
    mixin BodySink
    let sid = h2.openStream()
    if req.bodyStream != nil:
      # Stream the request body: HEADERS now, then DATA frames pulled from the
      # producer. When the send window closes, read so the peer's WINDOW_UPDATE
      # releases more of the body; the producer is pulled only once the queued
      # bytes are on the wire, so buffered upload memory stays ~one chunk.
      await sendAll(transport, h2.encodeRequestHead(sid, h2HeaderList(req)))
      var sending = true
      while sending and h2.connError.len == 0 and not h2.streamDone(sid):
        if h2.sendDrained(sid):
          var chunk: string
          {.cast(gcsafe).}: chunk = req.bodyStream()
          if chunk.len == 0:
            await sendAll(transport, h2.finishSend(sid))
            sending = false
          else:
            await sendAll(transport, h2.queueSend(sid, chunk))
        else:
          let inbound = await recvSome(transport)
          if inbound.len == 0: break
          let toSend = h2.feed(inbound)
          if toSend.len > 0: await sendAll(transport, toSend)
    else:
      await sendAll(transport, h2.encodeRequest(sid, h2HeaderList(req), req.body))
    # Deliver the body to the sink incrementally as DATA arrives (bounded memory),
    # or buffer it for a non-streaming request. The decoder is built once the
    # response headers are in (so content-encoding is known); the loop runs once
    # more after the END_STREAM feed, so the final chunk is delivered too. The sink
    # is `await`ed, so a slow sink stalls this read loop and, in turn, the peer
    # (backpressure). On the buffered path `takeBody` is never called, so the whole
    # body accumulates in the connection as before.
    var dec: StreamDecoder = nil
    var decReady = false
    var seen = 0
    while not h2.streamDone(sid):
      let chunk = await recvSome(transport)
      if chunk.len == 0: break
      let toSend = h2.feed(chunk)
      if toSend.len > 0: await sendAll(transport, toSend)
      if not sink.isNil:
        let raw = h2.takeBody(sid)
        if raw.len > 0:
          if not decReady:
            dec = if decompress: newStreamDecoder(h2.respHeader(sid, "content-encoding"))
                  else: nil
            decReady = true
          let decoded =
            if dec != nil: dec.update(raw.toOpenArrayByte(0, raw.high)) else: raw
          if decoded.len > 0:
            seen += decoded.len
            if cap > 0 and seen > cap:
              raise newException(ResponseTooLargeError,
                "navi: response exceeded maxResponseBytes")
            {.cast(gcsafe).}:
              {.cast(raises: [CatchableError]).}:
                await sink(decoded)     # native body type -> moved in, no copy
    let wasReset = h2.streamReset(sid)
    let tooLarge = h2.streamTooLarge(sid)
    let unprocessed = h2.streamUnprocessed(sid)
    let connErr = h2.connError
    var r = toResponse(h2.takeResponse(sid))
    if connErr.len > 0:            # bad preface / oversized frame / unexpected push
      raise newException(IOError, "navi: http/2 " & connErr)
    if tooLarge:
      raise newException(ResponseTooLargeError,
        "navi: response exceeded maxResponseBytes")
    if unprocessed:                # REFUSED_STREAM / above GOAWAY: safe to retry
      raise newException(UnprocessedError, "navi: http/2 request not processed")
    if wasReset or r.status == 0:  # reset, or gone away before a response
      raise newException(IOError, "navi: http/2 request did not complete")
    if not sink.isNil: r.body = ""  # delivered incrementally above
    r

template h2SendAndReadHeaders*(transport, h2, req: typed): uint32 =
  ## Open an h2 stream, send the request (including a streamed upload body), and
  ## read frames until the final response headers arrive; returns the stream id.
  ## The header/body split lets a pull-based caller return a handle here and drain
  ## the body later. Raises if the stream fails before any headers (so the caller's
  ## retry/redirect loop can react), mirroring `h2Stream`'s terminal errors.
  mixin await, sendAll, recvSome
  block:
    let sid = h2.openStream()
    if req.bodyStream != nil:
      await sendAll(transport, h2.encodeRequestHead(sid, h2HeaderList(req)))
      var sending = true
      while sending and h2.connError.len == 0 and not h2.streamDone(sid):
        if h2.sendDrained(sid):
          var chunk: string
          {.cast(gcsafe).}: chunk = req.bodyStream()
          if chunk.len == 0:
            await sendAll(transport, h2.finishSend(sid))
            sending = false
          else:
            await sendAll(transport, h2.queueSend(sid, chunk))
        else:
          let inbound = await recvSome(transport)
          if inbound.len == 0: break
          let toSend = h2.feed(inbound)
          if toSend.len > 0: await sendAll(transport, toSend)
    else:
      await sendAll(transport, h2.encodeRequest(sid, h2HeaderList(req), req.body))
    while not h2.headersReady(sid) and not h2.streamDone(sid):
      let chunk = await recvSome(transport)
      if chunk.len == 0: break
      let toSend = h2.feed(chunk)
      if toSend.len > 0: await sendAll(transport, toSend)
    if not h2.headersReady(sid):            # stream died before a response
      let connErr = h2.connError
      let unprocessed = h2.streamUnprocessed(sid)
      discard h2.takeResponse(sid)
      if connErr.len > 0: raise newException(IOError, "navi: http/2 " & connErr)
      if unprocessed:
        raise newException(UnprocessedError, "navi: http/2 request not processed")
      raise newException(IOError, "navi: http/2 request did not complete")
    sid

template h2DrainBody*(transport, h2, sid, sink, decompress, cap: typed) =
  ## Drain an h2 response body to `sink` incrementally (bounded memory), decoding
  ## if `decompress` and enforcing `cap`. A slow sink stalls the read loop and, in
  ## turn, the peer (backpressure). Raises on reset / oversized / unprocessed, like
  ## `h2Stream`. Drops the stream when done. `sink` must be non-nil (pull path).
  mixin await, sendAll, recvSome, BodySink
  block:
    var dec: StreamDecoder = nil
    var decReady = false
    var seen = 0
    template deliver() =
      let raw = h2.takeBody(sid)
      if raw.len > 0:
        if not decReady:
          dec = if decompress: newStreamDecoder(h2.respHeader(sid, "content-encoding"))
                else: nil
          decReady = true
        let decoded =
          if dec != nil: dec.update(raw.toOpenArrayByte(0, raw.high)) else: raw
        if decoded.len > 0:
          seen += decoded.len
          if cap > 0 and seen > cap:
            raise newException(ResponseTooLargeError,
              "navi: response exceeded maxResponseBytes")
          {.cast(gcsafe).}:
            {.cast(raises: [CatchableError]).}:
              await sink(decoded)     # native body type -> moved in, no copy
    deliver()                         # body read alongside the headers
    while not h2.streamDone(sid):
      let chunk = await recvSome(transport)
      if chunk.len == 0: break
      let toSend = h2.feed(chunk)
      if toSend.len > 0: await sendAll(transport, toSend)
      deliver()
    let wasReset = h2.streamReset(sid)
    let tooLarge = h2.streamTooLarge(sid)
    let unprocessed = h2.streamUnprocessed(sid)
    let connErr = h2.connError
    discard h2.takeResponse(sid)       # body delivered; drop the stream
    if connErr.len > 0: raise newException(IOError, "navi: http/2 " & connErr)
    if tooLarge:
      raise newException(ResponseTooLargeError,
        "navi: response exceeded maxResponseBytes")
    if unprocessed:
      raise newException(UnprocessedError, "navi: http/2 request not processed")
    if wasReset:
      raise newException(IOError, "navi: http/2 request did not complete")

template poolTransport*(client, req, sink: typed): Response =
  ## Pool-based transport: reuse a pooled connection (http/1.1 or a persistent
  ## h2 connection) or open a fresh one, negotiating the protocol via ALPN.
  ## One request at a time per connection. Used by the sync and chronos entries.
  mixin connect, sendAll, recvSome, close, await, BodySink
  block:
    var rq = req
    let proxy = resolveProxy(client.config, rq.url)
    rq.absoluteForm = proxy.isSet and not rq.url.isTls
    let alpn = if client.config.wantsH2 and rq.url.isTls:
                 @["h2", "http/1.1"] else: @[]
    let key = originKey(rq.url)
    var resp: Response
    var served = false

    var (found, pc) = popIdle(client.pool, key)
    if found:
      try:
        if pc.h2 != nil:
          resp = h2Stream(pc.transport, pc.h2, rq, sink,
                          client.config.wantsDecompress, client.config.maxResponseBytes)
          if not (pc.h2.canReuse and pushIdle(client.pool, key, pc)):
            await close(pc.transport)
        else:
          var keep = false
          resp = h1Exchange(pc.transport, rq, sink, keep,
                            client.config.wantsDecompress, client.config.maxResponseBytes)
          if not (keep and pushIdle(client.pool, key, pc)):
            await close(pc.transport)
        served = true
      except CatchableError:
        await close(pc.transport)  # pooled connection was stale; fall through

    if not served:
      let transport = await connect(rq.url.host, rq.url.port, rq.url.isTls,
                                    client.config.tls, proxy, alpn,
                                    client.config.connectMs, client.config.readMs,
                                    client.config.totalMs)
      var npc = PooledConn[typeof(transport)](transport: transport)
      if transport.protocol == "h2":
        npc.h2 = initH2Conn(client.config.maxResponseBytes)
        await sendAll(transport, npc.h2.preamble())
        resp = h2Stream(transport, npc.h2, rq, sink,
                        client.config.wantsDecompress, client.config.maxResponseBytes)
        if not (npc.h2.canReuse and pushIdle(client.pool, key, npc)):
          await close(transport)
      else:
        var keep = false
        resp = h1Exchange(transport, rq, sink, keep,
                          client.config.wantsDecompress, client.config.maxResponseBytes)
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
  mixin BodySink
  if resp.status == 401 and client.config.auth.kind == akDigest and
     not rreq.headers.contains("authorization"):
    let chal = bestChallenge(resp.headers.getAll("www-authenticate"))
    if chal.isSome:
      let auth = digestAuthHeader(
        client.config.auth.user, client.config.auth.pass,
        $rreq.verb, rreq.url.requestTarget, chal.get)
      if auth.len > 0:                 # "" means the challenge algorithm is unsupported
        rreq.headers["authorization"] = auth
        resp = run(client, rreq, BodySink(nil))

template followRedirects(client, startReq, resp: typed) =
  ## Issue `startReq`, following redirects into `resp`. Expands inline so its
  ## `await`s run in the caller's async proc.
  mixin BodySink
  var rreq = startReq
  var hops = 0
  let limit = client.config.redirectLimit
  while true:
    resp = run(client, rreq, BodySink(nil))
    maybeDigest(client, rreq, resp)
    decodeBody(resp, client.config)
    let location = resp.headers.get("location")
    if limit > 0 and hops < limit and isRedirect(resp.status) and location.len > 0:
      rreq = redirectRequest(rreq, resp.status, location)
      inc hops
    else:
      break

template performRequest*(client, req0: typed; cancel: CancelToken = nil): Response =
  ## Buffered request with the full policy layer: retries with backoff, redirect
  ## following, decompression, size cap, and throw-on-non-2xx. Middleware (which
  ## can wrap, short-circuit, or observe) is composed around this by the entry
  ## module. `cancel` is checked between attempts (cooperative on the sync
  ## backend; the async backends also abort in-flight via their guard).
  mixin sleep, BodySink
  block:
    var req = req0
    var resp: Response
    var attempt = 0
    let policy = client.config.retry
    while true:
      throwIfCancelled(cancel)
      var gotResp = false
      try:
        followRedirects(client, req, resp)
        gotResp = true
      except CatchableError as e:
        # A provably-unprocessed request (h2 REFUSED_STREAM / above GOAWAY) is
        # safe to retry even when non-idempotent.
        let retryable = isRetryableVerb(req.verb, policy) or (e of UnprocessedError)
        if not (attempt < policy.limit and retryable):
          raise # not retryable: propagate the transport error
      if gotResp and
         not (attempt < policy.limit and isRetryableVerb(req.verb, policy) and
              isRetryableStatus(resp.status, policy)):
        break
      inc attempt
      await sleep(backoffMs(attempt, resp, policy))
    enforceMaxResponse(resp, client.config.maxResponseBytes)
    if client.config.wantsThrow and not resp.ok:
      raiseHttpError(req, resp)
    resp
