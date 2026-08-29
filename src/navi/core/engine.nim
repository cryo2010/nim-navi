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

const h1TruncatedErr* =
  "navi: http/1.1 response truncated (connection closed before the body completed)"
const h2TruncatedErr* =
  "navi: http/2 response truncated (connection closed before the stream completed)"

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
    if not parser.headersReady and not parser.finished:
      # The peer closed before any response headers -- typically a pooled keep-alive
      # connection the server had already closed. Raise (rather than return a status-0
      # response) so the caller discards it and retries on a fresh connection.
      raise newException(IOError, "navi: http/1.1 connection closed before response")
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
      if chunk.len == 0: parser.eof()       # completes a read-until-close body
      else: parser.feed(chunk)
      deliver()
      if chunk.len == 0:
        # A length- or chunked-delimited body that isn't `finished` at EOF was cut
        # short by a premature close. Raise rather than return the partial body as a
        # complete response (silent truncation). `eof` already completed a
        # read-until-close body, so `finished` here means a clean end.
        if not parser.finished:
          raise newException(IOError, h1TruncatedErr)
        break
    keep = parser.keepAliveAfter()

template h1ReadChunk*(transport, parser, dec, decReady, seen, decompress, cap: typed): string =
  ## Pull the next decoded body chunk over `transport`, or "" at end of body.
  ## `dec`/`decReady`/`seen` hold the caller's persistent decode + size-cap state
  ## across calls (fields on the streaming handle). "" is returned only at true end
  ## of body; a decoder that buffers input without producing output loops for more.
  ## The caller does the terminal pool/close once "" comes back (`keepAliveAfter`
  ## is valid then). This is the single read/decode/cap path `drain` loops over.
  mixin await, recvSome
  block:
    var res = ""
    while true:
      let raw = parser.takeBody()
      if raw.len == 0:
        if parser.finished: break            # end of body: res stays ""
        let chunk = await recvSome(transport)
        if chunk.len == 0:
          parser.eof()                       # completes a read-until-close body
          # A length/chunked body not `finished` at EOF was cut short. Raise instead
          # of looping on a socket that keeps returning "" (a busy hang) or ending
          # silently with a truncated body.
          if not parser.finished:
            raise newException(IOError, h1TruncatedErr)
        else: parser.feed(chunk)
        continue
      if not decReady:
        dec = if decompress: newStreamDecoder(parser.contentEncoding()) else: nil
        decReady = true
      let decoded =
        if dec != nil: dec.update(raw.toOpenArrayByte(0, raw.high)) else: raw
      if decoded.len == 0: continue          # decoder buffered input; read more
      seen += decoded.len
      if cap > 0 and seen > cap:
        raise newException(ResponseTooLargeError,
          "navi: response exceeded maxResponseBytes")
      res = decoded
      break
    res

template h2ReadChunk*(transport, h2, sid, dec, decReady, seen, decompress, cap: typed): string =
  ## Pull the next decoded body chunk of an h2 stream over `transport` (the sync
  ## single-connection h2 path), or "" at end of stream, having dropped the stream.
  ## Sends any control frames the feed produces. Raises on reset / oversized /
  ## unprocessed / connection error, like the old drain loop. Persistent decode +
  ## cap state lives in `dec`/`decReady`/`seen`.
  mixin await, sendAll, recvSome
  block:
    var res = ""
    while true:
      let raw = h2.takeBody(sid)
      if raw.len > 0:
        if not decReady:
          dec = if decompress: newStreamDecoder(h2.respHeader(sid, "content-encoding"))
                else: nil
          decReady = true
        let decoded =
          if dec != nil: dec.update(raw.toOpenArrayByte(0, raw.high)) else: raw
        if decoded.len == 0: continue
        seen += decoded.len
        if cap > 0 and seen > cap:
          raise newException(ResponseTooLargeError,
            "navi: response exceeded maxResponseBytes")
        res = decoded
        break
      if h2.streamDone(sid):                  # no more body: terminal, drop the stream
        let wasReset = h2.streamReset(sid)
        let tooLarge = h2.streamTooLarge(sid)
        let unprocessed = h2.streamUnprocessed(sid)
        let connErr = h2.connError
        discard h2.takeResponse(sid)
        if connErr.len > 0: raise newException(IOError, "navi: http/2 " & connErr)
        if tooLarge:
          raise newException(ResponseTooLargeError,
            "navi: response exceeded maxResponseBytes")
        if unprocessed:
          raise newException(UnprocessedError, "navi: http/2 request not processed")
        if wasReset:
          raise newException(IOError, "navi: http/2 request did not complete")
        break                                 # clean end: res ""
      let chunk = await recvSome(transport)
      if chunk.len == 0:                       # transport EOF before END_STREAM:
        raise newException(IOError, h2TruncatedErr)   # truncated, not a clean end
      let toSend = h2.feed(chunk)
      if toSend.len > 0: await sendAll(transport, toSend)
    res

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
    let done = h2.streamDone(sid)  # END_STREAM seen (else the loop broke on transport EOF)
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
    if not done:                   # headers seen but the connection died mid-body:
      raise newException(IOError, h2TruncatedErr)   # don't return a partial body
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
    let done = h2.streamDone(sid)      # END_STREAM seen (else the loop broke on EOF)
    discard h2.takeResponse(sid)       # body delivered; drop the stream
    if connErr.len > 0: raise newException(IOError, "navi: http/2 " & connErr)
    if tooLarge:
      raise newException(ResponseTooLargeError,
        "navi: response exceeded maxResponseBytes")
    if unprocessed:
      raise newException(UnprocessedError, "navi: http/2 request not processed")
    if wasReset:
      raise newException(IOError, "navi: http/2 request did not complete")
    if not done:                       # connection died mid-body: don't truncate silently
      raise newException(IOError, h2TruncatedErr)

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
      # `gotResponse` splits a reused-connection failure into "before any response"
      # (the request never reached a working server -> unprocessed) vs "after the
      # response began" (the server processed it). h2 signals its own unprocessed
      # case via UnprocessedError, so treat its failures as post-response.
      var gotResponse = true
      try:
        if pc.h2 != nil:
          resp = h2Stream(pc.transport, pc.h2, rq, sink,
                          client.config.wantsDecompress, client.config.maxResponseBytes)
          if not (pc.h2.canReuse and pushIdle(client.pool, key, pc)):
            await close(pc.transport)
        else:
          var keep = false
          gotResponse = false
          var parser = h1SendAndReadHeaders(pc.transport, rq, not sink.isNil)
          gotResponse = true
          h1DrainBody(pc.transport, parser, sink, keep,
                      client.config.wantsDecompress, client.config.maxResponseBytes)
          resp = parser.toResponse()
          if not (keep and pushIdle(client.pool, key, pc)):
            await close(pc.transport)
        served = true
      except CatchableError as e:
        await close(pc.transport)  # pooled connection was stale
        # Fall through to a fresh connection only when replaying is safe. A reused
        # keep-alive connection can be dropped by the server at any time; a failure
        # BEFORE any response byte means the request was almost certainly not
        # processed (the classic keep-alive race), so it is safe to replay even when
        # non-idempotent. A failure AFTER the response began means the server did
        # process it, so only an idempotent method (or a proven-unprocessed peer
        # signal: h2 REFUSED_STREAM / above GOAWAY) may be replayed. A non-replayable
        # streamed body (`bodyStream`) is never retried (its producer cannot rewind).
        let replayable = req.bodyStream == nil
        if not (replayable and
                (not gotResponse or isIdempotent(req.verb) or (e of UnprocessedError))):
          raise

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
    validateRequest(rq)                # reject header/host CR-LF injection
    applyCookies(client.jar, rq)
    var resp = await transport(client, rq, sink)
    storeCookies(client.jar, rq.url, resp)
    resp

template maybeDigest(client, rreq, resp, digestOrigin: typed) =
  ## On a 401 Digest challenge, when digest auth is configured, the request
  ## carries no Authorization yet, and it is still on the origin the credentials
  ## were configured for, compute the response and retry once. The origin check
  ## keeps digest credentials from being answered to a cross-origin redirect
  ## target (mirroring the Authorization stripping in `redirectRequest`; without
  ## it, digest would bypass that protection since the strip clears the header the
  ## first condition tests). Expands inline so the retry's `await`s run in the
  ## caller's async proc.
  mixin BodySink
  if resp.status == 401 and client.config.auth.kind == akDigest and
     not rreq.headers.contains("authorization") and
     originKey(rreq.url) == digestOrigin:
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
  let digestOrigin = originKey(startReq.url)   # digest creds only for this origin
  var hops = 0
  let limit = client.config.redirectLimit
  while true:
    resp = run(client, rreq, BodySink(nil))
    maybeDigest(client, rreq, resp, digestOrigin)
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
    # A streamed request body (`bodyStream`) can't be rewound once its producer has
    # advanced, so replaying it would send a truncated body. Such a request is never
    # retried -- not even a provably-unprocessed one, since the producer may already
    # have been pulled during the attempt.
    let bodyReplayable = req.bodyStream == nil
    while true:
      throwIfCancelled(cancel)
      var gotResp = false
      try:
        followRedirects(client, req, resp)
        gotResp = true
      except CatchableError as e:
        # A provably-unprocessed request (h2 REFUSED_STREAM / above GOAWAY) is
        # safe to retry even when non-idempotent.
        let retryable = bodyReplayable and
          (isRetryableVerb(req.verb, policy) or (e of UnprocessedError))
        if not (attempt < policy.limit and retryable):
          raise # not retryable: propagate the transport error
      if gotResp and
         not (attempt < policy.limit and bodyReplayable and
              isRetryableVerb(req.verb, policy) and
              isRetryableStatus(resp.status, policy)):
        break
      inc attempt
      await sleep(backoffMs(attempt, resp, policy))
    enforceMaxResponse(resp, client.config.maxResponseBytes)
    if client.config.wantsThrow and not resp.ok:
      raiseHttpError(req, resp)
    resp
