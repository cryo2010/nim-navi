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
       ./retry, ./cookies, ./proxy, ./session, ./h2glue, ./digest, ./cancel,
       ./sinkgate
import ../proto/h1
import ../proto/h2/conn

const h1TruncatedErr* =
  "navi: http/1.1 response truncated (connection closed before the body completed)"
const h2TruncatedErr* =
  "navi: http/2 response truncated (connection closed before the stream completed)"
  # bodyLengthErr is defined in proto/h2/conn (shared with the h2 muxes).

proc raiseHttpError(req: Request, resp: Response) =
  raise (ref HttpError)(
    msg: $req.verb & " " & $req.url & " -> " & $resp.status & " " & resp.reason,
    response: resp)

template coalesceChunk(conn, pending, chunk: typed) =
  ## Hand one producer chunk to the streamed-upload write buffer (#299). Shared by
  ## the async-producer and sync `bodyStream` branches of `sendRequest`, which
  ## differ only in how they pull a chunk.
  ##
  ## A producer yielding many tiny chunks would otherwise cost one socket write --
  ## and, under TLS, one record with its own header and MAC -- per chunk, so small
  ## chunks accumulate in `pending` and leave as ONE wire chunk: chunk boundaries
  ## need not match producer boundaries (RFC 9112 7.1), only the framing must stay
  ## valid. The buffer is flushed BEFORE an append that would take it past
  ## `h1CoalesceSize`, so a framed buffer never exceeds one TLS record.
  ##
  ## Expanded inside `sendRequest`, itself expanded in an `{.async.}` proc on the
  ## async backends and in a plain proc on the sync one (where `await` is the
  ## identity template), so the `await`s below are correct in both.
  mixin await, sendAll
  if chunk.len >= h1CoalesceSize:
    # Already fills a write on its own: frame it directly instead of copying it
    # through the buffer. Anything still buffered is packed into the SAME write, so
    # making way for it costs no extra syscall or TLS record.
    var framed = newStringOfCap(pending.len + chunk.len + 40)
    framed.addChunk(pending)             # a no-op when nothing is buffered
    framed.addChunk(chunk)
    pending.setLen(0)
    await sendAll(conn, framed)
  else:
    if pending.len + chunk.len > h1CoalesceSize:
      await sendAll(conn, encodeChunk(pending))
      pending.setLen(0)
    pending.add(chunk)

template sendRequest(conn, req: typed; asyncStream: typed = nil) =
  ## Write the request, streaming the body as chunked transfer-encoding when a
  ## producer is set. A buffered body with trailers is also sent chunked (trailers
  ## only exist in chunked transfer-encoding); otherwise the body is sent buffered.
  ##
  ## `asyncStream` (async backends only) is an awaited pull-based producer: the send
  ## awaits it once per chunk, so producing a chunk can itself await (e.g. piping a
  ## streaming download into the upload). It outranks the sync `bodyStream`. On the
  ## sync backend the awaited branch is dropped at compile time (`await` of a
  ## Future-returning proc does not compile there), so the sync send stays identical.
  when compiles(await asyncStream()):
    let asyncBody = not asyncStream.isNil
  else:
    const asyncBody = false
  if asyncBody:
    when compiles(await asyncStream()):
      await sendAll(conn, serializeHead(req, chunked = true))
      # Small producer chunks are coalesced into one write (see coalesceChunk, #299).
      var pending = newStringOfCap(h1CoalesceSize)
      while true:
        # The producer is a bare closure (portable spelling, no chronos raises
        # annotation); navi's contract is it raises at most CatchableError. Discharge
        # chronos's strict gcsafe/raises obligation here, as the sink path does.
        var chunk: string
        {.cast(gcsafe).}:
          {.cast(raises: [CatchableError]).}:
            chunk = await asyncStream()
        if chunk.len == 0: break
        coalesceChunk(conn, pending, chunk)
      # Producer EOF: the terminator (plus any trailers) rides along with whatever is
      # still buffered, so a small streamed upload costs one body write in total.
      var tail = newStringOfCap(pending.len + 64)
      tail.addChunk(pending)
      tail.add(finalChunk(req))
      await sendAll(conn, tail)
  elif req.bodyStream != nil:
    await sendAll(conn, serializeHead(req, chunked = true))
    var pending = newStringOfCap(h1CoalesceSize)   # see coalesceChunk above
    while true:
      # single-threaded client; the producer need not be gcsafe (see h1.emitBody)
      var chunk: string
      {.cast(gcsafe).}:
        chunk = req.bodyStream()
      if chunk.len == 0: break
      coalesceChunk(conn, pending, chunk)
    var tail = newStringOfCap(pending.len + 64)
    tail.addChunk(pending)
    tail.add(finalChunk(req))
    await sendAll(conn, tail)
  elif req.trailers.len > 0:
    await sendAll(conn, serializeHead(req, chunked = true))
    var framed = newStringOfCap(req.body.len + 64)
    framed.addChunk(req.body)
    framed.add(finalChunk(req))
    await sendAll(conn, framed)
  else:
    # Send the head and body separately rather than `serializeHead(req) & req.body`,
    # which would allocate a whole (head + body)-sized buffer and copy the entire
    # upload just to prepend a ~200-byte head (repeated on every retry/redirect).
    await sendAll(conn, serializeHead(req))
    if req.body.len > 0:
      await sendAll(conn, req.body)

template h1SendAndReadHeaders*(transport, req, streaming: typed;
                               asyncStream: typed = nil): H1Parser =
  ## Send an HTTP/1.1 request and read up to the end of the response headers,
  ## returning the parser (status/headers available via `toResponse`; body bytes
  ## that arrived alongside the headers stay buffered in the parser for the drain).
  ## The header/body split lets a pull-based caller return a handle here and drain
  ## the body later. `asyncStream` (async backends only) is an awaited body producer
  ## passed through to `sendRequest`; nil on the sync path.
  mixin await, sendAll, recvSome
  block:
    # WRITE-TIME classification. A reused pooled connection the server RST while idle
    # fails at WRITE time with a plain transport error (sync "socket write failed" /
    # "SSL_write failed" IOError, asyncdispatch OSError, chronos AsyncStreamError) --
    # none of them a KeepAliveRaceError/UnprocessedError the replay layer accepts, so it
    # would decline the request even for an idempotent method. Wrap the send and classify
    # a transport write failure as the ambiguous keep-alive race (request written or
    # partially written, no response began), restoring the idempotent / Idempotency-Key
    # replay a pre-response write failure warrants (matching Go net/http; RFC 9110 9.2.2).
    #
    # Only reclassify a BUFFERED-body request: a streamed body (`bodyStream`/`asyncStream`)
    # is non-replayable (`isReplayable` is false), so the retry layer would decline a
    # replay regardless, and its send can raise from the user's producer -- which must keep
    # its own exception type, not become a race. A cancellation (chronos guard / CancelToken)
    # must also propagate untouched. `when declared(CancelledError)` resolves at the
    # instantiation site, so it is inert on the sync/asyncdispatch backends (no cancellation).
    var hasProducer = req.bodyStream != nil
    when compiles(await asyncStream()):
      if not asyncStream.isNil: hasProducer = true
    try:
      sendRequest(transport, req, asyncStream)
    except CatchableError as sendErr:
      when declared(CancelledError):
        if sendErr of CancelledError: raise
      if hasProducer: raise             # producer error / non-replayable streamed body
      raise newException(KeepAliveRaceError,
        "navi: http/1.1 send failed before any response: " & sendErr.msg)
    let noBody = req.verb == HEAD          # a HEAD response never carries a body
    # positional args: `streaming` is a template param, so a named `streaming =`
    # would be hygienically renamed and not match initH1Parser's parameter.
    var parser = initH1Parser(streaming, noBody)
    while not parser.headersReady and not parser.finished:
      let chunk = await recvSome(transport)
      if chunk.len == 0: parser.eof(); break
      parser.feed(chunk)
    if not parser.headersReady and not parser.finished:
      # The peer closed before any FINAL response headers. If a 1xx interim already
      # arrived (`responseBegan`) the peer demonstrably began replying, so this is a
      # truncation, not a race -- a plain IOError, never auto-replayed (the h1 analog of
      # the h2 `responseBegan` classification). Otherwise no response began: the keep-alive
      # race (a pooled connection the server had already closed, or a freshly-opened one
      # dropped before responding). Raise KeepAliveRaceError so the reused-connection path
      # replays it and the retry layer retries it once on a fresh connection (idempotent,
      # or any method with an Idempotency-Key).
      if parser.responseBegan:
        raise newException(IOError,
          "navi: http/1.1 connection closed after an interim response")
      raise newException(KeepAliveRaceError,
        "navi: http/1.1 connection closed before response")
    parser

template deliverChunk*(cd, sink, rawBody, encoding: typed) =
  ## Feed one raw body slice through the capped decoder `cd` and `await` any
  ## decoded output into `sink`. Shared by the h1/h2 drain loops. `encoding` is
  ## only consulted until the decoder resolves the content-encoding. The decoded
  ## buffer is navi's native body type (`string`), so its last use here moves it
  ## straight into the sink (into the async env on the async backends) with no
  ## copy; the raises cast discharges chronos's strict-raises obligation on the
  ## portable (annotation-free) sink type, as the middleware path does.
  mixin await
  let decoded = cd.feed(rawBody, if cd.encodingResolved: "" else: encoding)
  if decoded.len > 0:
    # single-threaded client; the sink need not be gcsafe (see sendRequest).
    {.cast(gcsafe).}:
      {.cast(raises: [CatchableError]).}:
        await sink(decoded)

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
    var cd = initCappedDecoder(decompress, cap)   # lazy decoder + running size cap
    template deliver() =
      if streaming:
        deliverChunk(cd, sink, parser.takeBody(), parser.contentEncoding())
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
    if streaming and not cd.streamComplete:   # compressed stream cut short mid-decode
      raise newException(IOError, truncatedBodyErr)
    keep = parser.keepAliveAfter()

template h1ReadChunk*(transport, parser, capped: typed): string =
  ## Pull the next decoded body chunk over `transport`, or "" at end of body.
  ## `capped` (a `var CappedDecoder` field on the streaming handle) holds the
  ## persistent decode + size-cap state across calls. "" is returned only at true
  ## end of body; a decoder that buffers input without producing output loops for
  ## more. The caller does the terminal pool/close once "" comes back
  ## (`keepAliveAfter` is valid then). The single read/decode/cap path `drain` loops
  ## over.
  mixin await, recvSome
  block:
    var res = ""
    while true:
      let raw = parser.takeBody()
      if raw.len == 0:
        if parser.finished:
          if not capped.streamComplete:      # compressed stream cut short mid-decode
            raise newException(IOError, truncatedBodyErr)
          break                              # end of body: res stays ""
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
      let decoded = capped.feed(raw,
                                if capped.encodingResolved: "" else: parser.contentEncoding())
      if decoded.len == 0: continue          # decoder buffered input; read more
      res = decoded
      break
    res

type H2Terminal* = object
  ## The terminal outcome of an h2 stream, captured by the caller before
  ## `takeResponse` clears the per-stream flags. Not a variant: these conditions
  ## are independent and checked in a fixed precedence order (a single
  ## discriminant would misrepresent them); the object exists so the seven
  ## captured signals travel as one named bundle instead of seven positional
  ## bools that are easy to transpose.
  connErr*: string          ## non-empty => connection-level error text
  tooLarge*: bool           ## response exceeded maxResponseBytes
  unprocessed*: bool        ## peer proved the request was not processed
  wasReset*: bool           ## RAW stream reset (RST_STREAM seen) -- NOT coerced with
                            ## `status == 0`, so the race branch below is not pre-empted
  responseBegan*: bool      ## ANY response HEADERS arrived (final OR a 1xx interim), so
                            ## a drop with no END_STREAM is a truncation, not a race
  noResponse*: bool         ## no final response status (`r.status == 0`): gone away before
                            ## a response. With `done` this is the abrupt-GOAWAY terminal;
                            ## without it, and with no response begun, it is the race
  done*: bool               ## END_STREAM seen (false => truncated mid-stream)
  lengthBad*: bool          ## content-length / DATA length mismatch
  decoderComplete*: bool    ## body decoder ended cleanly

template raiseH2Terminal*(t: H2Terminal) =
  ## The canonical "h2 stream reached a terminal point" -> exception cascade,
  ## shared by every h2 read path (`h2ReadChunk`, `h2Stream`, `h2DrainBody`) and the
  ## pre-header classifier in `h2SendAndReadHeaders`. Order is load-bearing: a
  ## connection error and the oversize/unprocessed/reset outcomes take precedence over
  ## the race/truncation checks. `done` is whether END_STREAM was seen (false => the
  ## peer died mid-stream); `decoderComplete` is whether the body decoder ended cleanly
  ## (a compressed body cut short is also a truncation). Falls through silently on a
  ## clean end.
  ##
  ## The keep-alive-race branch is the one consolidation point for every h2 read path:
  ## a drop with NO response begun (not even a 1xx interim), NO raw reset, NO connection
  ## error, and NO proven-unprocessed signal is the ambiguous race (request written, no
  ## response) -- `KeepAliveRaceError`, so the retry layer may replay it (idempotent, or
  ## any method with an Idempotency-Key). It sits AFTER the raw `wasReset` check (a
  ## pre-header RST is terminal, the peer may have processed it) and BEFORE the
  ## `noResponse` coercion (an abrupt GOAWAY with `done` set, or a 1xx-then-drop where a
  ## response DID begin, both stay a plain IOError). Every guard is load-bearing: `not
  ## responseBegan` keeps a begun-then-truncated response a truncation; `not done` keeps
  ## an abrupt GOAWAY(err) a plain reset.
  if t.connErr.len > 0: raise newException(IOError, "navi: http/2 " & t.connErr)
  if t.tooLarge:
    raise newException(ResponseTooLargeError,
      "navi: response exceeded maxResponseBytes")
  if t.unprocessed:
    raise newException(UnprocessedError, "navi: http/2 request not processed")
  if t.wasReset:
    raise newException(IOError, "navi: http/2 request did not complete")
  if not t.responseBegan and not t.done:
    raise newException(KeepAliveRaceError, "navi: http/2 connection closed")
  if t.noResponse:
    raise newException(IOError, "navi: http/2 request did not complete")
  if not t.done:
    raise newException(IOError, h2TruncatedErr)
  if t.lengthBad:
    raise newException(IOError, bodyLengthErr)
  if not t.decoderComplete:
    raise newException(IOError, truncatedBodyErr)

template h2ReadChunk*(transport, h2, sid, capped: typed): string =
  ## Pull the next decoded body chunk of an h2 stream over `transport` (the sync
  ## single-connection h2 path), or "" at end of stream, having dropped the stream.
  ## Sends any control frames the feed produces. Raises on reset / oversized /
  ## unprocessed / connection error, like the old drain loop. Persistent decode +
  ## cap state lives in `capped` (a `var CappedDecoder` field on the handle).
  mixin await, sendAll, recvSome
  block:
    var res = ""
    while true:
      let raw = h2.takeBody(sid)
      if raw.len > 0:
        let decoded = capped.feed(raw,
          if capped.encodingResolved: "" else: h2.respHeader(sid, "content-encoding"))
        if decoded.len == 0: continue
        res = decoded
        break
      if h2.streamDone(sid):                  # no more body: terminal, drop the stream
        let wasReset = h2.streamReset(sid)
        let tooLarge = h2.streamTooLarge(sid)
        let unprocessed = h2.streamUnprocessed(sid)
        let connErr = h2.connError
        let responseBegan = h2.responseBegan(sid)     # capture before takeResponse
        let lengthBad = h2.streamLengthMismatch(sid)  # capture before takeResponse
        discard h2.takeResponse(sid)
        # Reached only from the body-read path (headers were in), so END_STREAM was seen
        # (done=true) and a response began: the race branch never fires here.
        raiseH2Terminal(H2Terminal(connErr: connErr, tooLarge: tooLarge,
          unprocessed: unprocessed, wasReset: wasReset, responseBegan: responseBegan,
          noResponse: false, done: true,
          lengthBad: lengthBad, decoderComplete: capped.streamComplete))
        break                                 # clean end: res ""
      let chunk = await recvSome(transport)
      if chunk.len == 0:                       # transport EOF before END_STREAM:
        raise newException(IOError, h2TruncatedErr)   # truncated, not a clean end
      let toSend = h2.feed(chunk)
      if toSend.len > 0: await sendAll(transport, toSend)
    res

template h1Exchange*(transport, req, sink, keep, decompress, cap: typed;
                     asyncStream: typed = nil): Response =
  ## One HTTP/1.1 request/response over `transport` (send + read headers + drain
  ## the body), composed from the header/body split above. Sets `keep` to whether
  ## the connection may be reused; does not pool or close. `asyncStream` (async
  ## backends only) is an awaited body producer forwarded to the send; nil on sync.
  block:
    mixin BodySink
    let streaming = not sink.isNil
    var parser = h1SendAndReadHeaders(transport, req, streaming, asyncStream)
    h1DrainBody(transport, parser, sink, keep, decompress, cap)
    parser.toResponse()

template h2SendRequest*(transport, h2, req: typed): uint32 =
  ## Open a new h2 stream and send `req` on it, returning the stream id (response
  ## still to be read). A buffered body goes in one shot; a streamed body
  ## (`bodyStream`) is sent as DATA pulled from the producer, reading between frames
  ## so the peer's WINDOW_UPDATE releases more of the body -- the producer is pulled
  ## only once the queued bytes are on the wire, so buffered upload memory stays
  ## ~one chunk. This is the h2 analog of h1's `sendRequest`; both h2Stream and
  ## h2SendAndReadHeaders drive their read loop from the id it returns.
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
            await sendAll(transport, h2.finishSend(sid, h2TrailerList(req)))
            sending = false
          else:
            await sendAll(transport, h2.queueSend(sid, chunk))
        else:
          let inbound = await recvSome(transport)
          if inbound.len == 0: break
          let toSend = h2.feed(inbound)
          if toSend.len > 0: await sendAll(transport, toSend)
    else:
      await sendAll(transport,
        h2.encodeRequest(sid, h2HeaderList(req), req.body, h2TrailerList(req)))
    sid

template effectiveSink(sink, gate, ver, status, headers: typed): untyped =
  ## The sink to actually drain the body into at this drain site: the caller's
  ## `sink` when the gate is certain this response will be surfaced normally, else a
  ## nil sink of the same type (so the body buffers and the policy layer decides).
  ## A nil gate (no user sink) always yields nil. Evaluated after the headers are in.
  if gate != nil and gate.wantsDelivery(ver, status, headers): sink
  else: typeof(sink)(nil)

template h2Stream(transport, h2, req, sink, decompress, cap: typed): Response =
  ## One HTTP/2 request/response on a new stream of the shared connection `h2`.
  block:
    mixin BodySink
    let sid = h2SendRequest(transport, h2, req)
    # Deliver the body to the sink incrementally as DATA arrives (bounded memory),
    # or buffer it for a non-streaming request. The decoder is built once the
    # response headers are in (so content-encoding is known); the loop runs once
    # more after the END_STREAM feed, so the final chunk is delivered too. The sink
    # is `await`ed, so a slow sink stalls this read loop and, in turn, the peer
    # (backpressure). On the buffered path `takeBody` is never called, so the whole
    # body accumulates in the connection as before.
    var cd = initCappedDecoder(decompress, cap)
    while not h2.streamDone(sid):
      let chunk = await recvSome(transport)
      if chunk.len == 0: break
      let toSend = h2.feed(chunk)
      if toSend.len > 0: await sendAll(transport, toSend)
      if not sink.isNil:
        deliverChunk(cd, sink, h2.takeBody(sid), h2.respHeader(sid, "content-encoding"))
    let wasReset = h2.streamReset(sid)
    let tooLarge = h2.streamTooLarge(sid)
    let unprocessed = h2.streamUnprocessed(sid)
    let connErr = h2.connError
    let done = h2.streamDone(sid)  # END_STREAM seen (else the loop broke on transport EOF)
    let responseBegan = h2.responseBegan(sid)      # final OR 1xx headers; before takeResponse
    let lengthBad = h2.streamLengthMismatch(sid)   # capture before takeResponse drops it
    var r = toResponse(h2.takeResponse(sid))
    # The pre-header keep-alive-race classification now lives in raiseH2Terminal's shared
    # cascade: pass the RAW reset flag (NOT coerced with `r.status == 0`, which would
    # pre-empt the race branch since "no response began" implies `r.status == 0`) plus
    # `responseBegan` and `noResponse = r.status == 0` (gone away before a response),
    # which the cascade coerces to a plain reset AFTER the race branch. On the buffered
    # path the decoder-complete check only applies when streaming to a sink.
    raiseH2Terminal(H2Terminal(connErr: connErr, tooLarge: tooLarge,
      unprocessed: unprocessed, wasReset: wasReset, responseBegan: responseBegan,
      noResponse: r.status == 0, done: done,
      lengthBad: lengthBad, decoderComplete: sink.isNil or cd.streamComplete))
    if not sink.isNil: r.body = ""  # delivered incrementally above
    r

template h2GatedStream(transport, h2, req, userSink, gate,
                       decompress, cap: typed): Response =
  ## The gated h2 variant of `h2Stream` for the buffered `request()` sink path
  ## (sync + pooled-h2). Sends the request, reads the response headers, then decides
  ## via the gate whether this response will be surfaced: if so, drains the body to
  ## `userSink` incrementally; if not, buffers it for the policy layer. A gated stop
  ## (the sink returned false -> SinkStopSignal) RSTs the stream (the connection is
  ## kept), returns the snapshot with `bodyTruncated = true`, and skips the terminal
  ## error cascade. The snapshot is captured BEFORE any reset, since a reset drops
  ## the stream from the connection.
  mixin await, sendAll, recvSome, BodySink
  block:
    let sid = h2SendAndReadHeaders(transport, h2, req)
    let snap = toResponse(h2.respSnapshot(sid))
    let eff = effectiveSink(userSink, gate, snap.httpVersion, snap.status, snap.headers)
    var cd = initCappedDecoder(decompress, cap)
    var stopped = false
    template deliver() =
      if not eff.isNil:
        try:
          deliverChunk(cd, eff, h2.takeBody(sid), h2.respHeader(sid, "content-encoding"))
        except SinkStopSignal:
          stopped = true
    deliver()                            # body queued during the header read
    while not stopped and not h2.streamDone(sid):
      let chunk = await recvSome(transport)
      if chunk.len == 0: break
      let toSend = h2.feed(chunk)
      if toSend.len > 0: await sendAll(transport, toSend)
      deliver()
    if stopped:
      let rst = h2.resetStream(sid)      # stop the peer; the connection stays reusable
      if rst.len > 0: await sendAll(transport, rst)
      var r = snap
      r.body = ""
      r.bodyTruncated = true
      r
    else:
      let wasReset = h2.streamReset(sid)
      let tooLarge = h2.streamTooLarge(sid)
      let unprocessed = h2.streamUnprocessed(sid)
      let connErr = h2.connError
      let done = h2.streamDone(sid)
      let responseBegan = h2.responseBegan(sid)   # headers were in (this path read them)
      let lengthBad = h2.streamLengthMismatch(sid)
      var r = toResponse(h2.takeResponse(sid))
      # Headers were already read here (h2SendAndReadHeaders), so a response began: the
      # race branch never fires. Pass the RAW reset + `noResponse = r.status == 0` so the
      # cascade classifies an abrupt GOAWAY / truncation exactly as before.
      raiseH2Terminal(H2Terminal(connErr: connErr, tooLarge: tooLarge,
        unprocessed: unprocessed, wasReset: wasReset, responseBegan: responseBegan,
        noResponse: r.status == 0, done: done,
        lengthBad: lengthBad, decoderComplete: eff.isNil or cd.streamComplete))
      if not eff.isNil: r.body = ""      # delivered incrementally above
      r

template h2SendAndReadHeaders*(transport, h2, req: typed): uint32 =
  ## Open an h2 stream, send the request (including a streamed upload body), and
  ## read frames until the final response headers arrive; returns the stream id.
  ## The header/body split lets a pull-based caller return a handle here and drain
  ## the body later. Raises if the stream fails before any headers (so the caller's
  ## retry/redirect loop can react), mirroring `h2Stream`'s terminal errors.
  mixin await, sendAll, recvSome
  block:
    let sid = h2SendRequest(transport, h2, req)
    while not h2.headersReady(sid) and not h2.streamDone(sid):
      let chunk = await recvSome(transport)
      if chunk.len == 0: break
      let toSend = h2.feed(chunk)
      if toSend.len > 0: await sendAll(transport, toSend)
    if not h2.headersReady(sid):            # stream died before a response
      let connErr = h2.connError
      let unprocessed = h2.streamUnprocessed(sid)
      let wasReset = h2.streamReset(sid)     # capture before takeResponse drops the stream
      let responseBegan = h2.responseBegan(sid)  # 1xx or final headers (before takeResponse)
      let done = h2.streamDone(sid)          # abrupt GOAWAY / conn error is terminal
      discard h2.takeResponse(sid)
      # Delegate to the shared cascade: connErr -> IOError, unprocessed -> UnprocessedError,
      # a raw RST before headers (not REFUSED, which is `unprocessed`) -> plain IOError (the
      # peer may have processed it), a drop with no response begun (not even a 1xx interim)
      # -> KeepAliveRaceError (retryable idempotent / with an Idempotency-Key), a 1xx-then-
      # drop where a response DID begin -> plain IOError. `noResponse` = true (no final
      # status): with `done` (abrupt GOAWAY) it stays a plain IOError, after the race branch.
      raiseH2Terminal(H2Terminal(connErr: connErr, tooLarge: false,
        unprocessed: unprocessed, wasReset: wasReset, responseBegan: responseBegan,
        noResponse: true, done: done,
        lengthBad: false, decoderComplete: true))
      # Unreachable on a death (the cascade always raises), but keeps the `while` block
      # well-typed on the (impossible) fall-through.
      raise newException(KeepAliveRaceError, "navi: http/2 connection closed")
    sid

template h2DrainBody*(transport, h2, sid, sink, decompress, cap: typed) =
  ## Drain an h2 response body to `sink` incrementally (bounded memory), decoding
  ## if `decompress` and enforcing `cap`. A slow sink stalls the read loop and, in
  ## turn, the peer (backpressure). Raises on reset / oversized / unprocessed, like
  ## `h2Stream`. Drops the stream when done. `sink` must be non-nil (pull path).
  mixin await, sendAll, recvSome, BodySink
  block:
    var cd = initCappedDecoder(decompress, cap)
    template deliver() =
      deliverChunk(cd, sink, h2.takeBody(sid), h2.respHeader(sid, "content-encoding"))
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
    let responseBegan = h2.responseBegan(sid)      # headers were in (this drains the body)
    let lengthBad = h2.streamLengthMismatch(sid)   # capture before takeResponse
    discard h2.takeResponse(sid)       # body delivered; drop the stream
    # The body drain runs only after headers, so a response began: the race branch does
    # not fire. A mid-body EOF (not done) surfaces as the usual truncation.
    raiseH2Terminal(H2Terminal(connErr: connErr, tooLarge: tooLarge,
      unprocessed: unprocessed, wasReset: wasReset, responseBegan: responseBegan,
      noResponse: false, done: done,
      lengthBad: lengthBad, decoderComplete: cd.streamComplete))

template h1GatedFinish*(transport, parser, sink, gate, keep,
                        decompress, cap: typed): Response =
  ## Finish an h1 exchange whose headers are already parsed, routing the body either
  ## to the gated `sink` (when the gate wants this response delivered) or to the
  ## buffered path. Used at the three h1 drain sites when a user sink + gate are set.
  ## On a gated stop (the sink returned false -> SinkStopSignal) the body is left ""
  ## and `bodyTruncated` set, and the connection is not kept (keep = false), since a
  ## partially-read h1 response cannot be pooled.
  mixin await, recvSome, BodySink
  block:
    let snap = parser.toResponse()      # headers-only snapshot for the gate
    let eff = effectiveSink(sink, gate, snap.httpVersion, snap.status, snap.headers)
    var truncated = false
    if eff.isNil:
      setStreaming(parser, false)       # buffer: bytes that arrived with the headers
      var k = false                     # migrate from `pending` into `body`
      h1DrainBody(transport, parser, BodySink(nil), k, decompress, cap)
      keep = k
    else:
      var k = false
      try:
        h1DrainBody(transport, parser, eff, k, decompress, cap)
        keep = k
      except SinkStopSignal:
        truncated = true
        keep = false                    # a stopped h1 body left bytes on the wire
    var r = parser.toResponse()
    if not eff.isNil: r.body = ""        # delivered (or partly delivered) to the sink
    if truncated:
      r.trailers = initHeaders()         # trailers are absent on an early stop, even
                                         # when they had already been parsed off the wire
    r.bodyTruncated = truncated
    r

template serveOnce(client, pc, rq, sink, key: typed;
                   asyncStream: typed = nil; userSink: typed = nil;
                   gate: typed = nil): Response =
  ## Run one request/response over the pooled connection `pc` -- reused or freshly
  ## opened -- then return `pc` to the idle pool if it may be kept, else close it.
  ## `pc.h2` set means an established h2 connection; otherwise HTTP/1.1, unless the
  ## transport just negotiated "h2" on a fresh connection (`pc.h2` still nil), in
  ## which case the h2 connection is initialized and its client preamble sent first.
  ## A pre-response failure is classified by the exception TYPE (`h1SendAndReadHeaders`
  ## raises `KeepAliveRaceError` before any response; the h2 templates their own
  ## `UnprocessedError`/`KeepAliveRaceError`), so the caller's replay decision reads the
  ## error, not a flag. The caller must guard the call so a raised exchange closes the
  ## transport before re-raising; the pool/close below runs only on success, so a pooled
  ## connection is never double-closed.
  mixin sendAll, await, BodySink
  block:
    let gated = not userSink.isNil and not gate.isNil
    var r: Response
    var keep = false
    if pc.h2 != nil or pc.transport.protocol == "h2":
      if pc.h2 == nil:
        pc.h2 = initH2Conn(client.config.maxResponseBytes)
        await sendAll(pc.transport, pc.h2.preamble())
      if gated:
        r = h2GatedStream(pc.transport, pc.h2, rq, userSink, gate,
                          client.config.wantsDecompress, client.config.maxResponseBytes)
      else:
        r = h2Stream(pc.transport, pc.h2, rq, sink,
                     client.config.wantsDecompress, client.config.maxResponseBytes)
      keep = pc.h2.canReuse
    else:
      var parser = h1SendAndReadHeaders(pc.transport, rq,
                     not sink.isNil or gated, asyncStream)
      if gated:
        r = h1GatedFinish(pc.transport, parser, userSink, gate, keep,
                          client.config.wantsDecompress, client.config.maxResponseBytes)
      else:
        h1DrainBody(pc.transport, parser, sink, keep,
                    client.config.wantsDecompress, client.config.maxResponseBytes)
        r = parser.toResponse()
    if not (keep and pushIdle(client.pool, key, pc)):
      await close(pc.transport)
    r

template poolTransport*(client, req, sink: typed; asyncStream: typed = nil;
                        userSink: typed = nil; gate: typed = nil): Response =
  ## Pool-based transport: reuse a pooled connection (http/1.1 or a persistent
  ## h2 connection) or open a fresh one, negotiating the protocol via ALPN.
  ## One request at a time per connection. Used by the sync entry. `asyncStream`
  ## is accepted for signature parity with `transportInner` (an awaited upload
  ## producer) but is always nil here: the sync backend has no event loop to await
  ## a producer, so an async producer never reaches this path (it fails to compile
  ## at the sync `request` entry). `userSink`/`gate` (when set) stream the FINAL
  ## response body to the caller's gated sink; nil for the buffered/stream() paths.
  mixin connect, sendAll, recvSome, close, rearm, await, BodySink
  block:
    var rq = req
    let proxy = resolveProxy(client.config, rq.url)
    rq.absoluteForm = usesAbsoluteForm(proxy, rq.url.isTls)
    let alpn = if client.config.wantsH2 and rq.url.isTls:
                 @["h2", "http/1.1"] else: @[]
    let key = originKey(rq.url)
    var resp: Response
    var served = false

    for dead in reapExpired(client.pool):   # close idle connections past idleConnTimeout;
      await close(dead.transport)           # popIdle only defers expired entries, it does
                                            # not close them, so sweep here too (issue #313)
    var (found, pc) = popIdle(client.pool, key)
    if found:
      # navi's live-config contract: `client.config.timeouts.*` are read per request,
      # so a connection taken from the idle pool must adopt the CURRENT read timeout
      # and per-attempt total deadline, not the ones it was opened with (issue #360).
      rearm(pc.transport, client.config.readMs, totalMsFor(client.config, rq))
      try:
        resp = serveOnce(client, pc, rq, sink, key, asyncStream, userSink, gate)
        served = true
      except CatchableError as e:
        await close(pc.transport)  # pooled connection was stale
        # A half-delivered gated body must never be replayed onto a fresh connection
        # (it would double-feed the sink): once the sink has been fed, propagate.
        if gate != nil and gate.fed: raise
        # Fall through to a fresh connection only when replaying is safe (matching Go
        # net/http; RFC 9110 9.2.2). The error type carries the pre/post-response and
        # proven/ambiguous distinction: `replayableAfterError` replays an idempotent
        # method, a proven-unprocessed error (`UnprocessedError`), or an Idempotency-Key-
        # vouched keep-alive race (`KeepAliveRaceError`). A non-idempotent method without
        # a key, or a post-response truncation, is not replayed. A non-replayable streamed
        # body (`bodyStream`) is never retried (its producer cannot rewind).
        if not (isReplayable(req) and replayableAfterError(req, e)):
          raise

    if not served:
      let transport = await connect(rq.url.host, rq.url.port, rq.url.isTls,
                                    client.config.tls, proxy, alpn,
                                    client.config.connectMs, client.config.readMs,
                                    totalMsFor(client.config, rq))
      var npc = PooledConn[typeof(transport)](transport: transport)
      # Guard the exchange so a failure (h1/h2 send, the h2 preamble/stream, or a
      # truncated body) closes the just-opened transport before re-raising, instead
      # of leaking the fd/TLS handle. serveOnce's pool/close decision runs only on
      # success, so a pooled connection is never double-closed.
      try:
        resp = serveOnce(client, npc, rq, sink, key, asyncStream, userSink, gate)
      except CatchableError:
        await close(transport)
        raise
    resp

template run(client, req, sink: typed; asyncStream: typed = nil;
             userSink: typed = nil; gate: typed = nil): Response =
  ## Cookie handling around the backend's transport step. `transport` is
  ## resolved per entry: pool-based for sync, mux-based for the async backends.
  ## `asyncStream` (async only) is an awaited upload producer forwarded to the
  ## transport; nil on the sync path, where `transport` takes only (client, rq, sink).
  ## `userSink`/`gate` (when set) stream the final response body to the caller's
  ## gated sink; the `when compiles` ladder picks the widest form each backend's
  ## `transport` accepts, so the buffered `stream()`/batch call sites (no gate) and
  ## the sync path (no asyncStream) all compile unchanged.
  mixin transport, await
  block:
    var rq = req
    validateRequest(rq)                # reject header/host CR-LF injection
    applyCookies(client.jar, rq)
    when compiles(transport(client, rq, sink, asyncStream, userSink, gate)):
      var resp = await transport(client, rq, sink, asyncStream, userSink, gate)
    elif compiles(transport(client, rq, sink, asyncStream)):
      var resp = await transport(client, rq, sink, asyncStream)
    else:
      var resp = await transport(client, rq, sink)
    storeCookies(client.jar, rq.url, resp)
    resp

template maybeDigest(client, rreq, resp, digestOrigin: typed;
                     userSink: typed = nil; gate: typed = nil) =
  ## On a 401 Digest challenge, when digest auth is configured, the request
  ## carries no Authorization yet, and it is still on the origin the credentials
  ## were configured for, compute the response and retry once. The origin check
  ## keeps digest credentials from being answered to a cross-origin redirect
  ## target (mirroring the Authorization stripping in `redirectRequest`; without
  ## it, digest would bypass that protection since the strip clears the header the
  ## first condition tests). Expands inline so the retry's `await`s run in the
  ## caller's async proc. `userSink`/`gate` (when set) stream a digest-protected
  ## FINAL body: the one-shot replay clears `gate.digestReady` first (its response is
  ## the surfaced one, so its body may reach the sink) and forwards the user sink.
  mixin BodySink
  # A streamed body (`bodyStream`) is never retried: its producer was pulled to
  # EOF on the first attempt and cannot rewind, so a digest replay would send a
  # truncated (empty) body. Return the 401 to the caller instead (mirrors the
  # retry layer's guard and the 307/308 guard in followRedirects).
  if resp.status == 401 and client.config.auth.kind == akDigest and
     isReplayable(rreq) and
     not rreq.headers.contains("authorization") and
     originKey(rreq.url) == digestOrigin:
    let chal = bestChallenge(resp.headers.getAll("www-authenticate"))
    if chal.isSome:
      let auth = digestAuthHeader(
        client.config.auth.user, client.config.auth.pass,
        $rreq.verb, rreq.url.requestTarget, chal.get)
      if auth.len > 0:                 # "" means the challenge algorithm is unsupported
        rreq.headers["authorization"] = auth
        if gate != nil: gate.digestReady = false  # the replay's response is surfaced
        resp = run(client, rreq, BodySink(nil), nil, userSink, gate)

template followRedirects*(client, startReq, resp: typed; asyncStream: typed = nil;
                         userSink: typed = nil; gate: typed = nil) =
  ## Issue `startReq`, following redirects into `resp`. Expands inline so its
  ## `await`s run in the caller's async proc. `asyncStream` (async only) is the
  ## awaited upload producer for the initial send; a streamed request is
  ## non-replayable, so it breaks before any redirect rewrite that would carry its
  ## body forward (below) and the producer is never pulled a second time. A rewrite
  ## that DROPS the body takes the producer with it (`bodyDropped`), so the next hop
  ## goes out bodiless rather than as a chunked request with an empty producer (#395).
  ## `userSink`/`gate` (when set) stream the FINAL body: this loop refreshes the
  ## gate's per-hop fields (hops, limit, whether this hop is replayable, its verb,
  ## and whether digest is still armed) before each `run`, so the drain site can tell
  ## whether the response is the surfaced one.
  mixin BodySink
  var rreq = startReq
  let digestOrigin = originKey(startReq.url)   # digest creds only for this origin
  var hops = 0
  let limit = client.config.redirectLimit
  # Set once a rewrite drops the body (303, or 301/302 off a non-GET/HEAD method).
  # `redirectRequest` clears the Request's own body/`bodyStream`/trailers, but the
  # async producer lives outside the Request, so it has to be dropped here too: a
  # bodiless GET must not go out as `Transfer-Encoding: chunked` with a producer that
  # is immediately at EOF (a lone `0\r\n\r\n` body), which several intermediaries log
  # or reject. Sticky: once dropped, no later hop has a body to send either (#395).
  var bodyDropped = false
  while true:
    if gate != nil:
      gate.hops = hops
      gate.redirectLimit = limit
      gate.hopReplayable = isReplayable(rreq)
      gate.hopVerb = rreq.verb          # with hopReplayable: does this hop break?
      # Digest is only in play on the original origin, before an Authorization is set
      # (mirrors maybeDigest's own guard); off any other hop the 401 is surfaced.
      gate.digestReady = client.config.auth.kind == akDigest and
        not rreq.headers.contains("authorization") and
        originKey(rreq.url) == digestOrigin
    if bodyDropped:      # a rewrite dropped the body: send no body, chunked or not
      resp = run(client, rreq, BodySink(nil), nil, userSink, gate)
    else:
      resp = run(client, rreq, BodySink(nil), asyncStream, userSink, gate)
    maybeDigest(client, rreq, resp, digestOrigin, userSink, gate)
    decodeBody(resp, client.config)
    let location = resp.headers.get("location")
    if shouldFollowRedirect(resp.status, hops, limit, location):
      # A hop that carries the body forward (307/308 always; 301/302 when the method
      # is already GET/HEAD, which is NOT rewritten -- redirect.nim) cannot be followed
      # with a streamed body: `bodyStream` (or an async producer, flagged by
      # `hasStreamedBody`) can't be rewound after the first attempt pulled it, so the
      # next hop would upload a truncated body. Return the redirect response to the
      # caller instead (#295). A hop that DROPS the body (303, and 301/302 off a
      # non-GET/HEAD method, both rewritten to a bodyless GET) carries no stream to
      # replay and is followed as usual.
      if not isReplayable(rreq) and preservesBody(resp.status, rreq.verb):
        break
      if not preservesBody(resp.status, rreq.verb):
        bodyDropped = true            # the producer goes with the body (#395)
      rreq = redirectRequest(rreq, resp.status, location)
      inc hops
    else:
      break

template performRequest*(client, req0: typed; cancel: CancelToken = nil;
                         asyncStream: typed = nil; userSink: typed = nil;
                         gate: typed = nil): Response =
  ## Buffered request with the full policy layer: retries with backoff, redirect
  ## following, decompression, size cap, and throw-on-non-2xx. Middleware (which
  ## can wrap, short-circuit, or observe) is composed around this by the entry
  ## module. `cancel` is checked between attempts (cooperative on the sync
  ## backend; the async backends also abort in-flight via their guard).
  ##
  ## `asyncStream` (async backends only) is an awaited pull-based upload producer,
  ## threaded outside the `Request` because its Future type is backend-specific.
  ## The request that carries one flags `hasStreamedBody`, so `isReplayable` treats
  ## it as non-replayable: it is sent once and never retried/redirected/digest-replayed,
  ## exactly like a sync `bodyStream`.
  ##
  ## `userSink`/`gate` (when set) stream the FINAL response body to the caller's gated
  ## sink. The gate is filled once here with the retry/surfacing mirror (and its
  ## `attempt` updated per iteration); the streaming drain site is a best-effort
  ## optimization. The delivery RULE is enforced in ONE place: the fallback below.
  ## After the policy layer has settled on the surfaced response, if the sink was set,
  ## the body was not already streamed to it (bodyTruncated) and a buffered body
  ## remains, the whole (already decoded) body is delivered in one sink call and the
  ## body cleared. So a gate miss (retry-budget-exhausted final, unsupported digest
  ## algorithm, the h3 leg) still delivers the final body exactly once.
  mixin sleep, BodySink, guardedAttempt
  block:
    var req = req0
    var resp: Response
    var attempt = 0
    let policy = client.config.retry
    if gate != nil:
      gate.retryReplayable = isReplayable(req)
      gate.retryVerb = req.verb
      gate.policy = policy
      gate.wantsThrow = client.config.wantsThrow
      gate.http = client.config.http
    # `config.timeouts.total` covers the whole request including retries/redirects
    # and their backoff sleeps. The async backends enforce that with an outer `guard`
    # that aborts in-flight IO; the sync/batch paths have no such guard, so they carry
    # this cooperative deadline: it caps each backoff sleep and stops the retry loop
    # once the budget is spent, and threads the REMAINING budget into each attempt's
    # connect deadline via `req.deadlineMs` (issue #359). Armed once, here, so it spans
    # every attempt. On the async backends `totalMsFor` (via `deadlineMs`) is ignored
    # at connect and `backoffWithinDeadline` only trims a sleep the guard would preempt
    # anyway, so behavior there is unchanged.
    var deadline = initRetryDeadline(client.config.totalMs)
    # A streamed request body (`bodyStream`) can't be rewound once its producer has
    # advanced, so replaying it would send a truncated body. Such a request is never
    # retried -- not even a provably-unprocessed one, since the producer may already
    # have been pulled during the attempt.
    let bodyReplayable = isReplayable(req)
    while true:
      throwIfCancelled(cancel)
      # This attempt's budget: the remaining whole-request time, further capped by
      # `config.timeouts.attempt` when set (issue #375). On the sync/batch paths
      # `deadlineMs` bounds connect + reads directly; on the async backends it is
      # ignored at connect and the per-attempt slice is enforced by `guardedAttempt`
      # (below), which wraps this attempt in an inner `guard`. Either way, an attempt
      # timeout surfaces as a retryable error, while `total` exhaustion still stops
      # the loop via `backoffWithinDeadline`.
      let attemptMs = effectiveAttemptMs(deadline.attemptBudgetMs, client.config.attemptMs)
      req.deadlineMs = attemptMs
      if gate != nil: gate.attempt = attempt
      var gotResp = false
      try:
        guardedAttempt(client, req, resp, attemptMs, cancel, asyncStream, userSink, gate)
        gotResp = true
      except CatchableError as e:
        # A half-delivered gated body must never be re-issued (it would double-feed
        # the sink), so once the sink has been fed, propagate rather than retry.
        if gate != nil and gate.fed: raise
        # On the gated-sink path a cap breach is raised mid-drain (inside the retry
        # loop) rather than after it as on the buffered path; retrying it is pointless
        # (the response is deterministic) and would double-hit the sink budget, so
        # surface it straight away. Mirrors the buffered path, where enforceMaxResponse
        # raises after the loop and is never retried.
        if gate != nil and (e of ResponseTooLargeError): raise
        # An idempotent verb retries via the policy (`isRetryableVerb`); a non-idempotent
        # one only when `replayableAnyMethod` holds -- a proven-unprocessed error, or an
        # Idempotency-Key-vouched keep-alive race (see retry.nim). This mirrors Go
        # net/http's post-write policy and RFC 9110 9.2.2.
        if not shouldRetryAfterError(attempt, bodyReplayable,
                                     replayableAnyMethod(req, e), req.verb, policy):
          raise # not retryable: propagate the transport error
      if gotResp and
         not shouldRetryAfterResponse(attempt, resp.status, bodyReplayable,
                                      req.verb, policy):
        break
      inc attempt
      let backoff = backoffWithinDeadline(deadline, backoffMs(attempt, resp, policy))
      # Budget exhausted (or a backoff sleep would overrun it): stop retrying and
      # surface what we have -- the last response if one arrived, else the timeout
      # matching the async guard's expiry (same TimeoutError/message).
      if backoff < 0:
        if gotResp: break
        raise newException(TimeoutError,
          "navi: request timed out after " & $client.config.totalMs & " ms")
      await sleep(backoff)
    enforceMaxResponse(resp, client.config.maxResponseBytes)
    enforceProtocol(client.config, resp.httpVersion)  # strict: used proto in config.http
    if client.config.wantsThrow and not resp.ok:
      raiseHttpError(req, resp)      # HttpError carries the buffered body
    # Delivery-rule fallback: the surfaced response is settled. If a gated sink was
    # set, the body was not already streamed to it (bodyTruncated) and a buffered body
    # remains, deliver the whole (already decodeBody-decoded) body in ONE sink call,
    # then clear it. This is the single place the delivery rule is enforced; the
    # streaming drain sites are just an optimization the gate may skip. A `false`
    # returned on this single call does NOT set bodyTruncated (the body WAS fully
    # transferred), so the wrapped sink's SinkStopSignal is caught and discarded here.
    when compiles(await userSink("")):
      if not userSink.isNil and resp.body.len > 0 and not resp.bodyTruncated:
        try:
          # The wrapped sink is a bare closure (portable to js), so it carries no
          # chronos raises annotation; navi's contract is it raises at most
          # CatchableError -- discharge chronos's strict gcsafe/raises here, as the
          # deliverChunk path does.
          {.cast(gcsafe).}:
            {.cast(raises: [CatchableError]).}:
              await userSink(resp.body)
        except SinkStopSignal:
          discard
        resp.body = ""
    resp
