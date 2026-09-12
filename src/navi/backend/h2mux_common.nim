## Shared HTTP/2 multiplexer body, `include`d by both `h2mux.nim` (asyncdispatch)
## and `h2mux_chronos.nim` (chronos). Everything here is identical across the two
## backends: the includer imports its own async runtime and its backend `Conn`
## (aliased `be`) first, so `Future`, `newFuture`, `await`, `{.async.}` and the
## transport ops resolve per backend. The genuinely backend-specific pieces -- the
## reader's teardown, keepAlive's timer, `newH2Mux`, `close`, and the fire-and-forget
## `fireSend` (forward-declared below) -- live in each includer AFTER this fragment.
##
## Not a standalone module: it references symbols the includer imports, so do not
## `nim check` it directly.
##
## One transport carries many concurrent streams. A single background reader owns
## the transport, feeds received bytes into the sans-io `H2Conn`, sends control
## frames back, and completes each request's per-stream Future as its response
## finishes. Streaming responses (a `sink`) are owned by their own request coroutine
## (`drainDownload`), which acks the receive window per chunk so a slow sink stalls
## only that one stream (backpressure) without blocking the reader or other streams.

type
  MuxState = enum
    ## Transport teardown lifecycle (chronos only; asyncdispatch stays `msActive`
    ## throughout, its reader always owning the transport close unconditionally).
    ## A linear progression -- there is no path back to an earlier state:
    ##   msActive          -- connection live, no teardown initiated.
    ##   msClosing         -- chronos `close` has taken over the teardown, so the
    ##                        reader's self-exit path must defer to it (it EOFs the
    ##                        transport via `closeWait` rather than have the reader
    ##                        close it out from under a parked read).
    ##   msTransportClosed -- `be.close(transport)` has been performed. Whichever
    ##                        path (reader self-exit or `close`) reaches this state
    ##                        first owns the single `be.close`; the other sees the
    ##                        state and skips, so the transport's unshared SSL_CTX is
    ##                        never freed twice (issue #314).
    ## Transitions: msActive -> msClosing (chronos `close`); msActive -> msTransportClosed
    ## (reader self-exit wins the close); msClosing -> msTransportClosed (`close` performs
    ## the close). The reader's self-exit teardown runs only from `msActive` -- once
    ## `close` has moved to `msClosing`/`msTransportClosed` the reader defers.
    msActive
    msClosing
    msTransportClosed
  H2Mux* = ref object
    transport: be.Conn
    h2: H2Conn
    waiters: Table[uint32, Future[H2Response]]
    pendingSlots: Deque[Future[void]]  ## requests waiting for a concurrency slot
    sendReady: Table[uint32, seq[Future[void]]]  ## senders parked until the send
                                            ## window drains their queued chunk. A LIST
                                            ## per stream: a tunnel can have two in flight
                                            ## (a user send + the WS keepalive ping), and
                                            ## a single slot would strand one (issue #263)
    sinkStreams: HashSet[uint32]       ## streams owned by a drainDownload coroutine
                                       ## (their body goes to a sink, not `waiters`)
    recvq: Table[uint32, Deque[string]]  ## raw body chunks the reader drained per feed,
                                         ## awaiting the drain loop (one entry per feed
                                         ## keeps delivery incremental; bounded by the
                                         ## receive window, whose ack is gated by the sink)
    recvReady: Table[uint32, Future[void]]  ## a sink stream's drain loop waiting for
                                            ## the reader to feed more DATA
    decoders: Table[uint32, CappedDecoder]  ## per-sink-stream decode + size-cap state,
                                            ## created lazily once headers are in
    decompress: bool                   ## decode content-encoding before the sink
    cap: int                           ## max decoded response bytes (maxResponseBytes)
    sendTail: Future[void]   ## tail of the serialized send chain
    alive: bool
    readerDone: Future[void] ## completed once the reader has exited and the
                             ## transport is closed, so `close` can join it
    settingsSeen: Future[void]  ## completed once the peer's initial SETTINGS is seen
                                ## (or the reader exits), so an Extended CONNECT can gate
                                ## on ENABLE_CONNECT_PROTOCOL before sending (RFC 8441)
    readerFut: Future[void]  ## the reader task itself, held so chronos `close` can join
                             ## it (reaping its parked read cleanly). Unused on the
                             ## asyncdispatch backend, whose reader is `asyncCheck`ed.
    state: MuxState          ## transport teardown lifecycle (see `MuxState`). `msClosing`
                             ## tells the reader's self-exit teardown to defer to `close`;
                             ## `msTransportClosed` gates the single `be.close` so it never
                             ## runs twice (a single-bool `closing` flag can't prevent that
                             ## -- the reader may already be parked mid-teardown when `close`
                             ## arrives, and a double `be.close` frees the transport's
                             ## unshared SSL_CTX twice, issue #314). Chronos only; the
                             ## asyncdispatch reader always owns the close, staying msActive.
    keepAliveMs: int            ## PING keepalive interval (0 = off); see `keepAlive`
    sawFrameSinceTick: bool     ## the reader saw an inbound frame since the last
                                ## keepalive tick (any frame proves liveness)
    pingOutstanding: bool       ## a keepalive PING is awaiting any inbound frame

proc fireSend(mux: H2Mux, data: string) {.gcsafe, raises: [].}
  ## Fire-and-forget a control-frame send (RST_STREAM from the destructor path).
  ## Defined per backend after the include: `asyncCheck` on asyncdispatch,
  ## `asyncSpawn mux.trySend` on chronos.

proc reapStream(mux: H2Mux, sid: uint32) {.gcsafe, raises: [].}
  ## Forward-declared (defined after the teardown helpers it uses). Tears a half-open
  ## stream off a still-healthy connection -- RST it, drop bookkeeping, release the
  ## slot -- when a request's send/produce phase raises (issue #261) or dispatch finds
  ## its waiter cancelled (issue #262), so a failure never strands a concurrency slot.

const goAwayGraceMs = 30_000
  ## After a GOAWAY the peer promises (via last-stream-id) to finish the covered
  ## streams, so the reader keeps waiting for their responses. Bound that wait with
  ## a generous idle grace (reset on each datagram), so a peer that sends GOAWAY and
  ## then neither delivers nor closes cannot hang in-flight requests forever.

const h2KeepAlivePayload = "navi-kpa"   # 8 opaque PING bytes; any inbound frame answers

proc activeStreams(mux: H2Mux): int =
  ## Streams counting against the peer's MAX_CONCURRENT_STREAMS: buffered
  ## (`waiters`) plus streaming (`sinkStreams`).
  mux.waiters.len + mux.sinkStreams.len

proc releaseSlot(mux: H2Mux) =
  ## Wake one request waiting on MAX_CONCURRENT_STREAMS (a stream just freed up).
  while mux.pendingSlots.len > 0:
    let s = mux.pendingSlots.popFirst()
    if not s.finished:
      s.complete()
      break

proc resetError(mux: H2Mux, sid: uint32): ref CatchableError {.gcsafe, raises: [].} =
  ## The exception a RST_STREAM maps to, classified from the recorded stream flags:
  ## oversize -> ResponseTooLargeError, provably-unprocessed -> UnprocessedError
  ## (retryable), else a generic reset. Read the flags before `takeResponse` clears
  ## them. Shared by every reset path (reader dispatch, header-wait, body read).
  if mux.h2.streamTooLarge(sid):
    newException(ResponseTooLargeError, "navi: response exceeded maxResponseBytes")
  elif mux.h2.streamUnprocessed(sid):
    newException(UnprocessedError, "navi: http/2 request not processed")
  else:
    newException(IOError, "navi: http/2 stream reset")

proc detachSink(mux: H2Mux, sid: uint32) =
  ## Drop a sink stream that failed while its response headers were still awaited
  ## (connection closed, stream reset, or gone-away-unprocessed): remove its
  ## bookkeeping and drop the stream via `takeResponse` -- no RST, since it is
  ## already dead or the peer has gone away -- and release its concurrency slot.
  mux.sinkStreams.excl sid
  mux.recvq.del(sid)
  discard mux.h2.takeResponse(sid)
  mux.releaseSlot()

proc dispatch(mux: H2Mux) =
  ## Resolve any buffered streams that finished after the latest feed. Streaming
  ## (`sink`) streams are not in `waiters`; their own drain coroutine handles them.
  var done: seq[uint32]
  for sid in mux.waiters.keys: done.add sid
  for sid in done:
    let fut = mux.waiters[sid]
    if fut.finished:
      # On chronos a guard timeout / CancelToken can cancel the waiter while
      # `request` is parked at `await fut`, marking it finished(cancelled). Skipping
      # it would leak its stream + slot on every pass; after maxConcurrentStreams
      # cancellations every new request to the origin parks forever. Reap it: RST the
      # stream and release the slot (issue #262). asyncdispatch never cancels, so a
      # finished waiter here is always a cancellation.
      mux.reapStream(sid)
      continue
    if mux.h2.streamReset(sid):
      let err = mux.resetError(sid)          # classify before takeResponse clears flags
      discard mux.h2.takeResponse(sid)
      mux.waiters.del(sid)
      mux.releaseSlot()
      fut.fail(err)
    elif mux.h2.streamEnded(sid):
      let lengthBad = mux.h2.streamLengthMismatch(sid)   # before takeResponse drops it
      let resp = mux.h2.takeResponse(sid)
      mux.waiters.del(sid)
      mux.releaseSlot()
      if lengthBad: fut.fail(newException(IOError, bodyLengthErr))  # body != Content-Length
      else: fut.complete(resp)
    elif mux.h2.goneAway and mux.h2.streamUnprocessed(sid):
      # Above GOAWAY's last-stream-id: the peer will not process it, so fail it as
      # retryable. A stream at or below last-stream-id stays in `waiters` to finish
      # (RFC 9113 6.8: the peer may still deliver it); the reader keeps running until
      # it ends, or `failAll` fails it on the real connection close.
      mux.waiters.del(sid)
      mux.releaseSlot()
      fut.fail(newException(UnprocessedError, "navi: http/2 request not processed"))

proc wakeSenders(mux: H2Mux) =
  ## Wake streaming uploads / tunnel sends whose queued chunk has drained onto the
  ## wire (the reader released a window-blocked tail on WINDOW_UPDATE), or whose
  ## stream is finished/gone, so they pull the next chunk or stop. Each stream's
  ## waiters are a list -- a tunnel can have two parked at once (issue #263) -- and
  ## all are woken; each re-checks its own condition on resume.
  var wake: seq[uint32]
  for sid, futs in mux.sendReady:
    if not mux.alive or mux.h2.goneAway or mux.h2.sendDrained(sid) or
        mux.h2.streamEnded(sid):
      wake.add sid
  for sid in wake:
    let futs = mux.sendReady[sid]
    mux.sendReady.del(sid)
    for r in futs:
      if not r.finished: r.complete()

proc clearSendReady(mux: H2Mux, sid: uint32) =
  ## Complete and drop every sender parked on `sid` (they re-check state and exit),
  ## for the stream-teardown paths. Never strand a parked send future.
  if not mux.sendReady.hasKey(sid): return
  let futs = mux.sendReady[sid]
  mux.sendReady.del(sid)
  for r in futs:
    if not r.finished: r.complete()

proc wakeRecver(mux: H2Mux, sid: uint32) =
  ## Complete and drop `sid`'s parked reader (a `readChunk`), so a teardown racing an
  ## in-flight read does not strand it: `wakeRecvers` only iterates `sinkStreams`, so
  ## once the stream is pulled from that set the parked future is otherwise unreachable
  ## by every wakeup (issue #267). The woken `readChunk` re-checks state and exits.
  let r = mux.recvReady.getOrDefault(sid, nil)
  if r != nil and not r.finished: r.complete()
  mux.recvReady.del(sid)

proc waitSendable(mux: H2Mux, sid: uint32) {.async.} =
  ## Park until `sid`'s queued send drains onto the wire (or the stream/connection
  ## is gone). Multiple senders may park on one stream, so waiters are a per-stream
  ## list (issue #263). There is no yield between the state check and registering, so
  ## the reader (which runs only while we await) cannot slip a wake in: no lost wake.
  if mux.h2.sendDrained(sid) or mux.h2.streamDone(sid) or not mux.alive: return
  let ready = newFuture[void]("h2mux.sendready")
  if not mux.sendReady.hasKey(sid): mux.sendReady[sid] = @[]
  mux.sendReady[sid].add ready
  await ready

proc reapStream(mux: H2Mux, sid: uint32) {.gcsafe, raises: [].} =
  ## RST the live stream (so the peer frees its side), drop all per-stream
  ## bookkeeping, wake anything parked on it, and release its concurrency slot.
  ## Handles both a buffered waiter and a sink stream (the absent-key ops are no-ops),
  ## so it serves the send-phase failure (#261) and the cancelled-waiter reap (#262).
  ## Best-effort teardown: fully non-raising so it can run from the reader's dispatch
  ## (chronos, strict raises) and the async request paths alike -- completing a future
  ## can raise on asyncdispatch, and there is nothing useful to do with that here.
  try:
    mux.waiters.del(sid)
    mux.sinkStreams.excl sid
    mux.recvq.del(sid)
    mux.clearSendReady(sid)
    mux.wakeRecver(sid)                              # wake a parked reader; it re-checks
    mux.decoders.del(sid)
    if mux.alive:
      let rst = mux.h2.resetStream(sid)
      if rst.len > 0: mux.fireSend(rst)
    else:
      discard mux.h2.takeResponse(sid)
    mux.releaseSlot()
  except Exception:
    discard

proc queueBodies(mux: H2Mux) =
  ## Move each sink stream's newly-arrived body out of the connection into its
  ## `recvq` as one discrete chunk per feed. Draining the connection per feed keeps
  ## its buffer small and keeps delivery incremental: the drain pops chunks one at a
  ## time, so a fast reader can't collapse the whole body into a single sink call.
  ## Non-blocking (no sink here); the receive window still gates memory via ackRecv.
  for sid in mux.sinkStreams:
    let raw = mux.h2.takeBody(sid)
    if raw.len > 0:
      if not mux.recvq.hasKey(sid): mux.recvq[sid] = initDeque[string]()
      mux.recvq[sid].addLast(raw)

proc wakeRecvers(mux: H2Mux) =
  ## Wake every sink stream's drain loop so it re-checks its queue and the
  ## connection: new DATA may be queued, the stream may have ended/reset, or the
  ## connection may be gone. Spurious wakes are fine -- the drain loop re-checks.
  for sid in mux.sinkStreams:
    let r = mux.recvReady.getOrDefault(sid, nil)
    if r != nil and not r.finished: r.complete()

proc failAll(mux: H2Mux, msg: string) =
  mux.alive = false
  for sid, fut in mux.waiters:
    if not fut.finished:
      fut.fail(newException(IOError, msg))
  mux.waiters.clear()
  while mux.pendingSlots.len > 0:                 # wake blocked requests; they see
    let s = mux.pendingSlots.popFirst()           # `not alive` and raise
    if not s.finished: s.complete()
  mux.wakeSenders()                               # unblock in-flight streaming uploads
  mux.wakeRecvers()                               # unblock in-flight sink drains

proc send(mux: H2Mux, data: string) {.async.} =
  ## Serialize writes (chained on the previous send) so concurrent streams don't
  ## interleave frame bytes on the wire.
  if data.len == 0: return
  let prev = mux.sendTail
  let mine = newFuture[void]("h2mux.send")
  mux.sendTail = mine
  # `await prev` must be INSIDE the try: on chronos a cancellation (guard timeout,
  # CancelToken, SSE withTimeout) can land while we are parked here, and `mine` is
  # already installed as `sendTail`. If it were not completed on that path, every
  # later send on the connection would chain behind it forever (issue #258).
  try:
    if prev != nil and not prev.finished:
      await prev
    await be.sendAll(mux.transport, data)
  finally:
    mine.complete()

proc canReuse*(mux: H2Mux): bool = mux.alive and mux.h2.canReuse

proc streamBody(mux: H2Mux, sid: uint32, bodyStream: BodyProducer,
                trailers: seq[(string, string)] = @[]) {.async.} =
  ## Send DATA frames pulled from `bodyStream`, pulling the next chunk only once the
  ## previous one has drained onto the wire (the reader releases window-blocked bytes
  ## on WINDOW_UPDATE and wakes us), so buffered upload memory stays ~one chunk.
  ## END_STREAM rides the final frame via `finishSend` (a trailing HEADERS block when
  ## `trailers` is set).
  while mux.alive and not mux.h2.streamDone(sid):
    if mux.h2.sendDrained(sid):
      # single-threaded client; the producer need not be gcsafe (see engine).
      var chunk: string
      {.cast(gcsafe).}: chunk = bodyStream()
      if chunk.len == 0:
        await mux.send(mux.h2.finishSend(sid, trailers))
        break
      await mux.send(mux.h2.queueSend(sid, chunk))
    else:
      await mux.waitSendable(sid)

proc endStream(mux: H2Mux, sid: uint32) =
  ## Free a sink stream's per-stream state once it is done (or errored), and release
  ## its concurrency slot so a request parked on MAX_CONCURRENT_STREAMS can proceed.
  ## Idempotent: called at the end of `readChunk` and again if a sink error unwinds
  ## through the drain loop, so the slot is released only on the call that actually
  ## removes the stream (guarded by `wasActive`) -- never twice.
  let wasActive = sid in mux.sinkStreams
  mux.recvReady.del(sid)
  mux.recvq.del(sid)
  mux.sinkStreams.excl sid
  mux.decoders.del(sid)
  if wasActive and mux.alive and not mux.h2.streamEnded(sid) and
     not mux.h2.streamReset(sid):
    # Error unwind (the sink raised, or the content decoder failed on corrupt input)
    # before the stream finished: the server still thinks the stream is live and can
    # send up to a full stream window of discarded DATA, then stalls forever, leaking
    # a server-side zombie stream per abort on a pooled connection (issue #260). RST
    # it (also drops the stream locally) instead of a silent takeResponse.
    let rst = mux.h2.resetStream(sid)
    if rst.len > 0: mux.fireSend(rst)
  else:
    discard mux.h2.takeResponse(sid)
  if wasActive: mux.releaseSlot()

proc readChunk*(mux: H2Mux, sid: uint32): Future[string] {.async.} =
  ## Pull one decoded body chunk of the sink stream `sid`, or "" once the stream
  ## ends (dropping the stream). Raises on reset / oversized / unprocessed / gone.
  ## The per-stream decoder lives in `mux.decoders`. The gated receive window is
  ## acked per chunk, so a slow puller backpressures the peer. Runs concurrently
  ## with the reader, which wakes `recvReady[sid]` when new DATA lands or the stream
  ## finishes.
  try:
    while true:
      if not mux.alive:
        raise newException(IOError, "navi: http/2 connection closed")
      if sid notin mux.sinkStreams:
        # A concurrent abandon/dropStream/close pulled this stream out of sinkStreams
        # while we were parked (it wakes us via wakeRecver). Detect the removal and
        # exit instead of re-parking on a future no wakeup can reach (issue #267).
        raise newException(IOError, "navi: http/2 stream closed")
      if mux.h2.streamReset(sid):
        raise mux.resetError(sid)     # the except below drops the stream
      if mux.recvq.hasKey(sid) and mux.recvq[sid].len > 0:
        var raw = mux.recvq[sid].popFirst()
        let rawLen = raw.len   # window is acked by raw (wire) bytes, captured before the move
        if not mux.decoders.hasKey(sid):
          mux.decoders[sid] = initCappedDecoder(mux.decompress, mux.cap)
        var decoded: string
        # CappedDecoder enforces the decoded-size cap (and truncation) here, matching
        # the sync single-connection h2 path -- a bare StreamDecoder let a compression
        # bomb bypass maxResponseBytes on this (default async/chronos) path.
        mux.decoders.withValue(sid, cd):
          decoded = cd[].feed(raw,
            if cd[].encodingResolved: "" else: mux.h2.respHeader(sid, "content-encoding"))
        await mux.send(mux.h2.ackRecv(sid, rawLen))  # replenish window: gated by the puller
        if decoded.len > 0: return decoded
        continue                                     # decoder buffered input; pull more
      if mux.h2.streamEnded(sid):                    # ended and the queue is drained
        if mux.h2.streamLengthMismatch(sid):         # body != declared Content-Length
          raise newException(IOError, bodyLengthErr) # the except below drops the stream
        if mux.decoders.hasKey(sid) and not mux.decoders[sid].streamComplete:
          raise newException(IOError, truncatedBodyErr)  # compressed stream cut short
        mux.endStream(sid)
        return ""
      if mux.h2.goneAway and mux.h2.streamUnprocessed(sid):
        # Above last-stream-id: not processed, retryable. At or below it, fall through
        # and keep pulling -- the peer may still deliver more body / END_STREAM, and a
        # real close raises "connection closed" via the `not mux.alive` check above.
        raise newException(UnprocessedError, "navi: http/2 request not processed")
      # Nothing pending and not finished: wait for the reader to feed more. There is
      # no yield between the checks above and registering here, so the reader (which
      # runs only while we await) cannot slip a wake in between -- no lost wakeup.
      let ready = newFuture[void]("h2mux.recv")
      mux.recvReady[sid] = ready
      await ready
      mux.recvReady.del(sid)
  except CatchableError:
    mux.endStream(sid)
    raise

proc drainDownload*(mux: H2Mux, sid: uint32, sink: BodySink): Future[void] {.async.} =
  ## Own a streaming (`sink`) stream: pull chunks and `await` them into the sink (so
  ## the peer is paced by the sink -- backpressure) until the stream ends. Terminal
  ## cleanup and errors are `readChunk`'s; a sink error also drops the stream.
  try:
    while true:
      let c = await mux.readChunk(sid)
      if c.len == 0: break
      # single-threaded client; the sink need not be gcsafe (see engine). `c` is a
      # native `string`, moved into the sink's async env with no copy. The sink is a
      # bare closure (portable to js), so it carries no chronos raises annotation;
      # navi's contract is that it raises at most CatchableError -- assert that here.
      {.cast(gcsafe).}:
        {.cast(raises: [CatchableError]).}:
          await sink(c)
  except CatchableError:
    mux.endStream(sid)                # sink raised: readChunk returned, so clean up
    raise                            # (endStream releases the slot)

proc respSnapshot*(mux: H2Mux, sid: uint32): H2Response =
  ## Status + headers of a stream whose headers are in, without dropping it (the
  ## body is still to be drained). For the pull-based streaming handle.
  mux.h2.respSnapshot(sid)

proc sendAndReadHeaders*(mux: H2Mux, headers: seq[(string, string)], body: string,
                         bodyStream: BodyProducer = nil,
                         trailers: seq[(string, string)] = @[],
                         connectTunnel = false): Future[uint32] {.async.} =
  ## Open a sink stream, send the request, and await only until the response HEADERS
  ## arrive; return the stream id with the stream left open and its body queuing into
  ## `recvq` for a later `drainDownload`. The header/body split lets a pull-based
  ## caller inspect status/headers before deciding to drain. The stream is in
  ## `sinkStreams` (gated receive window), so buffered body is bounded until the
  ## drain acks it. Raises on reset/goaway before headers, like `request`.
  ##
  ## `connectTunnel` (RFC 8441 Extended CONNECT) sends the header block WITHOUT
  ## END_STREAM and streams no body, so the send side stays open for full-duplex
  ## tunnel DATA (see `tunnelSend`). Used for WebSocket-over-h2.
  if not mux.alive:
    raise newException(IOError, "navi: http/2 connection not usable")
  while mux.alive and mux.activeStreams >= mux.h2.maxConcurrentStreams:
    let slot = newFuture[void]("h2mux.slot")
    mux.pendingSlots.addLast(slot)
    await slot
  if not mux.alive:
    raise newException(IOError, "navi: http/2 connection not usable")
  if mux.h2.goneAway:       # a GOAWAY landed while we waited: opening a new stream now
    raise newException(UnprocessedError,   # would break RFC 9113 6.8 (peer PROTOCOL_ERRORs
      "navi: http/2 request not processed") # and drops the conn). Retry on a fresh conn.
  if mux.h2.nextStreamExhausted:   # 2^31 stream ids used (RFC 9113 5.1.1): opening one
    raise newException(UnprocessedError,   # more would alias an old id. Retry on a fresh
      "navi: http/2 request not processed") # connection; canReuse already retires this one.
  let sid = mux.h2.openStream()
  mux.h2.setSinkMode(sid)                 # gate the receive window; drainDownload acks it
  mux.sinkStreams.incl sid
  try:
    if connectTunnel:
      await mux.send(mux.h2.encodeRequestHead(sid, headers))   # no END_STREAM: send side open
    elif bodyStream != nil:
      await mux.send(mux.h2.encodeRequestHead(sid, headers))
      await mux.streamBody(sid, bodyStream, trailers)
    else:
      await mux.send(mux.h2.encodeRequest(sid, headers, body, trailers))
  except CatchableError:
    mux.reapStream(sid)                   # send/producer raised: RST + free the slot (#261)
    raise
  # Wait for the response headers. As in drainDownload, there is no yield between the
  # state checks and registering `recvReady`, so the reader (which runs only while we
  # await) cannot slip a wake in between: no lost wakeup.
  while true:
    if not mux.alive:
      mux.detachSink(sid)
      raise newException(IOError, "navi: http/2 connection closed")
    if mux.h2.streamReset(sid):
      let err = mux.resetError(sid)          # classify before detachSink clears flags
      mux.detachSink(sid)
      raise err
    if mux.h2.headersReady(sid): break
    if mux.h2.streamEnded(sid): break        # headers-only response (no body)
    if mux.h2.goneAway and mux.h2.streamUnprocessed(sid):
      # Above last-stream-id: not processed, retryable. At or below it, fall through
      # and keep waiting for headers -- the peer may still deliver them, and a real
      # close raises "connection closed" via the `not mux.alive` check above.
      mux.detachSink(sid)
      raise newException(UnprocessedError, "navi: http/2 request not processed")
    let ready = newFuture[void]("h2mux.recvhdr")
    mux.recvReady[sid] = ready
    await ready
    mux.recvReady.del(sid)
  return sid

proc openConnect*(mux: H2Mux, headers: seq[(string, string)]): Future[uint32] {.async.} =
  ## Open an Extended CONNECT (RFC 8441) tunnel stream and return its id once the
  ## response headers arrive (send side left open). The caller checks
  ## `respSnapshot(sid).status == 200`, then uses `tunnelSend` / `tunnelRecv`.
  ## Waits for the peer's SETTINGS and requires ENABLE_CONNECT_PROTOCOL first
  ## (RFC 8441), so an origin that does not support it fails fast and clearly.
  await mux.settingsSeen
  if not mux.alive:
    raise newException(IOError, "navi: http/2 connection closed")
  if not mux.h2.peerAllowsConnect:
    raise newException(ProtocolError,
      "navi: server does not support WebSocket over HTTP/2 " &
      "(no SETTINGS_ENABLE_CONNECT_PROTOCOL); use an h1 WebSocket")
  return await mux.sendAndReadHeaders(headers, "", connectTunnel = true)

proc tunnelSend*(mux: H2Mux, sid: uint32, data: string) {.async.} =
  ## Send `data` as DATA frames on a tunnel stream (never END_STREAM), waiting on
  ## the flow-control window like `streamBody` so buffered memory stays bounded.
  if not mux.alive: raise newException(IOError, "navi: http/2 connection closed")
  await mux.send(mux.h2.queueSend(sid, data))
  while mux.alive and not mux.h2.sendDrained(sid) and not mux.h2.streamDone(sid):
    await mux.waitSendable(sid)

proc tunnelRecv*(mux: H2Mux, sid: uint32): Future[string] =
  ## One inbound tunnel chunk, or "" once the peer half-closes (drops the stream).
  mux.readChunk(sid)

proc tunnelClose*(mux: H2Mux, sid: uint32) {.async.} =
  ## Half-close the send side (END_STREAM) if still open, best-effort. The caller
  ## then closes the whole mux (a WebSocket owns a dedicated h2 connection).
  if mux.alive and sid in mux.sinkStreams and not mux.h2.streamDone(sid):
    try: await mux.send(mux.h2.finishSend(sid))
    except CatchableError: discard

proc dropStream*(mux: H2Mux, sid: uint32) =
  ## Non-awaiting cleanup of an abandoned (never-drained) sink stream, for a
  ## destructor: free its slot and buffers so it cannot leak. A best-effort
  ## RST_STREAM is fired and forgotten (the event loop flushes it later); it may not
  ## be sent if the connection is already gone.
  if sid notin mux.sinkStreams: return
  mux.sinkStreams.excl sid
  mux.recvq.del(sid)
  mux.wakeRecver(sid)                          # wake a parked readChunk racing us
  mux.clearSendReady(sid)                     # a tunnel may have a parked send
  mux.decoders.del(sid)
  mux.releaseSlot()
  if mux.alive:
    let rst = mux.h2.resetStream(sid)        # also drops the stream in the conn
    if rst.len > 0:
      mux.fireSend(rst)
  else:
    discard mux.h2.takeResponse(sid)

proc abandon*(mux: H2Mux, sid: uint32): Future[void] {.async.} =
  ## Await-capable abandon (from `close`): RST the stream and flush it, then free its
  ## slot and buffers.
  if sid notin mux.sinkStreams: return
  mux.sinkStreams.excl sid
  mux.recvq.del(sid)
  mux.wakeRecver(sid)                          # wake a parked readChunk racing us
  mux.clearSendReady(sid)                     # a tunnel may have a parked send
  mux.decoders.del(sid)
  mux.releaseSlot()
  if mux.alive:
    let rst = mux.h2.resetStream(sid)
    if rst.len > 0:
      try: await mux.send(rst)
      except CatchableError: discard
  else:
    discard mux.h2.takeResponse(sid)

proc request*(mux: H2Mux, headers: seq[(string, string)], body: string,
              bodyStream: BodyProducer = nil,
              sink: BodySink = nil,
              trailers: seq[(string, string)] = @[]): Future[H2Response] {.async.} =
  ## Open a stream, send the request, and await this stream's response. Blocks while
  ## the connection is at the peer's MAX_CONCURRENT_STREAMS, resuming when a stream
  ## completes (so a burst of concurrent requests is queued, not RST). When
  ## `bodyStream` is set the body is streamed chunk by chunk instead of `body`.
  if not mux.alive:
    raise newException(IOError, "navi: http/2 connection not usable")
  while mux.alive and mux.activeStreams >= mux.h2.maxConcurrentStreams:
    let slot = newFuture[void]("h2mux.slot")
    mux.pendingSlots.addLast(slot)
    await slot
  if not mux.alive:
    raise newException(IOError, "navi: http/2 connection not usable")
  if mux.h2.goneAway:       # a GOAWAY landed while we waited: opening a new stream now
    raise newException(UnprocessedError,   # would break RFC 9113 6.8 (peer PROTOCOL_ERRORs
      "navi: http/2 request not processed") # and drops the conn). Retry on a fresh conn.
  if mux.h2.nextStreamExhausted:   # 2^31 stream ids used (RFC 9113 5.1.1): opening one
    raise newException(UnprocessedError,   # more would alias an old id. Retry on a fresh
      "navi: http/2 request not processed") # connection; canReuse already retires this one.
  # Streaming responses go through sendAndReadHeaders + readChunk/drainDownload on
  # the handle, not here, so this path is buffered: it waits for the whole response.
  # (`bodyStream` still streams the request body up.)
  let sid = mux.h2.openStream()
  let fut = newFuture[H2Response]("h2mux.stream")
  mux.waiters[sid] = fut
  try:
    if bodyStream != nil:
      await mux.send(mux.h2.encodeRequestHead(sid, headers))
      await mux.streamBody(sid, bodyStream, trailers)
    else:
      await mux.send(mux.h2.encodeRequest(sid, headers, body, trailers))
  except CatchableError:
    # The send or the user's BodyProducer raised (a file read error, etc.): the
    # waiter is registered, no END_STREAM/RST is on the wire, and the slot is held.
    # Left alone the mux stays pooled and reusable, so repeated producer failures
    # exhaust MAX_CONCURRENT_STREAMS. RST the stream and release the slot (issue #261).
    mux.reapStream(sid)
    raise
  result = await fut
