## Shared HTTP/2 connection multiplexer for the chronos backend.
##
## A chronos-Futures port of `backend/h2mux.nim` (the asyncdispatch multiplexer):
## one transport carries many concurrent streams, a single background reader owns
## the transport, feeds bytes into the sans-io `H2Conn`, sends control frames back,
## and completes each request's per-stream Future as its response finishes. The
## sans-io `proto/h2/conn` state machine is reused unchanged; only the async glue
## differs (chronos `Future`, `asyncSpawn`, and the chronos backend's Conn).
##
## Streaming responses (a `sink`) are owned by their own request coroutine, not the
## reader: the reader only feeds bytes and wakes a per-stream `recvReady`, and the
## request's `drainDownload` loop drains the body, `await`s the sink, and acks the
## receive window per chunk -- so a slow sink stalls only that one stream
## (backpressure via the gated window) without blocking the reader or other streams.

import std/[tables, deques, sets]
import pkg/chronos
import ../proto/h2/conn
import ../core/response          # for ResponseTooLargeError
import ../core/request           # for BodyProducer
import ../core/decompress        # for streaming response decompression
import ./chronos as be           # for Conn / BodySink

type
  H2Mux* = ref object
    transport: be.Conn
    h2: H2Conn
    waiters: Table[uint32, Future[H2Response]]
    pendingSlots: Deque[Future[void]]  ## requests waiting for a concurrency slot
    sendReady: Table[uint32, Future[void]]  ## streaming uploads waiting for the send
                                            ## window to drain their queued chunk
    sinkStreams: HashSet[uint32]       ## streams owned by a drainDownload coroutine
                                       ## (their body goes to a sink, not `waiters`)
    recvq: Table[uint32, Deque[string]]  ## raw body chunks the reader drained per feed,
                                         ## awaiting the drain loop (one entry per feed
                                         ## keeps delivery incremental; bounded by the
                                         ## receive window, whose ack is gated by the sink)
    recvReady: Table[uint32, Future[void]]  ## a sink stream's drain loop waiting for
                                            ## the reader to feed more DATA
    decoders: Table[uint32, StreamDecoder]  ## per-sink-stream decoder, created lazily
                                            ## once headers are in (presence = chosen)
    decompress: bool                   ## decode content-encoding before the sink
    sendTail: Future[void]   ## tail of the serialized send chain
    alive: bool
    readerDone: Future[void] ## completed once the reader has exited and the
                             ## transport is closed, so `close` can join it
    readerFut: Future[void]  ## the reader task itself, held so `close` can
                             ## `cancelAndWait` it (reaping its parked read cleanly)
    closing: bool            ## set by `close`, so the reader defers the transport
                             ## teardown to it rather than racing on a cancelled await

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

proc dispatch(mux: H2Mux) =
  ## Resolve any buffered streams that finished after the latest feed. Streaming
  ## (`sink`) streams are not in `waiters`; their own drain coroutine handles them.
  var done: seq[uint32]
  for sid in mux.waiters.keys: done.add sid
  for sid in done:
    let fut = mux.waiters[sid]
    if fut.finished: continue
    if mux.h2.streamReset(sid):
      let tooLarge = mux.h2.streamTooLarge(sid)
      let unprocessed = mux.h2.streamUnprocessed(sid)
      discard mux.h2.takeResponse(sid)
      mux.waiters.del(sid)
      mux.releaseSlot()
      if tooLarge:
        fut.fail(newException(ResponseTooLargeError,
          "navi: response exceeded maxResponseBytes"))
      elif unprocessed:
        fut.fail(newException(UnprocessedError, "navi: http/2 request not processed"))
      else:
        fut.fail(newException(IOError, "navi: http/2 stream reset"))
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
  ## Wake streaming uploads whose queued chunk has drained onto the wire (the
  ## reader just released a window-blocked tail on WINDOW_UPDATE), or whose stream
  ## is finished/gone, so they pull the next chunk or stop.
  var wake: seq[uint32]
  for sid, ready in mux.sendReady:
    if not ready.finished and (not mux.alive or mux.h2.goneAway or
        mux.h2.sendDrained(sid) or mux.h2.streamEnded(sid)):
      wake.add sid
  for sid in wake:
    let r = mux.sendReady[sid]
    if not r.finished: r.complete()

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
  if prev != nil and not prev.finished:
    await prev
  try:
    await be.sendAll(mux.transport, data)
  finally:
    mine.complete()

proc trySend(mux: H2Mux, data: string) {.async.} =
  ## Fire-and-forget send that swallows errors, so it is safe to `asyncSpawn`.
  try: await mux.send(data)
  except CatchableError: discard

const goAwayGraceMs = 30_000
  ## After a GOAWAY, bound the wait for the in-flight streams' responses with a
  ## generous idle grace (reset on each datagram), so a peer that sends GOAWAY and
  ## then neither delivers nor closes cannot hang in-flight requests forever. A slow-
  ## but-live server draining its work is unaffected. (chronos `withTimeout` cancels
  ## the pending read cleanly on expiry.)

proc reader(mux: H2Mux) {.async.} =
  try:
    while mux.alive:
      let recvFut = be.recvSome(mux.transport)
      if mux.h2.goneAway and mux.activeStreams > 0:
        if not await withTimeout(recvFut, goAwayGraceMs.milliseconds): break
      let chunk = await recvFut
      if chunk.len == 0: break                 # peer closed
      let toSend = mux.h2.feed(chunk)
      if toSend.len > 0: await mux.send(toSend)   # includes a GOAWAY on a conn error
      mux.queueBodies()                           # move sink-stream body into recvq
      mux.wakeRecvers()                           # let sink drains pull new body + ack
      mux.dispatch()                              # complete finished buffered streams
      mux.wakeSenders()                           # a WINDOW_UPDATE may have drained a send
      if mux.h2.connError.len > 0: break          # fatal: fail all in-flight below
      if mux.h2.goneAway and mux.activeStreams == 0: break
  except CatchableError:
    discard
  # When `close` is tearing us down it cancels this task (reaping the parked read)
  # and owns the transport close + readerDone itself, so awaiting here after
  # cancellation would just abort. Only self-exit (peer close / GOAWAY / error)
  # runs the teardown here.
  if not mux.closing:
    mux.failAll("navi: http/2 connection closed")
    try: await be.close(mux.transport)   # the reader owns the transport close
    except CatchableError: discard
    if not mux.readerDone.finished: mux.readerDone.complete()

proc newH2Mux*(transport: be.Conn, maxBody = 0, decompress = false): Future[H2Mux] {.async.} =
  ## Take ownership of a freshly connected h2 transport, send the preface, and
  ## start the background reader.
  let mux = H2Mux(transport: transport, h2: initH2Conn(maxBody), alive: true,
                  decompress: decompress,
                  readerDone: newFuture[void]("h2mux.readerDone"),
                  waiters: initTable[uint32, Future[H2Response]](),
                  sendReady: initTable[uint32, Future[void]](),
                  sinkStreams: initHashSet[uint32](),
                  recvq: initTable[uint32, Deque[string]](),
                  recvReady: initTable[uint32, Future[void]](),
                  decoders: initTable[uint32, StreamDecoder](),
                  pendingSlots: initDeque[Future[void]]())
  await be.sendAll(transport, mux.h2.preamble())
  mux.readerFut = reader(mux)   # held (not asyncSpawn'd) so close can cancelAndWait it
  result = mux

proc canReuse*(mux: H2Mux): bool = mux.alive and mux.h2.canReuse

proc close*(mux: H2Mux) {.async.} =
  ## Shut the shared connection down: fail any in-flight streams, close the
  ## transport so the reader's parked read completes with EOF, then let the reader
  ## unwind on its own. We deliberately do NOT cancel the reader: cancelling a
  ## chronos StreamTransport read in flight leaks the read's future/buffers, so we
  ## EOF it via `closeWait` instead. `closing` tells the reader to leave the
  ## transport teardown to us.
  if mux.readerDone.finished: return   # reader already exited (e.g. peer closed)
  mux.closing = true
  mux.alive = false
  mux.failAll("navi: client closed")
  try: await be.close(mux.transport)   # EOFs the reader's parked read (no cancel)
  except CatchableError: discard
  if mux.readerFut != nil and not mux.readerFut.finished:
    try: await mux.readerFut           # it observes EOF and returns; no cancellation
    except CatchableError: discard
  if not mux.readerDone.finished: mux.readerDone.complete()

proc streamBody(mux: H2Mux, sid: uint32, bodyStream: BodyProducer) {.async.} =
  ## Send DATA frames pulled from `bodyStream`, pulling the next chunk only once the
  ## previous one has drained onto the wire (the reader releases window-blocked bytes
  ## on WINDOW_UPDATE and wakes us), so buffered upload memory stays ~one chunk.
  ## END_STREAM rides the final frame via `finishSend`.
  while mux.alive and not mux.h2.streamDone(sid):
    if mux.h2.sendDrained(sid):
      # single-threaded client; the producer need not be gcsafe (see engine).
      var chunk: string
      {.cast(gcsafe).}: chunk = bodyStream()
      if chunk.len == 0:
        await mux.send(mux.h2.finishSend(sid))
        break
      await mux.send(mux.h2.queueSend(sid, chunk))
    else:
      let ready = newFuture[void]("h2mux.drain")
      mux.sendReady[sid] = ready
      if mux.h2.sendDrained(sid) or mux.h2.streamDone(sid) or not mux.alive:
        mux.sendReady.del(sid)                     # drained between check and register
      else:
        await ready
        mux.sendReady.del(sid)

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
      if mux.h2.streamReset(sid):
        if mux.h2.streamTooLarge(sid):
          raise newException(ResponseTooLargeError,
            "navi: response exceeded maxResponseBytes")
        elif mux.h2.streamUnprocessed(sid):
          raise newException(UnprocessedError, "navi: http/2 request not processed")
        else:
          raise newException(IOError, "navi: http/2 stream reset")
      if mux.recvq.hasKey(sid) and mux.recvq[sid].len > 0:
        var raw = mux.recvq[sid].popFirst()
        let rawLen = raw.len   # window is acked by raw (wire) bytes, captured before the move
        if not mux.decoders.hasKey(sid):
          mux.decoders[sid] = if mux.decompress:
              newStreamDecoder(mux.h2.respHeader(sid, "content-encoding")) else: nil
        let dec = mux.decoders[sid]
        let decoded =
          if dec != nil: dec.update(raw.toOpenArrayByte(0, raw.high)) else: move raw
        await mux.send(mux.h2.ackRecv(sid, rawLen))  # replenish window: gated by the puller
        if decoded.len > 0: return decoded
        continue                                     # decoder buffered input; pull more
      if mux.h2.streamEnded(sid):                    # ended and the queue is drained
        if mux.h2.streamLengthMismatch(sid):         # body != declared Content-Length
          raise newException(IOError, bodyLengthErr) # the except below drops the stream
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
                         bodyStream: BodyProducer = nil): Future[uint32] {.async.} =
  ## Open a sink stream, send the request, and await only until the response HEADERS
  ## arrive; return the stream id with the stream left open and its body queuing into
  ## `recvq` for a later `drainDownload`. The header/body split lets a pull-based
  ## caller inspect status/headers before deciding to drain. The stream is in
  ## `sinkStreams` (gated receive window), so buffered body is bounded until the
  ## drain acks it. Raises on reset/goaway before headers, like `request`.
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
  let sid = mux.h2.openStream()
  mux.h2.setSinkMode(sid)                 # gate the receive window; drainDownload acks it
  mux.sinkStreams.incl sid
  if bodyStream != nil:
    await mux.send(mux.h2.encodeRequestHead(sid, headers))
    await mux.streamBody(sid, bodyStream)
  else:
    await mux.send(mux.h2.encodeRequest(sid, headers, body))
  # Wait for the response headers. As in drainDownload, there is no yield between the
  # state checks and registering `recvReady`, so the reader (which runs only while we
  # await) cannot slip a wake in between: no lost wakeup.
  while true:
    if not mux.alive:
      mux.sinkStreams.excl sid
      mux.recvq.del(sid)
      discard mux.h2.takeResponse(sid)
      mux.releaseSlot()
      raise newException(IOError, "navi: http/2 connection closed")
    if mux.h2.streamReset(sid):
      let tooLarge = mux.h2.streamTooLarge(sid)
      let unprocessed = mux.h2.streamUnprocessed(sid)
      mux.sinkStreams.excl sid
      mux.recvq.del(sid)
      discard mux.h2.takeResponse(sid)
      mux.releaseSlot()
      if tooLarge:
        raise newException(ResponseTooLargeError, "navi: response exceeded maxResponseBytes")
      elif unprocessed:
        raise newException(UnprocessedError, "navi: http/2 request not processed")
      else:
        raise newException(IOError, "navi: http/2 stream reset")
    if mux.h2.headersReady(sid): break
    if mux.h2.streamEnded(sid): break        # headers-only response (no body)
    if mux.h2.goneAway and mux.h2.streamUnprocessed(sid):
      # Above last-stream-id: not processed, retryable. At or below it, fall through
      # and keep waiting for headers -- the peer may still deliver them, and a real
      # close raises "connection closed" via the `not mux.alive` check above.
      mux.sinkStreams.excl sid
      mux.recvq.del(sid)
      discard mux.h2.takeResponse(sid)
      mux.releaseSlot()
      raise newException(UnprocessedError, "navi: http/2 request not processed")
    let ready = newFuture[void]("h2mux.recvhdr")
    mux.recvReady[sid] = ready
    await ready
    mux.recvReady.del(sid)
  return sid

proc dropStream*(mux: H2Mux, sid: uint32) =
  ## Non-awaiting cleanup of an abandoned (never-drained) sink stream, for a
  ## destructor: free its slot and buffers so it cannot leak. A best-effort
  ## RST_STREAM is fired and forgotten (the event loop flushes it later); it may not
  ## be sent if the connection is already gone.
  if sid notin mux.sinkStreams: return
  mux.sinkStreams.excl sid
  mux.recvq.del(sid)
  mux.recvReady.del(sid)
  mux.decoders.del(sid)
  mux.releaseSlot()
  if mux.alive:
    let rst = mux.h2.resetStream(sid)        # also drops the stream in the conn
    if rst.len > 0:
      asyncSpawn mux.trySend(rst)
  else:
    discard mux.h2.takeResponse(sid)

proc abandon*(mux: H2Mux, sid: uint32): Future[void] {.async.} =
  ## Await-capable abandon (from `close`): RST the stream and flush it, then free its
  ## slot and buffers.
  if sid notin mux.sinkStreams: return
  mux.sinkStreams.excl sid
  mux.recvq.del(sid)
  mux.recvReady.del(sid)
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
              sink: BodySink = nil): Future[H2Response] {.async.} =
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
  # Streaming responses go through sendAndReadHeaders + readChunk/drainDownload on
  # the handle, not here, so this path is buffered: it waits for the whole response.
  # (`bodyStream` still streams the request body up.)
  let sid = mux.h2.openStream()
  let fut = newFuture[H2Response]("h2mux.stream")
  mux.waiters[sid] = fut
  if bodyStream != nil:
    await mux.send(mux.h2.encodeRequestHead(sid, headers))
    await mux.streamBody(sid, bodyStream)
  else:
    await mux.send(mux.h2.encodeRequest(sid, headers, body))
  result = await fut
