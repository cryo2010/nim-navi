## Shared HTTP/2 connection multiplexer for the asyncdispatch backend.
##
## One transport carries many concurrent streams. A single background reader
## owns the socket, feeds received bytes into the sans-io H2Conn, sends control
## frames back, and completes each request's per-stream Future as its response
## finishes. Requests just open a stream, send, and await their Future -- so
## concurrent `await api.get(...)` calls to the same origin multiplex over one
## connection.
##
## Streaming responses (a `sink`) are owned by their own request coroutine, not
## the reader: the reader only feeds bytes and wakes a per-stream `recvReady`
## future, and the request's `drainDownload` loop drains the body, `await`s the
## sink, and acks the receive window per chunk. So a slow sink stalls that one
## stream (backpressure via the gated window) without blocking the reader or the
## other multiplexed streams.

import std/[asyncdispatch, tables, deques, sets]
import ../proto/h2/conn
import ../core/response          # for ResponseTooLargeError
import ../core/request           # for BodyProducer
import ../core/decompress        # for streaming response decompression
import ./asyncdispatch as be     # for Conn / BodySink

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
                                         ## awaiting the drain loop. Discrete entries keep
                                         ## delivery incremental (one sink call per feed)
                                         ## even if the reader out-runs the drain; bounded
                                         ## by the receive window (ack is gated by the sink)
    recvReady: Table[uint32, Future[void]]  ## a sink stream's drain loop waiting for
                                            ## the reader to feed more DATA
    decoders: Table[uint32, StreamDecoder]  ## per-sink-stream decoder, created lazily
                                            ## once headers are in (presence = chosen)
    decompress: bool                   ## decode content-encoding before the sink
    sendTail: Future[void]   ## tail of the serialized send chain
    alive: bool
    readerDone: Future[void] ## completed when the background reader has exited and
                             ## closed the transport, so `close` can join it
    settingsSeen: Future[void]  ## completed once the peer's initial SETTINGS is seen
                                ## (or the reader exits), so an Extended CONNECT can gate
                                ## on ENABLE_CONNECT_PROTOCOL before sending (RFC 8441)

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
  ## its buffer small and keeps delivery incremental: the drain pops chunks one at
  ## a time, so a fast reader can't collapse the whole body into a single sink call.
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

const goAwayGraceMs = 30_000
  ## After a GOAWAY, a peer promises (via last-stream-id) to finish the streams at or
  ## below it, so the reader keeps waiting for their responses. Bound that wait with
  ## a generous idle grace: a peer that sends GOAWAY and then neither delivers nor
  ## closes must not hang in-flight requests forever (the wait was otherwise bounded
  ## only by an optional read timeout). Reset on every received datagram, so a slow-
  ## but-live server draining its in-flight work is unaffected.

proc reader(mux: H2Mux) {.async.} =
  try:
    while mux.alive:
      let recvFut = be.recvSome(mux.transport)
      if mux.h2.goneAway and mux.activeStreams > 0:
        if not await withTimeout(recvFut, goAwayGraceMs): break  # peer went silent
      let chunk = await recvFut
      if chunk.len == 0: break                 # peer closed
      let toSend = mux.h2.feed(chunk)
      if toSend.len > 0: await mux.send(toSend)   # includes a GOAWAY on a conn error
      if mux.h2.sawPeerSettings and not mux.settingsSeen.finished:
        mux.settingsSeen.complete()               # unblocks a waiting Extended CONNECT
      mux.queueBodies()                           # move sink-stream body into recvq
      mux.wakeRecvers()                           # let sink drains pull new body + ack
      mux.dispatch()                              # complete finished buffered streams
      mux.wakeSenders()                           # a WINDOW_UPDATE may have drained a send
      if mux.h2.connError.len > 0: break          # fatal: fail all in-flight below
      if mux.h2.goneAway and mux.activeStreams == 0: break
  except CatchableError:
    discard
  mux.failAll("navi: http/2 connection closed")
  try: await be.close(mux.transport)   # the reader owns the transport close
  except CatchableError: discard
  if not mux.settingsSeen.finished: mux.settingsSeen.complete()  # unblock a pending
  if not mux.readerDone.finished: mux.readerDone.complete()      # openConnect (dead conn)

proc newH2Mux*(transport: be.Conn, maxBody = 0, decompress = false): Future[H2Mux] {.async.} =
  ## Take ownership of a freshly connected h2 transport, send the preface, and
  ## start the background reader.
  let mux = H2Mux(transport: transport, h2: initH2Conn(maxBody), alive: true,
                  decompress: decompress,
                  readerDone: newFuture[void]("h2mux.readerDone"),
                  settingsSeen: newFuture[void]("h2mux.settingsSeen"),
                  waiters: initTable[uint32, Future[H2Response]](),
                  sendReady: initTable[uint32, Future[void]](),
                  sinkStreams: initHashSet[uint32](),
                  recvq: initTable[uint32, Deque[string]](),
                  recvReady: initTable[uint32, Future[void]](),
                  decoders: initTable[uint32, StreamDecoder](),
                  pendingSlots: initDeque[Future[void]]())
  await be.sendAll(transport, mux.h2.preamble())
  asyncCheck reader(mux)
  result = mux

proc canReuse*(mux: H2Mux): bool = mux.alive and mux.h2.canReuse

proc close*(mux: H2Mux) {.async.} =
  ## Shut the shared connection down: fail any in-flight streams, wake the
  ## background reader (socket shutdown), and wait for it to exit and close the
  ## transport. Joining the reader (rather than closing the transport out from
  ## under it) avoids leaving it suspended on a dead fd, which crashes at teardown.
  if mux.readerDone.finished: return   # reader already exited (e.g. peer closed)
  mux.alive = false
  mux.failAll("navi: client closed")
  be.shutdownConn(mux.transport)       # unblock the reader's pending read/write
  await mux.readerDone                  # it observes EOF, closes the transport, exits

proc streamBody(mux: H2Mux, sid: uint32, bodyStream: BodyProducer,
                trailers: seq[(string, string)] = @[]) {.async.} =
  ## Send DATA frames pulled from `bodyStream`, pulling the next chunk only once
  ## the previous one has drained onto the wire (the reader releases window-blocked
  ## bytes on WINDOW_UPDATE and wakes us), so buffered upload memory stays ~one
  ## chunk. END_STREAM rides the final frame via `finishSend` (a trailing HEADERS
  ## block when `trailers` is set).
  while mux.alive and not mux.h2.streamDone(sid):
    if mux.h2.sendDrained(sid):
      let chunk = bodyStream()
      if chunk.len == 0:
        await mux.send(mux.h2.finishSend(sid, trailers))
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
  ## Free a sink stream's per-stream state once it is done (or errored), and
  ## release its concurrency slot so a request parked on MAX_CONCURRENT_STREAMS
  ## can proceed. Idempotent: called at the end of `readChunk` and again if a sink
  ## error unwinds through the drain loop, so the slot is released only on the call
  ## that actually removes the stream (guarded by `wasActive`) -- never twice.
  let wasActive = sid in mux.sinkStreams
  mux.recvReady.del(sid)
  mux.recvq.del(sid)
  mux.sinkStreams.excl sid
  mux.decoders.del(sid)
  discard mux.h2.takeResponse(sid)
  if wasActive: mux.releaseSlot()

proc readChunk*(mux: H2Mux, sid: uint32): Future[string] {.async.} =
  ## Pull one decoded body chunk of the sink stream `sid`, or "" once the stream
  ## ends (dropping the stream). Raises on reset / oversized / unprocessed / gone,
  ## like the old drain loop. The per-stream decoder lives in `mux.decoders`. The
  ## gated receive window is acked per chunk, so a slow puller backpressures the
  ## peer. Runs concurrently with the reader, which wakes `recvReady[sid]` when new
  ## DATA lands or the stream finishes.
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
      # Nothing pending and not finished: wait for the reader to feed more. There
      # is no yield between the checks above and registering here, so the reader
      # (which runs only while we await) cannot slip a wake in between -- no lost
      # wakeup. takeBody already drained all pending body, so nothing is missed.
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
      # native `string`, moved into the sink's async env with no copy.
      {.cast(gcsafe).}: await sink(c)
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
  ## Open a sink stream, send the request, and await only until the response
  ## HEADERS arrive; return the stream id with the stream left open and its body
  ## queuing into `recvq` for a later `drainDownload`. The header/body split lets a
  ## pull-based caller inspect status/headers before deciding to drain. The stream
  ## is in `sinkStreams` (gated receive window), so buffered body is bounded until
  ## the drain acks it. Raises on reset/goaway before headers, like `request`.
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
  let sid = mux.h2.openStream()
  mux.h2.setSinkMode(sid)                 # gate the receive window; drainDownload acks it
  mux.sinkStreams.incl sid
  if connectTunnel:
    await mux.send(mux.h2.encodeRequestHead(sid, headers))   # no END_STREAM: send side open
  elif bodyStream != nil:
    await mux.send(mux.h2.encodeRequestHead(sid, headers))
    await mux.streamBody(sid, bodyStream, trailers)
  else:
    await mux.send(mux.h2.encodeRequest(sid, headers, body, trailers))
  # Wait for the response headers. As in drainDownload, there is no yield between
  # the state checks and registering `recvReady`, so the reader (which runs only
  # while we await) cannot slip a wake in between: no lost wakeup.
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
    let ready = newFuture[void]("h2mux.tunnelsend")
    mux.sendReady[sid] = ready
    if mux.h2.sendDrained(sid) or mux.h2.streamDone(sid) or not mux.alive:
      mux.sendReady.del(sid)
    else:
      await ready
      mux.sendReady.del(sid)

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
  ## RST_STREAM is fired and forgotten (the event loop flushes it later); it may
  ## not be sent if the connection is already gone.
  if sid notin mux.sinkStreams: return
  mux.sinkStreams.excl sid
  mux.recvq.del(sid)
  mux.recvReady.del(sid)
  mux.decoders.del(sid)
  mux.releaseSlot()
  if mux.alive:
    let rst = mux.h2.resetStream(sid)        # also drops the stream in the conn
    if rst.len > 0:
      try: asyncCheck mux.send(rst)
      except CatchableError: discard
  else:
    discard mux.h2.takeResponse(sid)

proc abandon*(mux: H2Mux, sid: uint32): Future[void] {.async.} =
  ## Await-capable abandon (from `close`): RST the stream and flush it, then free
  ## its slot and buffers.
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
              sink: BodySink = nil,
              trailers: seq[(string, string)] = @[]): Future[H2Response] {.async.} =
  ## Open a stream, send the request, and await this stream's response. Blocks
  ## while the connection is at the peer's MAX_CONCURRENT_STREAMS, resuming when
  ## a stream completes (so a burst of concurrent requests is queued, not RST).
  ## When `bodyStream` is set the body is streamed chunk by chunk instead of `body`.
  ## When `sink` is set the response body is delivered to it incrementally as it
  ## arrives (the returned response's body is empty), gated by the sink so a slow
  ## consumer stalls only this stream (backpressure), instead of being buffered.
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
    await mux.streamBody(sid, bodyStream, trailers)
  else:
    await mux.send(mux.h2.encodeRequest(sid, headers, body, trailers))
  result = await fut
