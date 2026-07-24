## Shared HTTP/2 connection multiplexer for the asyncdispatch backend.
##
## One transport carries many concurrent streams. A single background reader
## owns the socket, feeds received bytes into the sans-io H2Conn, sends control
## frames back, and completes each request's per-stream Future as its response
## finishes. Requests just open a stream, send, and await their Future — so
## concurrent `await api.get(...)` calls to the same origin multiplex over one
## connection.

import std/[asyncdispatch, tables, deques]
import ../proto/h2/conn
import ../core/response          # for ResponseTooLargeError
import ./asyncdispatch as be

type
  H2Mux* = ref object
    transport: be.Conn
    h2: H2Conn
    waiters: Table[uint32, Future[H2Response]]
    pendingSlots: Deque[Future[void]]  ## requests waiting for a concurrency slot
    sendTail: Future[void]   ## tail of the serialized send chain
    alive: bool

proc releaseSlot(mux: H2Mux) =
  ## Wake one request waiting on MAX_CONCURRENT_STREAMS (a stream just freed up).
  while mux.pendingSlots.len > 0:
    let s = mux.pendingSlots.popFirst()
    if not s.finished:
      s.complete()
      break

proc dispatch(mux: H2Mux) =
  ## Resolve any streams that finished after the latest feed.
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
      let resp = mux.h2.takeResponse(sid)
      mux.waiters.del(sid)
      mux.releaseSlot()
      fut.complete(resp)
    elif mux.h2.goneAway:
      let unprocessed = mux.h2.streamUnprocessed(sid)
      mux.waiters.del(sid)
      if unprocessed:              # above GOAWAY's last id: not processed, retryable
        fut.fail(newException(UnprocessedError, "navi: http/2 request not processed"))
      else:
        fut.fail(newException(IOError, "navi: http/2 connection went away"))

proc failAll(mux: H2Mux, msg: string) =
  mux.alive = false
  for sid, fut in mux.waiters:
    if not fut.finished:
      fut.fail(newException(IOError, msg))
  mux.waiters.clear()
  while mux.pendingSlots.len > 0:                 # wake blocked requests; they see
    let s = mux.pendingSlots.popFirst()           # `not alive` and raise
    if not s.finished: s.complete()

proc send(mux: H2Mux, data: string) {.async.} =
  ## Serialize writes (chained on the previous send) so concurrent streams don't
  ## interleave frame bytes on the wire.
  let prev = mux.sendTail
  let mine = newFuture[void]("h2mux.send")
  mux.sendTail = mine
  if prev != nil and not prev.finished:
    await prev
  try:
    await be.sendAll(mux.transport, data)
  finally:
    mine.complete()

proc reader(mux: H2Mux) {.async.} =
  try:
    while mux.alive:
      let chunk = await be.recvSome(mux.transport)
      if chunk.len == 0: break                 # peer closed
      let toSend = mux.h2.feed(chunk)
      if toSend.len > 0: await mux.send(toSend)   # includes a GOAWAY on a conn error
      mux.dispatch()
      if mux.h2.connError.len > 0: break          # fatal: fail all in-flight below
      if mux.h2.goneAway and mux.waiters.len == 0: break
  except CatchableError:
    discard
  mux.failAll("navi: http/2 connection closed")
  try: await be.close(mux.transport)
  except CatchableError: discard

proc newH2Mux*(transport: be.Conn, maxBody = 0): Future[H2Mux] {.async.} =
  ## Take ownership of a freshly connected h2 transport, send the preface, and
  ## start the background reader.
  let mux = H2Mux(transport: transport, h2: initH2Conn(maxBody), alive: true,
                  waiters: initTable[uint32, Future[H2Response]](),
                  pendingSlots: initDeque[Future[void]]())
  await be.sendAll(transport, mux.h2.preamble())
  asyncCheck reader(mux)
  result = mux

proc canReuse*(mux: H2Mux): bool = mux.alive and mux.h2.canReuse

proc close*(mux: H2Mux) {.async.} =
  ## Shut the shared connection down: fail any in-flight streams and close the
  ## transport (which also frees its TLS context). The background reader unblocks
  ## on the closed transport and exits.
  if not mux.alive and mux.waiters.len == 0: return
  mux.failAll("navi: client closed")
  try: await be.close(mux.transport)
  except CatchableError: discard

proc request*(mux: H2Mux, headers: seq[(string, string)],
              body: string): Future[H2Response] {.async.} =
  ## Open a stream, send the request, and await this stream's response. Blocks
  ## while the connection is at the peer's MAX_CONCURRENT_STREAMS, resuming when
  ## a stream completes (so a burst of concurrent requests is queued, not RST).
  if not mux.alive:
    raise newException(IOError, "navi: http/2 connection not usable")
  while mux.alive and mux.waiters.len >= mux.h2.maxConcurrentStreams:
    let slot = newFuture[void]("h2mux.slot")
    mux.pendingSlots.addLast(slot)
    await slot
  if not mux.alive:
    raise newException(IOError, "navi: http/2 connection not usable")
  let sid = mux.h2.openStream()
  let fut = newFuture[H2Response]("h2mux.stream")
  mux.waiters[sid] = fut
  await mux.send(mux.h2.encodeRequest(sid, headers, body))
  result = await fut
