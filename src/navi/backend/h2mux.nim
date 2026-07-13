## Shared HTTP/2 connection multiplexer for the asyncdispatch backend.
##
## One transport carries many concurrent streams. A single background reader
## owns the socket, feeds received bytes into the sans-io H2Conn, sends control
## frames back, and completes each request's per-stream Future as its response
## finishes. Requests just open a stream, send, and await their Future — so
## concurrent `await api.get(...)` calls to the same origin multiplex over one
## connection.

import std/[asyncdispatch, tables]
import ../proto/h2/conn
import ./asyncdispatch as be

type
  H2Mux* = ref object
    transport: be.Conn
    h2: H2Conn
    waiters: Table[uint32, Future[H2Response]]
    sendTail: Future[void]   ## tail of the serialized send chain
    alive: bool

proc dispatch(mux: H2Mux) =
  ## Resolve any streams that finished after the latest feed.
  var done: seq[uint32]
  for sid in mux.waiters.keys: done.add sid
  for sid in done:
    let fut = mux.waiters[sid]
    if fut.finished: continue
    if mux.h2.streamReset(sid):
      discard mux.h2.takeResponse(sid)
      mux.waiters.del(sid)
      fut.fail(newException(IOError, "navi: http/2 stream reset"))
    elif mux.h2.streamEnded(sid):
      let resp = mux.h2.takeResponse(sid)
      mux.waiters.del(sid)
      fut.complete(resp)
    elif mux.h2.goneAway:
      mux.waiters.del(sid)
      fut.fail(newException(IOError, "navi: http/2 connection went away"))

proc failAll(mux: H2Mux, msg: string) =
  mux.alive = false
  for sid, fut in mux.waiters:
    if not fut.finished:
      fut.fail(newException(IOError, msg))
  mux.waiters.clear()

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
      if toSend.len > 0: await mux.send(toSend)
      mux.dispatch()
      if mux.h2.goneAway and mux.waiters.len == 0: break
  except CatchableError:
    discard
  mux.failAll("navi: http/2 connection closed")
  try: await be.close(mux.transport)
  except CatchableError: discard

proc newH2Mux*(transport: be.Conn): Future[H2Mux] {.async.} =
  ## Take ownership of a freshly connected h2 transport, send the preface, and
  ## start the background reader.
  let mux = H2Mux(transport: transport, h2: initH2Conn(), alive: true,
                  waiters: initTable[uint32, Future[H2Response]]())
  await be.sendAll(transport, mux.h2.preamble())
  asyncCheck reader(mux)
  result = mux

proc canReuse*(mux: H2Mux): bool = mux.alive and mux.h2.canReuse

proc request*(mux: H2Mux, headers: seq[(string, string)],
              body: string): Future[H2Response] {.async.} =
  ## Open a stream, send the request, and await this stream's response.
  if not mux.alive:
    raise newException(IOError, "navi: http/2 connection not usable")
  let sid = mux.h2.openStream()
  let fut = newFuture[H2Response]("h2mux.stream")
  mux.waiters[sid] = fut
  await mux.send(mux.h2.encodeRequest(sid, headers, body))
  result = await fut
