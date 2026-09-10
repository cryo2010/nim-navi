# The native chronos implementation, `include`d by navi/chronos.nim on non-js
# targets. Kept separate so the entry can fall back to navi/js under `nim js`
# without pulling in the chronos package (which has no JavaScript backend).

import std/[options, tables]
import navi/private/[entryguard, streamguard]
import navi/proto/sse
import navi/core/public
export sse.SseEvent
import navi/core/[engine, pool, session, proxy, h2glue]
import navi/core/[redirect, cookies, digest, cancel, retry, response]
import navi/core/decompress   # StreamDecoder, for the readChunk decode state
import navi/proto/h1
import navi/proto/ws
import navi/backend/[chronos, h2mux_chronos]
from std/strutils import startsWith, find, splitLines, contains, toLowerAscii
when defined(naviHttp3):
  import navi/core/altsvc
  import navi/backend/quic_chronos

claimEntry("navi/chronos")
export public, chronos

# --- per-backend prelude (provides what the shared body below relies on) ---

{.pragma: naviMwClosure, closure, gcsafe.}

when defined(naviHttp3):
  type QuicConn = QuicConnChronos
  template openQuicConn(host, port, sni, ca, verify, maxBody: untyped): untyped =
    openConnChronos(host, port, sni, ca, verify, maxBody)

template msOf(ms: int): untyped = ms.milliseconds

proc guard[T](totalMs: int; fut: Future[T];
              cancel: CancelToken): Future[T] {.async.} =
  ## Bound the whole operation by `timeout` and `cancel`. On either, the in-flight
  ## request is cancelled via chronos structured cancellation (its cleanup closes
  ## the socket) and TimeoutError / RequestCancelledError is raised.
  let ms = totalMs
  if ms <= 0 and cancel == nil:
    return await fut
  var cancelFut = newFuture[void]("navi.cancel")
  if cancel != nil:
    cancel.armHook(proc() {.gcsafe, raises: [].} =
      if not cancelFut.finished: cancelFut.complete())
  var timer: Future[void] = nil
  if ms > 0: timer = sleepAsync(ms.milliseconds)
  try:
    var cands = @[FutureBase(fut), FutureBase(cancelFut)]
    if timer != nil: cands.add(FutureBase(timer))
    discard await race(cands)
    if fut.finished:
      return await fut
    await fut.cancelAndWait()
    if cancel != nil and cancel.cancelled:
      raise newException(RequestCancelledError, "navi: request cancelled")
    raise newException(TimeoutError, "navi: request timed out after " & $ms & " ms")
  finally:
    if cancel != nil: cancel.disarmHook()
    if not cancelFut.finished: cancelFut.complete()
    if timer != nil and not timer.finished: timer.cancelSoon()

include navi/private/impl_common

# kaRecv: per-backend (forward-declared in the shared body). chronos can't cancel a
# read without losing its buffered bytes, so the in-flight read is raced against a
# timer (cancelled on the read's completion) rather than withTimeout'd.
proc kaRecv(ws: WebSocket): Future[string] {.async.} =
  ## One read chunk. With keepalive off, a plain read. With it on, a single
  ## outstanding read is kept in `pendingRecv` (raced against a timer, so a timed-out
  ## read is never cancelled and cannot lose bytes): on an idle interval send a ping,
  ## and on a second idle interval with a ping still unanswered, declare the peer dead.
  if ws.keepAlive <= 0: return await ws.recvRaw()
  while true:
    if ws.pendingRecv == nil: ws.pendingRecv = ws.recvRaw()
    let timer = sleepAsync(ws.keepAlive.milliseconds)
    discard await race(ws.pendingRecv, timer)
    if ws.pendingRecv.finished:
      await timer.cancelAndWait()
      let chunk = ws.pendingRecv.read()     # completed (re-raises a read error)
      ws.pendingRecv = nil
      ws.pingOutstanding = false            # any inbound byte proves liveness
      return chunk
    if ws.pingOutstanding:                   # pinged last interval, still nothing back
      ws.open = false
      try: await ws.closeRaw() except CatchableError: discard
      raise newException(TimeoutError, "navi: websocket keepalive timed out")
    await ws.sendRaw(encodeFrame(opPing, ""))
    ws.pingOutstanding = true
