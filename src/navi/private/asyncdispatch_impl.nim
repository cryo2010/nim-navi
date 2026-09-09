# The native asyncdispatch implementation, `include`d by navi/asyncdispatch.nim
# on non-js targets. Kept separate so the entry can fall back to navi/js under
# `nim js` without pulling in std/asyncdispatch (which has no JS backend).

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
import navi/backend/[asyncdispatch, h2mux]
from std/strutils import startsWith, find, splitLines, contains, toLowerAscii
when defined(naviHttp3):
  import navi/core/altsvc
  import navi/backend/quic_async

claimEntry("navi/asyncdispatch")
export public, asyncdispatch

# --- per-backend prelude (provides what the shared body below relies on) ---

{.pragma: naviMwClosure, closure.}

when defined(naviHttp3):
  type QuicConn = QuicConnAsync
  template openQuicConn(host, port, sni, ca, verify: untyped): untyped =
    openConnAsync(host, port, sni, ca, verify)

template msOf(ms: int): int = ms

proc guard[T](totalMs: int; fut: Future[T];
              cancel: CancelToken): Future[T] {.async.} =
  ## Bound the whole request (all attempts) by `timeout` and `cancel`. On expiry
  ## or cancellation the abandoned future runs to completion in the background
  ## (asyncdispatch has no true cancellation); its socket is later reclaimed.
  let ms = totalMs
  if ms <= 0 and cancel == nil:
    return await fut
  var cancelFut = newFuture[void]("navi.cancel")
  if cancel != nil:
    cancel.armHook(proc() {.gcsafe, raises: [].} =
      # complete() only raises if already finished, which the guard rules out.
      {.cast(raises: []).}:
        if not cancelFut.finished: cancelFut.complete())
  try:
    if ms > 0:
      await fut or cancelFut or sleepAsync(ms)
    else:
      await fut or cancelFut
    if fut.finished:
      return fut.read
    if cancel != nil and cancel.cancelled:
      raise newException(RequestCancelledError, "navi: request cancelled")
    raise newException(TimeoutError, "navi: request timed out after " & $ms & " ms")
  finally:
    if cancel != nil: cancel.disarmHook()
    if not cancelFut.finished: cancelFut.complete()

include navi/private/impl_common

# kaRecv: per-backend (forward-declared in the shared body). asyncdispatch has no
# cancellation, so a single in-flight read is parked in `pendingRecv` and raced
# against a timeout via withTimeout, kept across timeouts so it is never orphaned.
proc kaRecv(ws: WebSocket): Future[string] {.async.} =
  ## One read chunk. With keepalive off, a plain read. With it on, a single
  ## outstanding read is kept in `pendingRecv` (so a timed-out read is never
  ## orphaned to steal the next bytes): on an idle interval send a ping, and on a
  ## second idle interval with a ping still unanswered, declare the peer dead.
  if ws.keepAlive <= 0: return await ws.recvRaw()
  while true:
    if ws.pendingRecv == nil: ws.pendingRecv = ws.recvRaw()
    if await withTimeout(ws.pendingRecv, ws.keepAlive):
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
