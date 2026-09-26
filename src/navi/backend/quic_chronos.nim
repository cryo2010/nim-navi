## Async, multiplexed HTTP/3 driver for the chronos backend -- the chronos twin of
## quic_async.nim. A `QuicConnChronos` carries many concurrent streams over one
## QUIC connection: a single background reader (asyncSpawn) drives the connection's
## I/O (send / recv / QUIC timer) and completes each stream's future, while
## requestOnConn / the streaming API submit streams and await them. Imports quic
## for the FFI and the {.compile.}/link pragmas. -d:naviHttp3-only.
##
## The QUIC transport, nghttp3 session, TLS and the UDP socket all live in the C
## driver (h3client.cpp, shared with the async backend); this file only pumps
## datagrams and drives the timer from chronos's event loop, waiting on the raw fd
## via chronos's low-level fd readiness (register2/addReader2) rather than a
## StreamTransport (which is TCP-only). Only that fd-readiness core lives here; the
## request/stream/tunnel bookkeeping is the shared `quic_common.nim`, `include`d at
## the end.
when not defined(naviHttp3):
  {.error: "navi/backend/quic_chronos is a -d:naviHttp3-only module.".}

import std/[strutils, tables]
import pkg/chronos
import ../core/response   # ResponseTooLargeError, used by the shared quic_common fragment
import ./quic
export quic

proc sockSend(fd: cint, buf: pointer, len: csize_t, flags: cint): int
  {.importc: "send", header: "<sys/socket.h>".}
proc sockRecv(fd: cint, buf: pointer, len: csize_t, flags: cint): int
  {.importc: "recv", header: "<sys/socket.h>".}

const wakeCapMs = 100
  ## Cap on one wait, so a lost poke costs at most this even on an idle connection.

type
  QuicConnChronos* = ref object
    ## One multiplexed HTTP/3 connection to an origin. Reused across requests.
    c: pointer                        ## H3Conn* (nil once closed)
    fd: AsyncFD
    waiters: Table[int64, Future[void]]   ## buffered request: id -> done future
    recvReady: Table[int64, Future[void]] ## streaming: id -> progress future, completed
                                          ## only on a cycle that moved bytes
    wakeup: Future[void]              ## the reader's current wait; the fd-readable
                                      ## callback (or wake()) completes it
    timer: Future[void]               ## the connection's ONE fallback sleep, re-armed
                                      ## only once it fired or would fire too late
    timerAt: Moment                   ## when `timer` fires
    flushPending: bool                ## a consumer queued frames (stream-window credit
                                      ## from a body read): the next park pokes the reader
    alive*: bool
    readerDone: Future[void]
  QuicConn = QuicConnChronos          ## the name the shared quic_common fragment uses

proc wake(qc: QuicConn) =
  ## Poke the reader so it flushes just-submitted or just-credited frames without
  ## waiting its timer. A lost poke is bounded by the capped wait below.
  qc.flushPending = false             # whatever was queued goes out on the next cycle
  if qc.wakeup != nil and not qc.wakeup.finished: qc.wakeup.complete()

proc onReadable(arg: pointer) {.gcsafe, raises: [].} =
  ## Persistent fd-readable callback (registered once via addReader2). chronos is
  ## level-triggered, so it re-fires while the socket is readable -- no lost wake.
  let qc = cast[QuicConn](arg)
  if qc.wakeup != nil and not qc.wakeup.finished: qc.wakeup.complete()

proc armTimer(qc: QuicConn) =
  ## Make sure a fallback sleep is pending that fires no later than this cycle needs,
  ## reusing the connection's existing one whenever possible.
  ##
  ## The wait used to race a fresh `sleepAsync` per cycle through `one()`. chronos's
  ## `one()` does not cancel the loser (its docstring says so outright: the other
  ## futures WILL NOT be cancelled) and `clearTimer` only nils a timer's callback,
  ## leaving the entry in the loop's timer list until its moment -- so every cycle left
  ## a live timer, an `one()` retFuture, a future seq and two closures behind.
  let due = int(navi_h3_timeout_ms(qc.c))
  # navi_h3_timeout_ms truncates to whole milliseconds, so an expiry less than 1 ms out
  # reads as 0; floor the wait at 1 ms so a not-quite-due QUIC timer cannot spin the
  # reader at poll rate (handle_timeout below still runs the moment it is really due).
  let to = max(1, min(due, wakeCapMs))
  let at = Moment.now() + to.milliseconds
  if qc.timer != nil and not qc.timer.finished() and qc.timerAt <= at:
    return                            # the pending one fires no later than we need
  let t = sleepAsync(to.milliseconds)
  qc.timer = t
  qc.timerAt = at
  # A closure, not the raw-pointer/GC_ref pairing `onReadable` uses: it holds `qc` by
  # refcount, so a timer still pending after teardown cannot touch a freed connection.
  # A superseded timer is left to expire on its own (cancelling would not free its slot
  # any sooner, and its poke costs at worst one extra idle cycle).
  t.addCallback(proc(udata: pointer) {.gcsafe, raises: [].} =
    if qc.wakeup != nil and not qc.wakeup.finished: qc.wakeup.complete())

proc step(qc: QuicConn): Future[bool] {.async.} =
  ## One I/O cycle: drain outgoing datagrams, wait for readability / the QUIC timer
  ## / a wake, then feed incoming datagrams and run any due loss recovery. Returns
  ## whether the cycle actually moved anything (datagrams out or in, or the connection
  ## ended), which is what paces the reader below.
  var buf: array[1500, uint8]
  var moved = false
  var n = navi_h3_send(qc.c, addr buf[0], csize_t(buf.len))
  while n > 0:
    moved = true
    discard sockSend(cint(qc.fd), addr buf[0], csize_t(n), 0)
    n = navi_h3_send(qc.c, addr buf[0], csize_t(buf.len))
  if n < 0:
    raise newException(QuicError, "navi HTTP/3: send failed")

  # Wait for readability, the QUIC timer or a poke: one future per cycle, against the
  # connection's single re-armed timer (no `one()`, whose loser leaks -- see armTimer).
  qc.armTimer()
  let signal = newFuture[void]("navi.h3.wait")
  qc.wakeup = signal
  await signal
  qc.wakeup = nil

  while true:                                   # drain incoming datagrams
    let r = sockRecv(cint(qc.fd), addr buf[0], csize_t(buf.len), 0)
    if r <= 0: break
    moved = true
    let rc = navi_h3_recv(qc.c, addr buf[0], csize_t(r))
    if rc < 0:
      raise newException(QuicError, "navi HTTP/3: read_pkt failed")
    if rc > 0:                    # peer closed gracefully: stop reading and let the reader
      qc.alive = false            # deliver completed streams, then tear down cleanly (#278)
      break
  if qc.alive and navi_h3_timeout_ms(qc.c) == 0:   # skip once a graceful close ended it (#278)
    if navi_h3_handle_timeout(qc.c) != 0:
      raise newException(QuicError, "navi HTTP/3: handle_timeout failed")
  return moved or not qc.alive

proc reader(qc: QuicConn) {.async.} =
  ## The background reader: drive I/O and complete finished streams until the
  ## connection dies, then fail any survivors and free the connection.
  try:
    while qc.alive:
      # Only re-check the streams on a cycle that moved bytes. Completing every parked
      # pull unconditionally (as this did) is what let the pump sustain itself: a
      # consumer's park poked the reader, the reader completed all parks, every
      # consumer re-checked, found nothing new and parked again -- a loop at poll rate
      # rather than at I/O rate, whose per-cycle futures and closures are cyclic and
      # float the heap up until ORC's cycle collector (whose trigger scales with the
      # live heap) gets to them. The asyncdispatch twin is paced by real I/O; this is
      # the same pacing, arrived at from the other side.
      if await step(qc):
        for sid, fut in qc.waiters:               # buffered: wake on stream done
          if not fut.finished and navi_h3_stream_done(qc.c, sid) != 0:
            fut.complete()
        for sid, fut in qc.recvReady:             # streaming: wake parked pulls
          if not fut.finished: fut.complete()
  except CatchableError:
    discard
  qc.alive = false
  for sid, fut in qc.waiters:
    if not fut.finished:
      fut.fail(newException(QuicError, "navi HTTP/3 connection closed"))
  qc.waiters.clear()
  for sid, fut in qc.recvReady:
    if not fut.finished: fut.complete()
  qc.recvReady.clear()
  discard removeReader2(qc.fd)
  discard unregister2(qc.fd)
  GC_unref(qc)                      # release the ref the readable callback borrowed
  navi_h3_close(qc.c)
  qc.c = nil
  if not qc.readerDone.finished: qc.readerDone.complete()

proc waitProgress(qc: QuicConn, sid: int64) {.async.} =
  ## Park until the reader's next cycle that actually moved bytes, then re-check the
  ## C-side buffers. Poke the reader only when there is something to flush -- a
  ## just-submitted stream pokes it itself, and a body read that consumed bytes
  ## returned stream-window credit that must go out (`flushPending`). Poking on every
  ## park instead made the pump self-sustaining (see `reader`) and spun the loop at
  ## poll rate, leaking a wait future, a timer and an `one()` retFuture per turn.
  let f = newFuture[void]("navi.h3.recv")
  qc.recvReady[sid] = f
  defer: qc.recvReady.del(sid)   # runs on cancellation/exception too, not just success
  if qc.flushPending: wake(qc)
  await f

proc openConnChronos*(host: string, port: int, sni, caFile: string,
                      verify: bool, maxBody: uint64 = 0): Future[QuicConnChronos] {.async.} =
  ## Open a QUIC connection, complete the handshake, bind the h3 session, and start
  ## the background reader. `maxBody` caps a buffered response body (0 = unlimited,
  ## navi maxResponseBytes). Raises `QuicError` on failure.
  let name = if sni.len > 0: sni else: host
  let c = navi_h3_new(host.cstring, ($port).cstring, name.cstring, caFile.cstring,
                      cint(verify), culonglong(maxBody))
  if c == nil:
    raise newException(QuicError,
      "navi HTTP/3 connect to " & host & ":" & $port & " failed")
  let fd = AsyncFD(navi_h3_fd(c))
  let qc = QuicConnChronos(c: c, fd: fd, waiters: initTable[int64, Future[void]](),
                           recvReady: initTable[int64, Future[void]](), alive: true,
                           readerDone: newFuture[void]("navi.h3.readerDone"))
  # Register the fd once; the persistent onReadable completes qc.wakeup. GC_ref keeps
  # qc alive for the raw pointer the callback borrows (released in the reader).
  GC_ref(qc)
  if register2(fd).isErr or
     addReader2(fd, onReadable, cast[pointer](qc)).isErr:
    GC_unref(qc)
    navi_h3_close(c)
    raise newException(QuicError, "navi HTTP/3: fd register failed")
  try:
    while navi_h3_handshake_done(c) == 0:
      discard await step(qc)
    if navi_h3_bind(c) != 0:
      raise newException(QuicError, "navi HTTP/3 bind failed")
  except CatchableError:
    qc.alive = false
    discard removeReader2(fd)
    discard unregister2(fd)
    GC_unref(qc)
    navi_h3_close(c)
    qc.c = nil
    raise
  asyncSpawn reader(qc)
  return qc

include ./quic_common
