## Async, multiplexed HTTP/3 driver for the asyncdispatch backend. A
## `QuicConnAsync` carries many concurrent streams over one QUIC connection: a
## single background reader drives the connection's I/O (send / recv / QUIC timer)
## and completes each stream's future as it finishes, while `requestOnConn` and the
## streaming API submit streams and await them. Imports quic for the FFI and the
## {.compile.}/link pragmas. -d:naviHttp3-only.
##
## Only the fd-readiness core (wake / step / reader / waitProgress / openConnAsync)
## is written here; the request/stream/tunnel bookkeeping is the shared
## `quic_common.nim`, `include`d at the end. See that file for the split.
when not defined(naviHttp3):
  {.error: "navi/backend/quic_async is a -d:naviHttp3-only module.".}

import std/[asyncdispatch, strutils, tables]
import ../core/response   # ResponseTooLargeError, used by the shared quic_common fragment
import ./quic
export quic

proc sockSend(fd: cint, buf: pointer, len: csize_t, flags: cint): int
  {.importc: "send", header: "<sys/socket.h>".}
proc sockRecv(fd: cint, buf: pointer, len: csize_t, flags: cint): int
  {.importc: "recv", header: "<sys/socket.h>".}

template rawFd(fd: AsyncFD): cint = fd.int.cint

type
  QuicConnAsync* = ref object
    ## One multiplexed HTTP/3 connection to an origin. Reused across requests.
    c: pointer                        ## H3Conn* (nil once closed)
    fd: AsyncFD
    waiters: Table[int64, Future[void]]  ## buffered request: stream id -> done future
    recvReady: Table[int64, Future[void]] ## streaming: stream id -> "progress" future,
                                          ## woken each reader cycle so a parked
                                          ## headers/body pull re-checks the C buffers
    wakeup: Future[void]              ## the reader's current wait, wake() to poke it
    alive*: bool
    readerDone: Future[void]
  QuicConn = QuicConnAsync            ## the name the shared quic_common fragment uses

proc wake(qc: QuicConn) =
  ## Poke the reader so it sends a just-submitted request without waiting for its
  ## timer. A lost poke (reader momentarily not waiting) is bounded by the capped
  ## wait below.
  if qc.wakeup != nil and not qc.wakeup.finished: qc.wakeup.complete()

proc step(qc: QuicConn) {.async.} =
  ## One I/O cycle: drain outgoing datagrams, wait for readability / the QUIC
  ## timer / a wake, then feed incoming datagrams and run any due loss recovery.
  var buf: array[1500, uint8]
  var n = navi_h3_send(qc.c, addr buf[0], csize_t(buf.len))
  while n > 0:
    discard sockSend(rawFd(qc.fd), addr buf[0], csize_t(n), 0)
    n = navi_h3_send(qc.c, addr buf[0], csize_t(buf.len))
  if n < 0:
    raise newException(QuicError, "navi HTTP/3: send failed")

  # Cap the wait so a lost wake costs at most ~100 ms even on an idle connection.
  # Use sleepAsync (a heap timer) rather than addTimer, which would leak a timerfd
  # per iteration. The fd-readable callback is registered ONCE (openConnAsync), not
  # here: a per-step addRead leaks, since asyncdispatch only removes a read callback
  # when it fires. A wait that ends via the timer or a wake (the streaming pull wakes
  # on every park) leaves the callback registered forever, so a busy streaming read
  # accumulates them until the process is OOM-killed.
  let to = min(int(navi_h3_timeout_ms(qc.c)), 100)
  let signal = newFuture[void]("navi.h3.wait")
  qc.wakeup = signal
  sleepAsync(to).addCallback(proc() =
    (if not signal.finished: signal.complete()))
  await signal
  qc.wakeup = nil

  while true:                                   # drain incoming datagrams
    let r = sockRecv(rawFd(qc.fd), addr buf[0], csize_t(buf.len), 0)
    if r <= 0: break
    let rc = navi_h3_recv(qc.c, addr buf[0], csize_t(r))
    if rc < 0:
      raise newException(QuicError, "navi HTTP/3: read_pkt failed")
    if rc > 0:                    # peer closed gracefully: stop reading and let the reader
      qc.alive = false            # deliver completed streams, then tear down cleanly (#278)
      break
  if qc.alive and navi_h3_timeout_ms(qc.c) == 0:   # skip once a graceful close ended it (#278)
    if navi_h3_handle_timeout(qc.c) != 0:
      raise newException(QuicError, "navi HTTP/3: handle_timeout failed")

proc reader(qc: QuicConn) {.async.} =
  ## The background reader: drive I/O and complete finished streams until the
  ## connection dies, then fail any survivors and free the connection.
  try:
    while qc.alive:
      await step(qc)
      for sid, fut in qc.waiters:                 # buffered: wake on stream done
        if not fut.finished and navi_h3_stream_done(qc.c, sid) != 0:
          fut.complete()
      for sid, fut in qc.recvReady:               # streaming: wake parked pulls to
        if not fut.finished: fut.complete()        # re-check headers/body each cycle
  except CatchableError:
    discard
  qc.alive = false
  for sid, fut in qc.waiters:
    if not fut.finished:
      fut.fail(newException(QuicError, "navi HTTP/3 connection closed"))
  qc.waiters.clear()
  for sid, fut in qc.recvReady:                    # unblock parked streaming pulls;
    if not fut.finished: fut.complete()             # they observe `not alive` and end
  qc.recvReady.clear()
  unregister(qc.fd)
  navi_h3_close(qc.c)
  qc.c = nil
  if not qc.readerDone.finished: qc.readerDone.complete()

proc waitProgress(qc: QuicConn, sid: int64) {.async.} =
  ## Park until the reader makes a cycle (headers/body may have advanced). Unlike
  ## chronos, we must NOT poke the reader here: wake() completes the reader's wait
  ## synchronously, so asyncdispatch runs the continuation without returning to
  ## poll(). The per-cycle sleepAsync fallback timers then never fire (poll is
  ## starved) and pile up unbounded -- a busy streaming read OOMs in seconds. The
  ## reader is instead paced by real I/O: the fd-readable callback wakes it when
  ## body data lands, and its own 100ms fallback bounds an idle wait. (chronos can
  ## wake here because its `one()` cancels the loser timer; asyncdispatch cannot.)
  let f = newFuture[void]("navi.h3.recv")
  qc.recvReady[sid] = f
  defer: qc.recvReady.del(sid)    # runs on cancellation/exception too, not just success
  await f

proc openConnAsync*(host: string, port: int, sni, caFile: string,
                    verify: bool, maxBody: uint64 = 0): Future[QuicConnAsync] {.async.} =
  ## Open a QUIC connection, complete the handshake, bind the h3 session, and
  ## start the background reader. `maxBody` caps a buffered response body
  ## (0 = unlimited, navi maxResponseBytes). Raises `QuicError` on failure.
  let name = if sni.len > 0: sni else: host
  let c = navi_h3_new(host.cstring, ($port).cstring, name.cstring, caFile.cstring,
                      cint(verify), culonglong(maxBody))
  if c == nil:
    raise newException(QuicError,
      "navi HTTP/3 connect to " & host & ":" & $port & " failed")
  let fd = navi_h3_fd(c).int.AsyncFD
  register(fd)
  let qc = QuicConnAsync(c: c, fd: fd, waiters: initTable[int64, Future[void]](),
                         recvReady: initTable[int64, Future[void]](),
                         alive: true,
                         readerDone: newFuture[void]("navi.h3.readerDone"))
  # Persistent readable callback (matches chronos's addReader2): completes the
  # reader's current wait when the socket is readable. Returns false to stay
  # registered across cycles; dropped by unregister(fd) at teardown. Registering
  # this per step() instead would leak callbacks and OOM a busy streaming read.
  addRead(fd, proc(a: AsyncFD): bool =
    (if qc.wakeup != nil and not qc.wakeup.finished: qc.wakeup.complete()); false)
  try:
    while navi_h3_handshake_done(c) == 0:
      await step(qc)
    if navi_h3_bind(c) != 0:
      raise newException(QuicError, "navi HTTP/3 bind failed")
  except CatchableError:
    qc.alive = false
    unregister(fd)
    navi_h3_close(c)
    qc.c = nil
    raise
  asyncCheck reader(qc)
  return qc

include ./quic_common
