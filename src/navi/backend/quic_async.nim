## Async, multiplexed HTTP/3 driver for the asyncdispatch backend (phase 2h,
## slice 3). A `QuicConnAsync` carries many concurrent streams over one QUIC
## connection: a single background reader drives the connection's I/O (send /
## recv / QUIC timer) and completes each stream's future as it finishes, while
## `requestOnConn` submits a stream and awaits it. Imports quic for the FFI and
## the {.compile.}/link pragmas. -d:naviHttp3-only.
when not defined(naviHttp3):
  {.error: "navi/backend/quic_async is a -d:naviHttp3-only module (HTTP/3 WIP).".}

import std/[asyncdispatch, strutils, tables]
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

proc wake(qc: QuicConnAsync) =
  ## Poke the reader so it sends a just-submitted request without waiting for its
  ## timer. A lost poke (reader momentarily not waiting) is bounded by the capped
  ## wait below.
  if qc.wakeup != nil and not qc.wakeup.finished: qc.wakeup.complete()

proc step(qc: QuicConnAsync) {.async.} =
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
  # per iteration.
  let to = min(int(navi_h3_timeout_ms(qc.c)), 100)
  let signal = newFuture[void]("navi.h3.wait")
  qc.wakeup = signal
  addRead(qc.fd, proc(a: AsyncFD): bool =
    (if not signal.finished: signal.complete()); true)
  sleepAsync(to).addCallback(proc() =
    (if not signal.finished: signal.complete()))
  await signal
  qc.wakeup = nil

  while true:                                   # drain incoming datagrams
    let r = sockRecv(rawFd(qc.fd), addr buf[0], csize_t(buf.len), 0)
    if r <= 0: break
    if navi_h3_recv(qc.c, addr buf[0], csize_t(r)) != 0:
      raise newException(QuicError, "navi HTTP/3: read_pkt failed")
  if navi_h3_timeout_ms(qc.c) == 0:
    if navi_h3_handle_timeout(qc.c) != 0:
      raise newException(QuicError, "navi HTTP/3: handle_timeout failed")

proc reader(qc: QuicConnAsync) {.async.} =
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

proc openConnAsync*(host: string, port: int, sni, caFile: string,
                    verify: bool): Future[QuicConnAsync] {.async.} =
  ## Open a QUIC connection, complete the handshake, bind the h3 session, and
  ## start the background reader. Raises `QuicError` on failure.
  let name = if sni.len > 0: sni else: host
  let c = navi_h3_new(host.cstring, ($port).cstring, name.cstring, caFile.cstring,
                      cint(verify))
  if c == nil:
    raise newException(QuicError,
      "navi HTTP/3 connect to " & host & ":" & $port & " failed")
  let fd = navi_h3_fd(c).int.AsyncFD
  register(fd)
  let qc = QuicConnAsync(c: c, fd: fd, waiters: initTable[int64, Future[void]](),
                         recvReady: initTable[int64, Future[void]](),
                         alive: true,
                         readerDone: newFuture[void]("navi.h3.readerDone"))
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

proc requestOnConn*(qc: QuicConnAsync, verb, path: string,
                    headers: seq[(string, string)], body: string,
                    producer: proc(): string {.closure, raises: [CatchableError].} = nil):
                    Future[Http3Response] {.async.} =
  ## Run one HTTP/3 request on the shared connection, concurrently with others. The
  ## request body is buffered (`body`) or streamed from `producer` (navi bodyStream).
  if not qc.alive:
    raise newException(QuicError, "navi HTTP/3 connection is closed")
  var reqHdr = ""
  for (k, v) in headers:
    reqHdr.add k; reqHdr.add '\n'; reqHdr.add v; reqHdr.add '\n'
  var b = body
  let streamed = producer != nil
  let pe = if streamed: H3PullEnv(producer: producer) else: nil  # kept alive by this
  let pull = if streamed: h3PullThunk else: nil                  # async frame until done
  let bp = if not streamed and b.len > 0: cast[ptr char](addr b[0]) else: nil
  # The reader calls the pull env from a background task, long after this proc's last
  # textual use of `pe` (the submit below). That's before the await, so the async
  # transform would let `pe` be collected mid-request -> a use-after-free in the pull
  # callback. Pin it for the request's lifetime.
  if pe != nil: GC_ref(pe)
  let sid = navi_h3_submit(qc.c, verb.cstring, path.cstring, reqHdr.cstring, bp,
                           csize_t(if streamed: 0 else: b.len), pull, cast[pointer](pe))
  if sid < 0:
    if pe != nil: GC_unref(pe)
    raise newException(QuicError, "navi HTTP/3 submit failed")
  let fut = newFuture[void]("navi.h3.stream")
  qc.waiters[sid] = fut
  var consumed = false                     # true once take_response has taken the stream
  defer:
    qc.waiters.del(sid)
    if pe != nil: GC_unref(pe)
    if not consumed and qc.c != nil:       # cancelled, reset, or take failed: free the
      navi_h3_stream_free(qc.c, sid)       # C stream so an abandoned request isn't left
  wake(qc)
  await fut

  if navi_h3_stream_reset(qc.c, sid) != 0:
    # The stream was reset/aborted, not answered. The defer frees its C-side entry;
    # raise so the engine falls back to h2/h1 instead of a bogus empty response.
    raise newException(QuicError, "navi HTTP/3 stream was reset")

  var status: clong
  var blen, hlen: csize_t
  var rbody = newString(64 * 1024)
  var hbuf = newString(16 * 1024)
  if navi_h3_take_response(qc.c, sid, addr status, cast[ptr char](addr rbody[0]),
                           csize_t(rbody.len), addr blen,
                           cast[ptr char](addr hbuf[0]), csize_t(hbuf.len),
                           addr hlen) != 0:
    raise newException(QuicError, "navi HTTP/3 take_response failed")
  consumed = true                          # take_response erased the C stream on success
  rbody.setLen(int(blen))
  hbuf.setLen(int(hlen))
  var hs: seq[(string, string)]
  let parts = hbuf.split('\n')
  var i = 0
  while i + 1 < parts.len:
    hs.add((parts[i], parts[i + 1]))
    i += 2
  result = Http3Response(status: int(status), body: rbody, headers: hs)

# --- streaming API (for stream()/SSE over h3) ------------------------------
# Unlike requestOnConn (which awaits the whole buffered response), these let the
# caller read a response incrementally: submit, await headers, then pull body
# chunks. A parked pull waits on a per-stream `recvReady` future the reader wakes
# each cycle, then re-checks the C-side buffers (mirrors the h2 mux's recvReady).

proc waitProgress(qc: QuicConnAsync, sid: int64) {.async.} =
  ## Park until the reader makes a cycle (headers/body may have advanced).
  let f = newFuture[void]("navi.h3.recv")
  qc.recvReady[sid] = f
  defer: qc.recvReady.del(sid)    # runs on cancellation/exception too, not just success
  wake(qc)                        # poke the reader so we don't wait its full timer
  await f

proc submitStream*(qc: QuicConnAsync, verb, path: string,
                   headers: seq[(string, string)], body: string,
                   pull: H3BodyPull = nil, pullEnv: pointer = nil): int64 =
  ## Open an h3 stream for a streaming read; returns the stream id (< 0 on error).
  ## `pull`/`pullEnv` optionally stream the request body (the caller keeps the env
  ## alive until the stream ends).
  if not qc.alive: return -1
  var reqHdr = ""
  for (k, v) in headers:
    reqHdr.add k; reqHdr.add '\n'; reqHdr.add v; reqHdr.add '\n'
  var b = body
  let bp = if pull == nil and b.len > 0: cast[ptr char](addr b[0]) else: nil
  result = navi_h3_submit(qc.c, verb.cstring, path.cstring, reqHdr.cstring, bp,
                          csize_t(if pull != nil: 0 else: b.len), pull, pullEnv)
  wake(qc)

proc awaitHeaders*(qc: QuicConnAsync, sid: int64):
    Future[tuple[status: int, headers: seq[(string, string)]]] {.async.} =
  ## Await the response status + headers for `sid` (they arrive before any body).
  var status: clong
  var hbuf = newString(16 * 1024)
  var ready: cint
  while true:
    if not qc.alive: raise newException(QuicError, "navi HTTP/3 connection closed")
    var hlen: csize_t
    if navi_h3_response_headers(qc.c, sid, addr status, cast[ptr char](addr hbuf[0]),
                                csize_t(hbuf.len), addr hlen, addr ready) != 0:
      raise newException(QuicError, "navi HTTP/3 stream gone")
    if ready != 0:
      hbuf.setLen(int(hlen))
      var hs: seq[(string, string)]
      let parts = hbuf.split('\n')
      var i = 0
      while i + 1 < parts.len:
        hs.add((parts[i], parts[i + 1])); i += 2
      return (int(status), hs)
    await waitProgress(qc, sid)

proc readStreamBody*(qc: QuicConnAsync, sid: int64): Future[string] {.async.} =
  ## The next body chunk of `sid`, or "" at end of body. Parks until data lands.
  var buf = newString(64 * 1024)
  var eof: cint
  while true:
    if not qc.alive: raise newException(QuicError, "navi HTTP/3 connection closed")
    let n = navi_h3_read_body(qc.c, sid, cast[ptr char](addr buf[0]),
                              csize_t(buf.len), addr eof)
    if n < 0: raise newException(QuicError, "navi HTTP/3 stream gone")
    if n > 0:
      buf.setLen(int(n)); return buf
    if eof != 0: return ""
    await waitProgress(qc, sid)

proc streamWasReset*(qc: QuicConnAsync, sid: int64): bool =
  ## Whether `sid` ended by reset/abort rather than a clean end. Check at EOF.
  qc.c != nil and navi_h3_stream_reset(qc.c, sid) != 0

proc freeStream*(qc: QuicConnAsync, sid: int64) =
  ## Drop `sid` (after an EOF+reset check, or to abandon an undrained handle).
  if qc.c != nil: navi_h3_stream_free(qc.c, sid)
  qc.recvReady.del(sid)

proc closeConn*(qc: QuicConnAsync): Future[void] {.async.} =
  ## Stop the reader and free the connection. Idempotent.
  if qc.c != nil and qc.alive:
    qc.alive = false
    wake(qc)                      # let the reader observe `not alive` and exit
    await qc.readerDone
