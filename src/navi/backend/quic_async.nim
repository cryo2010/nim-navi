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
    waiters: Table[int64, Future[void]]  ## stream id -> completion future
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
      for sid, fut in qc.waiters:
        if not fut.finished and navi_h3_stream_done(qc.c, sid) != 0:
          fut.complete()
  except CatchableError:
    discard
  qc.alive = false
  for sid, fut in qc.waiters:
    if not fut.finished:
      fut.fail(newException(QuicError, "navi HTTP/3 connection closed"))
  qc.waiters.clear()
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
                    headers: seq[(string, string)],
                    body: string): Future[Http3Response] {.async.} =
  ## Run one HTTP/3 request on the shared connection, concurrently with others.
  if not qc.alive:
    raise newException(QuicError, "navi HTTP/3 connection is closed")
  var reqHdr = ""
  for (k, v) in headers:
    reqHdr.add k; reqHdr.add '\n'; reqHdr.add v; reqHdr.add '\n'
  var b = body
  let bp = if b.len > 0: cast[ptr char](addr b[0]) else: nil
  let sid = navi_h3_submit(qc.c, verb.cstring, path.cstring, reqHdr.cstring, bp,
                           csize_t(b.len))
  if sid < 0:
    raise newException(QuicError, "navi HTTP/3 submit failed")
  let fut = newFuture[void]("navi.h3.stream")
  qc.waiters[sid] = fut
  wake(qc)
  try:
    await fut
  finally:
    qc.waiters.del(sid)

  if navi_h3_stream_reset(qc.c, sid) != 0:
    # The stream was reset/aborted, not answered. Free its C-side entry (take
    # erases it), then raise so the engine falls back to h2/h1 for this request
    # instead of returning a bogus empty response.
    var s: clong
    var bl, hl: csize_t
    var rb = newString(1)
    var hb = newString(1)
    discard navi_h3_take_response(qc.c, sid, addr s, cast[ptr char](addr rb[0]),
                                  csize_t(rb.len), addr bl,
                                  cast[ptr char](addr hb[0]), csize_t(hb.len), addr hl)
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
  rbody.setLen(int(blen))
  hbuf.setLen(int(hlen))
  var hs: seq[(string, string)]
  let parts = hbuf.split('\n')
  var i = 0
  while i + 1 < parts.len:
    hs.add((parts[i], parts[i + 1]))
    i += 2
  result = Http3Response(status: int(status), body: rbody, headers: hs)

proc closeConn*(qc: QuicConnAsync): Future[void] {.async.} =
  ## Stop the reader and free the connection. Idempotent.
  if qc.c != nil and qc.alive:
    qc.alive = false
    wake(qc)                      # let the reader observe `not alive` and exit
    await qc.readerDone
