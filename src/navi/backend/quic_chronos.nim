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
## StreamTransport (which is TCP-only).
when not defined(naviHttp3):
  {.error: "navi/backend/quic_chronos is a -d:naviHttp3-only module.".}

import std/[strutils, tables]
import pkg/chronos
import ./quic
export quic

proc sockSend(fd: cint, buf: pointer, len: csize_t, flags: cint): int
  {.importc: "send", header: "<sys/socket.h>".}
proc sockRecv(fd: cint, buf: pointer, len: csize_t, flags: cint): int
  {.importc: "recv", header: "<sys/socket.h>".}

type
  QuicConnChronos* = ref object
    ## One multiplexed HTTP/3 connection to an origin. Reused across requests.
    c: pointer                        ## H3Conn* (nil once closed)
    fd: AsyncFD
    waiters: Table[int64, Future[void]]   ## buffered request: id -> done future
    recvReady: Table[int64, Future[void]] ## streaming: id -> per-cycle progress future
    wakeup: Future[void]              ## the reader's current wait; the fd-readable
                                      ## callback (or wake()) completes it
    alive*: bool
    readerDone: Future[void]

proc wake(qc: QuicConnChronos) =
  ## Poke the reader so it flushes a just-submitted request without waiting its
  ## timer. A lost poke is bounded by the capped wait below.
  if qc.wakeup != nil and not qc.wakeup.finished: qc.wakeup.complete()

proc onReadable(arg: pointer) {.gcsafe, raises: [].} =
  ## Persistent fd-readable callback (registered once via addReader2). chronos is
  ## level-triggered, so it re-fires while the socket is readable -- no lost wake.
  let qc = cast[QuicConnChronos](arg)
  if qc.wakeup != nil and not qc.wakeup.finished: qc.wakeup.complete()

proc step(qc: QuicConnChronos) {.async.} =
  ## One I/O cycle: drain outgoing datagrams, wait for readability / the QUIC timer
  ## / a wake, then feed incoming datagrams and run any due loss recovery.
  var buf: array[1500, uint8]
  var n = navi_h3_send(qc.c, addr buf[0], csize_t(buf.len))
  while n > 0:
    discard sockSend(cint(qc.fd), addr buf[0], csize_t(n), 0)
    n = navi_h3_send(qc.c, addr buf[0], csize_t(buf.len))
  if n < 0:
    raise newException(QuicError, "navi HTTP/3: send failed")

  # Cap the wait so a lost wake costs at most ~100 ms even on an idle connection.
  let to = min(int(navi_h3_timeout_ms(qc.c)), 100)
  let signal = newFuture[void]("navi.h3.wait")
  qc.wakeup = signal
  discard await one(signal, sleepAsync(milliseconds(to)))  # readable or timeout
  qc.wakeup = nil

  while true:                                   # drain incoming datagrams
    let r = sockRecv(cint(qc.fd), addr buf[0], csize_t(buf.len), 0)
    if r <= 0: break
    if navi_h3_recv(qc.c, addr buf[0], csize_t(r)) != 0:
      raise newException(QuicError, "navi HTTP/3: read_pkt failed")
  if navi_h3_timeout_ms(qc.c) == 0:
    if navi_h3_handle_timeout(qc.c) != 0:
      raise newException(QuicError, "navi HTTP/3: handle_timeout failed")

proc reader(qc: QuicConnChronos) {.async.} =
  ## The background reader: drive I/O and complete finished streams until the
  ## connection dies, then fail any survivors and free the connection.
  try:
    while qc.alive:
      await step(qc)
      for sid, fut in qc.waiters:                 # buffered: wake on stream done
        if not fut.finished and navi_h3_stream_done(qc.c, sid) != 0:
          fut.complete()
      for sid, fut in qc.recvReady:               # streaming: wake parked pulls
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

proc openConnChronos*(host: string, port: int, sni, caFile: string,
                      verify: bool): Future[QuicConnChronos] {.async.} =
  ## Open a QUIC connection, complete the handshake, bind the h3 session, and start
  ## the background reader. Raises `QuicError` on failure.
  let name = if sni.len > 0: sni else: host
  let c = navi_h3_new(host.cstring, ($port).cstring, name.cstring, caFile.cstring,
                      cint(verify))
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
      await step(qc)
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

proc requestOnConn*(qc: QuicConnChronos, verb, path: string,
                    headers: seq[(string, string)], body: string,
                    producer: proc(): string {.closure, raises: [CatchableError].} = nil,
                    trailers: seq[(string, string)] = @[]):
                    Future[Http3Response] {.async.} =
  ## Run one buffered HTTP/3 request on the shared connection, concurrently with
  ## others. Awaits the whole response. The request body is buffered (`body`) or
  ## streamed from `producer` (navi bodyStream); `trailers` are sent after the body.
  if not qc.alive:
    raise newException(QuicError, "navi HTTP/3 connection is closed")
  let reqHdr = encodeH3Fields(headers)
  let reqTrl = encodeH3Fields(trailers)
  var b = body
  let streamed = producer != nil
  let pe = if streamed: H3PullEnv(producer: producer) else: nil  # kept alive by this
  let pull = if streamed: h3PullThunk else: nil                  # async frame until done
  let bp = if not streamed and b.len > 0: cast[ptr char](addr b[0]) else: nil
  # Pin the pull env for the request's lifetime: the reader calls it from a
  # background task after this proc's last textual use (the submit), so the async
  # transform would otherwise let it be collected mid-request (use-after-free).
  if pe != nil: GC_ref(pe)
  let sid = navi_h3_submit(qc.c, verb.cstring, path.cstring, reqHdr.cstring, bp,
                           csize_t(if streamed: 0 else: b.len), pull, cast[pointer](pe),
                           reqTrl.cstring)
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
    raise newException(QuicError, "navi HTTP/3 stream was reset")
  if navi_h3_stream_length_mismatch(qc.c, sid) != 0:   # body != Content-Length: real
    raise newException(IOError, h3BodyLengthErr)        # response -> raise, no fallback
  var status: clong
  var blen, hlen, tlen: csize_t
  var rbody = newString(64 * 1024)
  var hbuf = newString(16 * 1024)
  var tbuf = newString(16 * 1024)
  if navi_h3_take_response(qc.c, sid, addr status, cast[ptr char](addr rbody[0]),
                           csize_t(rbody.len), addr blen,
                           cast[ptr char](addr hbuf[0]), csize_t(hbuf.len), addr hlen,
                           cast[ptr char](addr tbuf[0]), csize_t(tbuf.len),
                           addr tlen) != 0:
    raise newException(QuicError, "navi HTTP/3 take_response failed")
  consumed = true                          # take_response erased the C stream on success
  rbody.setLen(int(blen))
  hbuf.setLen(int(hlen))
  tbuf.setLen(int(tlen))
  result = Http3Response(status: int(status), body: rbody,
                         headers: parseH3Fields(hbuf), trailers: parseH3Fields(tbuf))

# --- streaming API (stream()/SSE over h3), twin of quic_async's ------------

proc waitProgress(qc: QuicConnChronos, sid: int64) {.async.} =
  let f = newFuture[void]("navi.h3.recv")
  qc.recvReady[sid] = f
  defer: qc.recvReady.del(sid)   # runs on cancellation/exception too, not just success
  wake(qc)
  await f

proc submitStream*(qc: QuicConnChronos, verb, path: string,
                   headers: seq[(string, string)], body: string,
                   pull: H3BodyPull = nil, pullEnv: pointer = nil,
                   trailers: seq[(string, string)] = @[]): int64 =
  if not qc.alive: return -1
  let reqHdr = encodeH3Fields(headers)
  let reqTrl = encodeH3Fields(trailers)
  var b = body
  let bp = if pull == nil and b.len > 0: cast[ptr char](addr b[0]) else: nil
  result = navi_h3_submit(qc.c, verb.cstring, path.cstring, reqHdr.cstring, bp,
                          csize_t(if pull != nil: 0 else: b.len), pull, pullEnv,
                          reqTrl.cstring)
  wake(qc)

proc awaitHeaders*(qc: QuicConnChronos, sid: int64):
    Future[tuple[status: int, headers: seq[(string, string)]]] {.async.} =
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

proc readStreamBody*(qc: QuicConnChronos, sid: int64): Future[string] {.async.} =
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

proc streamTrailers*(qc: QuicConnChronos, sid: int64): seq[(string, string)] =
  ## Response trailer fields of `sid` (after the body EOF); "" if none. Read after
  ## `readStreamBody` returns "" and before `freeStream`.
  if qc.c == nil: return
  var tbuf = newString(16 * 1024)
  var tlen: csize_t
  if navi_h3_response_trailers(qc.c, sid, cast[ptr char](addr tbuf[0]),
                               csize_t(tbuf.len), addr tlen) != 0:
    return
  tbuf.setLen(int(tlen))
  parseH3Fields(tbuf)

proc streamWasReset*(qc: QuicConnChronos, sid: int64): bool =
  qc.c != nil and navi_h3_stream_reset(qc.c, sid) != 0

proc streamLengthMismatch*(qc: QuicConnChronos, sid: int64): bool =
  qc.c != nil and navi_h3_stream_length_mismatch(qc.c, sid) != 0

proc freeStream*(qc: QuicConnChronos, sid: int64) =
  if qc.c != nil: navi_h3_stream_free(qc.c, sid)
  qc.recvReady.del(sid)

proc closeConn*(qc: QuicConnChronos): Future[void] {.async.} =
  ## Stop the reader and free the connection. Idempotent.
  if qc.c != nil and qc.alive:
    qc.alive = false
    wake(qc)
    await qc.readerDone
