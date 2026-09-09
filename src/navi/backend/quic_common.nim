## Shared HTTP/3 stream bookkeeping, `include`d by both `quic_async.nim`
## (asyncdispatch) and `quic_chronos.nim` (chronos) at the END of each file. The
## request/stream/tunnel logic here is identical across the two backends; only the
## fd-readiness core -- `wake`, `step`, `reader`, `waitProgress`, and `openConn*` --
## genuinely differs (asyncdispatch's `addRead`/`sleepAsync` vs chronos's
## `addReader2`/`one`), so each includer defines those ABOVE the include, along with
## its own `QuicConn` object type (aliased to the public `QuicConnAsync` /
## `QuicConnChronos`). This fragment refers to the connection only as `QuicConn` and
## calls `wake` / `waitProgress`, which resolve to the includer's definitions.
##
## Not a standalone module: it references symbols the includer defines/imports, so
## do not `nim check` it directly.

proc requestOnConn*(qc: QuicConn, verb, path: string,
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
  # The reader calls the pull env from a background task, long after this proc's last
  # textual use of `pe` (the submit below). That's before the await, so the async
  # transform would let `pe` be collected mid-request -> a use-after-free in the pull
  # callback. Pin it for the request's lifetime.
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
    # The stream was reset/aborted, not answered. The defer frees its C-side entry;
    # raise so the engine falls back to h2/h1 instead of a bogus empty response.
    raise newException(QuicError, "navi HTTP/3 stream was reset")
  if navi_h3_stream_length_mismatch(qc.c, sid) != 0:
    # Cleanly ended but body != Content-Length: a real (received) response, so raise a
    # non-QuicError (IOError) that propagates rather than triggering the h2/h1 fallback.
    raise newException(IOError, h3BodyLengthErr)

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

# --- streaming API (for stream()/SSE over h3) ------------------------------
# Unlike requestOnConn (which awaits the whole buffered response), these let the
# caller read a response incrementally: submit, await headers, then pull body
# chunks. A parked pull waits on a per-stream `recvReady` future (`waitProgress`,
# defined per backend) that the reader wakes each cycle, then re-checks the C-side
# buffers (mirrors the h2 mux's recvReady).

proc submitStream*(qc: QuicConn, verb, path: string,
                   headers: seq[(string, string)], body: string,
                   pull: H3BodyPull = nil, pullEnv: pointer = nil,
                   trailers: seq[(string, string)] = @[]): int64 =
  ## Open an h3 stream for a streaming read; returns the stream id (< 0 on error).
  ## `pull`/`pullEnv` optionally stream the request body (the caller keeps the env
  ## alive until the stream ends); `trailers` are sent after the body.
  if not qc.alive: return -1
  let reqHdr = encodeH3Fields(headers)
  let reqTrl = encodeH3Fields(trailers)
  var b = body
  let bp = if pull == nil and b.len > 0: cast[ptr char](addr b[0]) else: nil
  result = navi_h3_submit(qc.c, verb.cstring, path.cstring, reqHdr.cstring, bp,
                          csize_t(if pull != nil: 0 else: b.len), pull, pullEnv,
                          reqTrl.cstring)
  wake(qc)

proc awaitHeaders*(qc: QuicConn, sid: int64):
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

proc readStreamBody*(qc: QuicConn, sid: int64): Future[string] {.async.} =
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

proc streamTrailers*(qc: QuicConn, sid: int64): seq[(string, string)] =
  ## Response trailer fields of `sid` (they land after the body EOF); "" if none.
  ## Read after `readStreamBody` returns "" and before `freeStream`.
  if qc.c == nil: return
  var tbuf = newString(16 * 1024)
  var tlen: csize_t
  if navi_h3_response_trailers(qc.c, sid, cast[ptr char](addr tbuf[0]),
                               csize_t(tbuf.len), addr tlen) != 0:
    return
  tbuf.setLen(int(tlen))
  parseH3Fields(tbuf)

proc streamWasReset*(qc: QuicConn, sid: int64): bool =
  ## Whether `sid` ended by reset/abort rather than a clean end. Check at EOF.
  qc.c != nil and navi_h3_stream_reset(qc.c, sid) != 0

proc streamLengthMismatch*(qc: QuicConn, sid: int64): bool =
  ## Whether `sid` ended cleanly but its body length disagreed with Content-Length.
  qc.c != nil and navi_h3_stream_length_mismatch(qc.c, sid) != 0

proc freeStream*(qc: QuicConn, sid: int64) =
  ## Drop `sid` (after an EOF+reset check, or to abandon an undrained handle).
  if qc.c != nil: navi_h3_stream_free(qc.c, sid)
  qc.recvReady.del(sid)

# --- WebSocket-over-h3 tunnel (RFC 9220 Extended CONNECT) ---------------------
# A CONNECT stream kept open for full-duplex frames: openConnect handshakes,
# tunnelSend/tunnelRecv move frame bytes, tunnelClose half-closes. The background
# reader drives the socket, so these just queue work and wake it.

proc openConnect*(qc: QuicConn, path: string, headers: seq[(string, string)],
                  protocol: string): Future[tuple[sid: int64, status: int]] {.async.} =
  ## Open an Extended CONNECT tunnel with `:protocol` and await its :status. The
  ## stream is left open for tunnelSend/tunnelRecv.
  if not qc.alive: raise newException(QuicError, "navi HTTP/3 connection closed")
  let reqHdr = encodeH3Fields(headers)
  let sid = navi_h3_open_connect(qc.c, path.cstring, reqHdr.cstring, protocol.cstring)
  if sid < 0: raise newException(QuicError, "navi: HTTP/3 Extended CONNECT failed to open")
  wake(qc)
  let (status, _) = await qc.awaitHeaders(sid)
  return (sid, status)

proc tunnelSend*(qc: QuicConn, sid: int64, data: string): Future[void] {.async.} =
  ## Send `data` as tunnel DATA on `sid` (never END_STREAM); the reader flushes it.
  if not qc.alive: raise newException(QuicError, "navi HTTP/3 connection closed")
  var d = data
  let p = if d.len > 0: cast[pointer](addr d[0]) else: nil
  if navi_h3_tunnel_send(qc.c, sid, p, csize_t(d.len)) != 0:   # copies d into the driver
    raise newException(QuicError, "navi: HTTP/3 tunnel send failed")
  wake(qc)

proc tunnelRecv*(qc: QuicConn, sid: int64): Future[string] =
  ## The next inbound tunnel chunk, or "" once the peer half-closes.
  qc.readStreamBody(sid)

proc tunnelClose*(qc: QuicConn, sid: int64) {.async.} =
  ## Half-close the send side, then drop the stream.
  if qc.alive and qc.c != nil:
    discard navi_h3_tunnel_close(qc.c, sid)
    wake(qc)
  qc.freeStream(sid)

proc closeConn*(qc: QuicConn): Future[void] {.async.} =
  ## Stop the reader and free the connection. Idempotent.
  if qc.c != nil and qc.alive:
    qc.alive = false
    wake(qc)                      # let the reader observe `not alive` and exit
    await qc.readerDone
