## Sans-io HTTP/2 client connection (RFC 9113).
##
## A persistent, multi-stream connection with no I/O. HPACK encode/decode
## contexts, the frame decoder, and settings are connection-wide and survive
## across requests, so the connection can be reused (and, with an async driver,
## multiplex concurrent streams). The caller:
##
##   1. sends `preamble()` once on a new connection
##   2. per request: `id = openStream()`, send `encodeRequest(id, ...)`
##   3. feeds received bytes into `feed(...)` (returns control bytes to send)
##      until `streamDone(id)`, then `takeResponse(id)`
##
## Request headers/bodies are assumed to fit the peer's limits (no send-side
## flow-control blocking yet); the full control-frame set is handled.

import std/[strutils, tables]
import ./frame, ./hpack

type
  H2Response* = object
    status*: int
    headers*: seq[(string, string)]
    body*: string

  Stream = ref object
    resp: H2Response
    ended: bool
    reset: bool
    refused: bool         ## RST_STREAM(REFUSED_STREAM): server did not process it
    tooLarge: bool        ## response body exceeded maxBodyBytes (we RST'd it)
    hdrBuf: string
    hdrEndStream: bool
    sawFinal: bool        ## the final (non-1xx) response HEADERS block has arrived
    recvPending: int      ## received bytes not yet acked with a WINDOW_UPDATE
    bodyTotal: int        ## total body bytes received (for the size cap; `resp.body`
                          ## is drained incrementally by `takeBody`)
    sinkMode: bool        ## hold the stream receive window until `ackRecv`, so a
                          ## slow sink backpressures the peer (see setSinkMode)
    sendBuf: string       ## request body not yet on the wire (flow-control bound)
    sendOff: int          ## bytes of sendBuf already sent
    sendWindow: int       ## per-stream send window (peer's INITIAL_WINDOW_SIZE)
    sendClosed: bool      ## the request body is complete; END_STREAM may be sent
    endSent: bool         ## END_STREAM has already been emitted for this body

  H2Conn* = ref object
    enc: HpackEncoder
    dec: HpackDecoder            ## connection-wide (dynamic table is per-direction)
    frames: FrameDecoder
    nextId: uint32
    maxFrameSize: int
    maxBodyBytes: int            ## cap on a response body; 0 disables (maxResponseBytes)
    streams: Table[uint32, Stream]
    sawFirstFrame: bool          ## the server's first frame must be SETTINGS (the preface)
    fatal: string                ## non-empty once a connection error tore the conn down
    goneAway*: bool
    goAwayLastId: uint32
    connSendWindow: int          ## connection-level send window (shared by streams)
    connRecvPending: int         ## received bytes not yet acked at the connection level
    peerInitialWindow: int       ## peer's SETTINGS_INITIAL_WINDOW_SIZE
    maxConcurrent: int           ## peer's SETTINGS_MAX_CONCURRENT_STREAMS

const
  defaultWindow = 65535          ## HTTP/2 default flow-control window (RFC 9113)
  maxHeaderListBytes = 128 * 1024
    ## Cap on a single response's accumulated (compressed) header block. Bounds
    ## memory against a CONTINUATION flood -- a peer sending endless CONTINUATION
    ## frames without END_HEADERS (CVE-2024-27316 and related). Generous for real
    ## headers; a stream that exceeds it is RST'd.
  recvWindowSize = 8 * 1024 * 1024
    ## Per-stream receive window we advertise (SETTINGS_INITIAL_WINDOW_SIZE), so a
    ## single download is not throttled to the 64 KiB default per round trip.
  streamReplenish = recvWindowSize div 2
  connReplenish = 4 * 1024 * 1024
    ## Batch flow-control replenishment: emit a WINDOW_UPDATE only when consumed-
    ## but-unacked bytes cross these thresholds, instead of one per DATA frame.

proc initH2Conn*(maxBody = 0): H2Conn =
  H2Conn(dec: initHpackDecoder(), nextId: 1, maxFrameSize: defaultMaxFrameSize,
         maxBodyBytes: maxBody, streams: initTable[uint32, Stream](),
         connSendWindow: defaultWindow, peerInitialWindow: defaultWindow,
         maxConcurrent: int.high)   # RFC 9113: unlimited until the peer says otherwise

proc maxConcurrentStreams*(c: H2Conn): int = c.maxConcurrent
  ## The peer's SETTINGS_MAX_CONCURRENT_STREAMS (int.high if not advertised).

proc preamble*(c: H2Conn): string =
  ## Connection preface, our SETTINGS (server push disabled, a large per-stream
  ## receive window), and a large connection-level WINDOW_UPDATE so downloads are
  ## not throttled to the 64 KiB default.
  result = connectionPreface
  result.add encodeSettings({settingsEnablePush: 0'u32,
                             settingsInitialWindowSize: uint32(recvWindowSize),
                             settingsMaxHeaderListSize: uint32(defaultMaxHeaderList)})
  result.add encodeWindowUpdate(0, 0x3fff0000'u32)

proc openStream*(c: H2Conn): uint32 =
  result = c.nextId
  c.nextId += 2
  c.streams[result] = Stream(sendWindow: c.peerInitialWindow)

proc flushSend(c: H2Conn, streamId: uint32, s: Stream, outbuf: var string) =
  ## Emit as many DATA frames as the stream and connection send windows allow.
  ## END_STREAM rides the final DATA frame once the body is closed (`sendClosed`);
  ## a body that closes with nothing pending gets an empty END_STREAM DATA frame.
  if s.endSent: return
  while s.sendOff < s.sendBuf.len:
    let avail = min(s.sendWindow, c.connSendWindow)
    if avail <= 0: return                      # windowed out; wait for a WINDOW_UPDATE
    let n = min(min(avail, c.maxFrameSize), s.sendBuf.len - s.sendOff)
    let last = s.sendClosed and s.sendOff + n >= s.sendBuf.len
    outbuf.add encodeData(streamId, s.sendBuf[s.sendOff ..< s.sendOff + n],
                          endStream = last)
    s.sendOff += n
    s.sendWindow -= n
    c.connSendWindow -= n
    if last: s.endSent = true
  if s.sendClosed and not s.endSent:           # closed with all bytes already sent
    outbuf.add encodeData(streamId, "", endStream = true)
    s.endSent = true

proc encodeHeaderFrames(c: H2Conn, streamId: uint32, headers: openArray[HeaderPair],
                        endStream: bool): string =
  ## Encode the header block as a HEADERS frame, splitting it across CONTINUATION
  ## frames when it exceeds the peer's max frame size (RFC 9113 6.2/6.10); a single
  ## oversized HEADERS frame would be a FRAME_SIZE_ERROR.
  let headerBlock = c.enc.encode(headers)
  let mfs = c.maxFrameSize
  if headerBlock.len <= mfs:
    result = encodeHeaders(streamId, headerBlock, endStream = endStream,
                           endHeaders = true)
  else:
    result = encodeHeaders(streamId, headerBlock[0 ..< mfs],
                           endStream = endStream, endHeaders = false)
    var i = mfs
    while i < headerBlock.len:
      let n = min(mfs, headerBlock.len - i)
      result.add encodeContinuation(streamId, headerBlock[i ..< i + n],
                                    endHeaders = i + n >= headerBlock.len)
      i += n

proc encodeRequest*(c: H2Conn, streamId: uint32, headers: openArray[HeaderPair],
                    body: string): string =
  ## `headers` must start with the pseudo-headers (:method, :scheme, :path,
  ## :authority) in order, followed by regular headers. Sends the header block
  ## and as much of the (buffered) body as the send window allows now; the rest is
  ## released by `feed` as the peer sends WINDOW_UPDATE frames.
  let hasBody = body.len > 0
  result = c.encodeHeaderFrames(streamId, headers, endStream = not hasBody)
  if hasBody:
    let s = c.streams[streamId]
    s.sendBuf = body
    s.sendClosed = true                        # whole body known: END_STREAM on last DATA
    c.flushSend(streamId, s, result)

proc encodeRequestHead*(c: H2Conn, streamId: uint32,
                        headers: openArray[HeaderPair]): string =
  ## HEADERS for a request whose body is streamed via `queueSend`/`finishSend`.
  ## END_STREAM is not set here; it rides the last DATA frame.
  c.encodeHeaderFrames(streamId, headers, endStream = false)

proc sendDrained*(c: H2Conn, streamId: uint32): bool =
  ## True when every queued request-body byte is on the wire, so the caller may
  ## pull the next chunk -- keeping buffered upload memory to ~one chunk.
  let s = c.streams.getOrDefault(streamId)
  s == nil or s.sendOff >= s.sendBuf.len

proc queueSend*(c: H2Conn, streamId: uint32, data: string): string =
  ## Append a streamed request-body chunk and emit as much as the send window
  ## allows now; the remainder is released by `feed` on WINDOW_UPDATE. Compacts
  ## the already-sent prefix so buffered memory stays bounded.
  let s = c.streams.getOrDefault(streamId)
  if s == nil or data.len == 0: return
  if s.sendOff > 0:                            # drop the sent prefix
    s.sendBuf = s.sendBuf[s.sendOff .. ^1]
    s.sendOff = 0
  s.sendBuf.add data
  c.flushSend(streamId, s, result)

proc finishSend*(c: H2Conn, streamId: uint32): string =
  ## Mark the streamed request body complete. Emits END_STREAM -- on the last DATA
  ## frame, or an empty DATA frame if nothing is pending. Bytes still blocked by
  ## the send window are released (with END_STREAM) by `feed` on WINDOW_UPDATE.
  let s = c.streams.getOrDefault(streamId)
  if s == nil: return
  s.sendClosed = true
  c.flushSend(streamId, s, result)

proc replenishConn(c: H2Conn, n: int, outbuf: var string) =
  ## Give back connection-level receive-window credit for `n` consumed DATA bytes,
  ## batched. Every DATA frame's payload counts against the connection window,
  ## regardless of stream state (RFC 9113 6.9.1) -- see the DATA handler.
  c.connRecvPending += n
  if c.connRecvPending >= connReplenish:
    outbuf.add encodeWindowUpdate(0, uint32(c.connRecvPending))
    c.connRecvPending = 0

proc replenishRecv(c: H2Conn, sid: uint32, s: Stream, n: int, outbuf: var string) =
  ## Give back stream- and connection-level receive-window credit for `n` consumed
  ## bytes on an active stream, batched: emit a WINDOW_UPDATE only once the unacked
  ## total crosses the threshold, so a large download costs a handful of control
  ## frames instead of one per DATA frame.
  s.recvPending += n
  if s.recvPending >= streamReplenish:
    outbuf.add encodeWindowUpdate(sid, uint32(s.recvPending))
    s.recvPending = 0
  c.replenishConn(n, outbuf)

proc applyHeaders(c: H2Conn, s: Stream) =
  # HPACK is stateful, so every block must be decoded even when its fields are
  # dropped -- otherwise the dynamic table desyncs and later blocks corrupt.
  var status = 0
  var headers: seq[(string, string)]
  for (name, value) in c.dec.decode(s.hdrBuf):
    if name == ":status":
      try: status = parseInt(value)
      except ValueError: discard
    elif not name.startsWith(":"):
      headers.add((name, value))
  s.hdrBuf.setLen(0)
  if s.sawFinal:
    # A header block after the final response is trailers (RFC 9113 8.1). navi
    # does not surface trailers; they were decoded above (HPACK sync) and dropped.
    if s.hdrEndStream: s.ended = true
    return
  if status in 100 .. 199:
    # Interim response (100 Continue, 103 Early Hints, ...): its headers do not
    # belong to the final response, and it never carries END_STREAM. Discard it;
    # the final response follows in a later HEADERS block.
    return
  s.sawFinal = true
  s.resp.status = status
  for h in headers: s.resp.headers.add(h)
  if s.hdrEndStream: s.ended = true

proc connFail(c: H2Conn, code: uint32, reason: string, outbuf: var string) =
  ## Fatal connection error: send GOAWAY and mark the connection unusable. Every
  ## stream then reports done (see `streamDone`), so drivers unwind and close.
  if c.fatal.len == 0:
    c.fatal = reason
    outbuf.add encodeGoAway(0, code)

proc unpad(c: H2Conn, f: Frame, frag: var string, outbuf: var string): bool =
  ## Strip DATA/HEADERS padding (RFC 9113 6.1/6.2): the first payload byte is the
  ## pad length, and that many trailing bytes are the padding. Sets `frag` to the
  ## content in between. A pad length that meets or exceeds the payload is a
  ## PROTOCOL_ERROR (GOAWAY sent, returns false).
  if f.payload.len < 1:
    c.connFail(errProtocolError, "padded frame with no pad length", outbuf)
    return false
  let padLen = int(uint8(f.payload[0]))
  if padLen > f.payload.len - 1:
    c.connFail(errProtocolError, "padding exceeds frame payload", outbuf)
    return false
  frag = f.payload[1 ..< f.payload.len - padLen]
  true

proc handle(c: H2Conn, f: Frame, outbuf: var string) =
  if not c.sawFirstFrame:                     # RFC 9113 3.4: server preface is SETTINGS
    c.sawFirstFrame = true
    if f.typ != uint8(ftSettings):
      c.connFail(errProtocolError, "server preface: first frame not SETTINGS", outbuf)
      return
  case f.typ
  of uint8(ftSettings):
    if (f.flags and flagAck) == 0:
      for (id, value) in parseSettings(f.payload):
        if id == settingsMaxFrameSize and value >= 16384'u32:
          c.maxFrameSize = int(value)
        elif id == settingsMaxConcurrentStreams:
          c.maxConcurrent = int(value)
        elif id == settingsInitialWindowSize:
          # Adjust every open stream's send window by the delta (RFC 9113 6.9.2),
          # then release any body the new room allows.
          let delta = int(value) - c.peerInitialWindow
          c.peerInitialWindow = int(value)
          for sid, s in c.streams:
            s.sendWindow += delta
            c.flushSend(sid, s, outbuf)
      outbuf.add encodeSettingsAck()
  of uint8(ftPing):
    if (f.flags and flagAck) == 0:
      outbuf.add encodePing(f.payload, ack = true)
  of uint8(ftGoAway):
    c.goneAway = true
    c.goAwayLastId = readU32(f.payload, 0) and 0x7fffffff'u32
  of uint8(ftHeaders), uint8(ftContinuation):
    let s = c.streams.getOrDefault(f.streamId)
    if s != nil and not s.reset:
      var frag = f.payload
      if f.typ == uint8(ftHeaders):
        # Only HEADERS carries padding / priority; a CONTINUATION is a raw
        # fragment. Strip the pad byte + trailing padding, then the 5-byte
        # priority block (stream dependency + weight), so what is left is the
        # header block fragment HPACK expects.
        if (f.flags and flagPadded) != 0 and not c.unpad(f, frag, outbuf): return
        if (f.flags and flagPriority) != 0:
          if frag.len < 5:
            c.connFail(errProtocolError, "HEADERS priority block truncated", outbuf)
            return
          frag = frag[5 ..< frag.len]
      s.hdrBuf.add frag
      if s.hdrBuf.len > maxHeaderListBytes:       # CONTINUATION flood: bound and RST
        outbuf.add encodeRstStream(f.streamId, errEnhanceYourCalm)
        s.reset = true; s.ended = true; s.hdrBuf.setLen(0)
      else:
        if f.typ == uint8(ftHeaders):
          s.hdrEndStream = (f.flags and flagEndStream) != 0
        if (f.flags and flagEndHeaders) != 0:
          # Any HPACK decoding failure (truncated block, integer overflow, a
          # table-size update over the advertised max, or a header list past
          # SETTINGS_MAX_HEADER_LIST_SIZE) is a connection-level COMPRESSION_ERROR.
          try: c.applyHeaders(s)
          except ValueError as e:
            c.connFail(errCompressionError, e.msg, outbuf)
            return
  of uint8(ftData):
    # Every DATA payload -- pad length byte and padding included (RFC 9113 6.9.1)
    # -- counts against the connection flow-control window, even on a stream we
    # have reset or never opened. Skipping that leaks the window and eventually
    # stalls a long-lived pooled/mux connection.
    let s = c.streams.getOrDefault(f.streamId)
    if s != nil and not s.reset:
      var data = f.payload
      if (f.flags and flagPadded) != 0 and not c.unpad(f, data, outbuf): return
      s.resp.body.add data
      s.bodyTotal += data.len
      if c.maxBodyBytes > 0 and s.bodyTotal > c.maxBodyBytes:  # over the size cap: RST
        outbuf.add encodeRstStream(f.streamId, errCancel)
        s.reset = true; s.ended = true; s.tooLarge = true
        c.replenishConn(f.payload.len, outbuf)   # still owe the connection window
      else:
        if f.payload.len > 0:
          if s.sinkMode:
            # Hold the stream window until the sink consumes (via ackRecv); still
            # replenish the shared connection window so other streams don't stall.
            c.replenishConn(f.payload.len, outbuf)
          else:
            c.replenishRecv(f.streamId, s, f.payload.len, outbuf)
        if (f.flags and flagEndStream) != 0: s.ended = true
    elif f.payload.len > 0:
      c.replenishConn(f.payload.len, outbuf)     # reset/unknown stream: keep the conn window in sync
  of uint8(ftRstStream):
    let s = c.streams.getOrDefault(f.streamId)
    if s != nil:
      if f.payload.len >= 4 and readU32(f.payload, 0) == errRefusedStream:
        s.refused = true                       # not processed -> safe to retry
      s.reset = true
      s.ended = true
  of uint8(ftWindowUpdate):
    let inc = int(readU32(f.payload, 0) and 0x7fffffff'u32)
    if f.streamId == 0:                        # connection-level: release all streams
      c.connSendWindow += inc
      for sid, s in c.streams:
        c.flushSend(sid, s, outbuf)
    else:
      let s = c.streams.getOrDefault(f.streamId)
      if s != nil:
        s.sendWindow += inc
        c.flushSend(f.streamId, s, outbuf)
  of uint8(ftPushPromise):
    # We advertised SETTINGS_ENABLE_PUSH=0, so a PUSH_PROMISE is a PROTOCOL_ERROR.
    c.connFail(errProtocolError, "unexpected PUSH_PROMISE (push disabled)", outbuf)
  else:
    discard # PRIORITY, unknown types: ignore

proc feed*(c: H2Conn, data: string): string =
  ## Consume received bytes; return control bytes (ACKs, window updates) to send.
  c.frames.feed(data)
  var f: Frame
  while c.frames.next(f):
    c.handle(f, result)
  if c.frames.frameSizeError:                 # a peer frame exceeded the max frame size
    c.connFail(errFrameSizeError, "frame size error", result)

proc connError*(c: H2Conn): string = c.fatal
  ## Non-empty when a connection error (bad preface, oversized frame, unexpected
  ## PUSH_PROMISE) tore the connection down; the request should fail and not reuse it.

proc streamDone*(c: H2Conn, streamId: uint32): bool =
  ## True when the stream has ended (or been reset), or the connection is gone.
  if c.goneAway or c.fatal.len > 0: return true
  let s = c.streams.getOrDefault(streamId)
  s != nil and s.ended

proc streamEnded*(c: H2Conn, streamId: uint32): bool =
  ## The stream itself received END_STREAM or RST_STREAM (independent of GOAWAY).
  let s = c.streams.getOrDefault(streamId)
  s != nil and s.ended

proc streamReset*(c: H2Conn, streamId: uint32): bool =
  let s = c.streams.getOrDefault(streamId)
  s != nil and s.reset

proc streamTooLarge*(c: H2Conn, streamId: uint32): bool =
  ## The stream was RST because its body exceeded `maxBodyBytes`.
  let s = c.streams.getOrDefault(streamId)
  s != nil and s.tooLarge

proc streamUnprocessed*(c: H2Conn, streamId: uint32): bool =
  ## The peer signalled the request was not processed -- RST_STREAM with
  ## REFUSED_STREAM, or a stream id above GOAWAY's last-processed id -- so it is
  ## safe to retry even a non-idempotent method.
  let s = c.streams.getOrDefault(streamId)
  (s != nil and s.refused) or (c.goneAway and streamId > c.goAwayLastId)

proc takeBody*(c: H2Conn, streamId: uint32): string =
  ## Drain and return the response-body bytes buffered for `streamId` so far, for
  ## incremental delivery to a sink; clears the buffer so a large download stays
  ## bounded rather than accumulating the whole body. (`respHeader` reads the
  ## content-encoding to decode as chunks arrive.)
  let s = c.streams.getOrDefault(streamId)
  if s != nil and s.resp.body.len > 0:
    result = move(s.resp.body)
    s.resp.body = ""

proc respHeader*(c: H2Conn, streamId: uint32, name: string): string =
  ## A response header value once the HEADERS block has arrived (h2 field names
  ## are lowercase, so `name` must be lowercase). "" if absent.
  let s = c.streams.getOrDefault(streamId)
  if s != nil:
    for (k, v) in s.resp.headers:
      if k == name: return v

proc setSinkMode*(c: H2Conn, streamId: uint32) =
  ## Defer this stream's receive-window replenishment to `ackRecv`, so the stream
  ## window is held until a (possibly slow) sink has consumed the delivered bytes.
  ## Use for a streaming download that must apply backpressure to the peer.
  let s = c.streams.getOrDefault(streamId)
  if s != nil: s.sinkMode = true

proc ackRecv*(c: H2Conn, streamId: uint32, n: int): string =
  ## Acknowledge that the sink consumed `n` received body bytes, replenishing the
  ## STREAM receive window (batched, like the eager path). For a sinkMode stream
  ## whose DATA handler deferred the stream-level WINDOW_UPDATE, this releases it;
  ## holding it until now is what turns a slow consumer into peer backpressure.
  ## (The connection window is replenished in the DATA handler, so other streams
  ## are never starved.) Returns the WINDOW_UPDATE bytes to send, if any.
  let s = c.streams.getOrDefault(streamId)
  if s == nil: return
  s.recvPending += n
  if s.recvPending >= streamReplenish:
    result.add encodeWindowUpdate(streamId, uint32(s.recvPending))
    s.recvPending = 0

proc takeResponse*(c: H2Conn, streamId: uint32): H2Response =
  ## Return the stream's response and drop the stream.
  let s = c.streams.getOrDefault(streamId)
  if s != nil:
    result = s.resp
    c.streams.del(streamId)

proc canReuse*(c: H2Conn): bool = not c.goneAway and c.fatal.len == 0
