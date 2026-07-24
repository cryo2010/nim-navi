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
    recvPending: int      ## received bytes not yet acked with a WINDOW_UPDATE
    sendBuf: string       ## request body not yet on the wire (flow-control bound)
    sendOff: int          ## bytes of sendBuf already sent
    sendWindow: int       ## per-stream send window (peer's INITIAL_WINDOW_SIZE)

  H2Conn* = ref object
    enc: HpackEncoder
    dec: HpackDecoder            ## connection-wide (dynamic table is per-direction)
    frames: FrameDecoder
    nextId: uint32
    maxFrameSize: int
    maxBodyBytes: int            ## cap on a response body; 0 disables (maxResponseBytes)
    streams: Table[uint32, Stream]
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
                             settingsInitialWindowSize: uint32(recvWindowSize)})
  result.add encodeWindowUpdate(0, 0x3fff0000'u32)

proc openStream*(c: H2Conn): uint32 =
  result = c.nextId
  c.nextId += 2
  c.streams[result] = Stream(sendWindow: c.peerInitialWindow)

proc flushSend(c: H2Conn, streamId: uint32, s: Stream, outbuf: var string) =
  ## Emit as many DATA frames as the stream and connection send windows allow,
  ## setting END_STREAM on the frame that drains the body.
  while s.sendOff < s.sendBuf.len:
    let avail = min(s.sendWindow, c.connSendWindow)
    if avail <= 0: break                       # windowed out; wait for a WINDOW_UPDATE
    let n = min(min(avail, c.maxFrameSize), s.sendBuf.len - s.sendOff)
    outbuf.add encodeData(streamId, s.sendBuf[s.sendOff ..< s.sendOff + n],
                          endStream = s.sendOff + n >= s.sendBuf.len)
    s.sendOff += n
    s.sendWindow -= n
    c.connSendWindow -= n

proc encodeRequest*(c: H2Conn, streamId: uint32, headers: openArray[HeaderPair],
                    body: string): string =
  ## `headers` must start with the pseudo-headers (:method, :scheme, :path,
  ## :authority) in order, followed by regular headers. Sends the header block
  ## and as much of the body as the send window allows now; the rest is released
  ## by `feed` as the peer sends WINDOW_UPDATE frames.
  let headerBlock = c.enc.encode(headers)
  let hasBody = body.len > 0
  # Split a header block larger than the peer's max frame size across a HEADERS
  # frame and one or more CONTINUATION frames (RFC 9113 6.2/6.10); a single
  # oversized HEADERS frame would be a FRAME_SIZE_ERROR.
  let mfs = c.maxFrameSize
  if headerBlock.len <= mfs:
    result = encodeHeaders(streamId, headerBlock, endStream = not hasBody,
                           endHeaders = true)
  else:
    result = encodeHeaders(streamId, headerBlock[0 ..< mfs],
                           endStream = not hasBody, endHeaders = false)
    var i = mfs
    while i < headerBlock.len:
      let n = min(mfs, headerBlock.len - i)
      result.add encodeContinuation(streamId, headerBlock[i ..< i + n],
                                    endHeaders = i + n >= headerBlock.len)
      i += n
  if hasBody:
    let s = c.streams[streamId]
    s.sendBuf = body
    c.flushSend(streamId, s, result)

proc replenishRecv(c: H2Conn, sid: uint32, s: Stream, n: int, outbuf: var string) =
  ## Give back receive-window credit for `n` consumed bytes, batched: emit a
  ## WINDOW_UPDATE only once the unacked total crosses the threshold, so a large
  ## download costs a handful of control frames instead of one per DATA frame.
  s.recvPending += n
  if s.recvPending >= streamReplenish:
    outbuf.add encodeWindowUpdate(sid, uint32(s.recvPending))
    s.recvPending = 0
  c.connRecvPending += n
  if c.connRecvPending >= connReplenish:
    outbuf.add encodeWindowUpdate(0, uint32(c.connRecvPending))
    c.connRecvPending = 0

proc applyHeaders(c: H2Conn, s: Stream) =
  for (name, value) in c.dec.decode(s.hdrBuf):
    if name == ":status":
      try: s.resp.status = parseInt(value)
      except ValueError: discard
    elif not name.startsWith(":"):
      s.resp.headers.add((name, value))
  s.hdrBuf.setLen(0)
  if s.hdrEndStream: s.ended = true

proc handle(c: H2Conn, f: Frame, outbuf: var string) =
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
      s.hdrBuf.add f.payload
      if s.hdrBuf.len > maxHeaderListBytes:       # CONTINUATION flood: bound and RST
        outbuf.add encodeRstStream(f.streamId, errEnhanceYourCalm)
        s.reset = true; s.ended = true; s.hdrBuf.setLen(0)
      else:
        if f.typ == uint8(ftHeaders):
          s.hdrEndStream = (f.flags and flagEndStream) != 0
        if (f.flags and flagEndHeaders) != 0: c.applyHeaders(s)
  of uint8(ftData):
    let s = c.streams.getOrDefault(f.streamId)
    if s != nil and not s.reset:
      s.resp.body.add f.payload
      if c.maxBodyBytes > 0 and s.resp.body.len > c.maxBodyBytes:  # over the size cap: RST
        outbuf.add encodeRstStream(f.streamId, errCancel)
        s.reset = true; s.ended = true; s.tooLarge = true
      else:
        if f.payload.len > 0:
          c.replenishRecv(f.streamId, s, f.payload.len, outbuf)
        if (f.flags and flagEndStream) != 0: s.ended = true
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
  else:
    discard # PRIORITY, PUSH_PROMISE (push disabled): ignore

proc feed*(c: H2Conn, data: string): string =
  ## Consume received bytes; return control bytes (ACKs, window updates) to send.
  c.frames.feed(data)
  var f: Frame
  while c.frames.next(f):
    c.handle(f, result)

proc streamDone*(c: H2Conn, streamId: uint32): bool =
  ## True when the stream has ended (or been reset), or the connection is gone.
  if c.goneAway: return true
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

proc takeResponse*(c: H2Conn, streamId: uint32): H2Response =
  ## Return the stream's response and drop the stream.
  let s = c.streams.getOrDefault(streamId)
  if s != nil:
    result = s.resp
    c.streams.del(streamId)

proc canReuse*(c: H2Conn): bool = not c.goneAway
