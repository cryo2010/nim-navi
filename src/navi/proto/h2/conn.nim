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
    hdrBuf: string
    hdrEndStream: bool

  H2Conn* = ref object
    enc: HpackEncoder
    dec: HpackDecoder            ## connection-wide (dynamic table is per-direction)
    frames: FrameDecoder
    nextId: uint32
    maxFrameSize: int
    streams: Table[uint32, Stream]
    goneAway*: bool
    goAwayLastId: uint32

proc initH2Conn*(): H2Conn =
  H2Conn(dec: initHpackDecoder(), nextId: 1, maxFrameSize: defaultMaxFrameSize,
         streams: initTable[uint32, Stream]())

proc preamble*(c: H2Conn): string =
  ## Connection preface, our SETTINGS (server push disabled), and a large
  ## connection-level WINDOW_UPDATE so downloads are not throttled.
  result = connectionPreface
  result.add encodeSettings({settingsEnablePush: 0'u32})
  result.add encodeWindowUpdate(0, 0x3fff0000'u32)

proc openStream*(c: H2Conn): uint32 =
  result = c.nextId
  c.nextId += 2
  c.streams[result] = Stream()

proc encodeRequest*(c: H2Conn, streamId: uint32, headers: openArray[HeaderPair],
                    body: string): string =
  ## `headers` must start with the pseudo-headers (:method, :scheme, :path,
  ## :authority) in order, followed by regular headers.
  let headerBlock = c.enc.encode(headers)
  let hasBody = body.len > 0
  result = encodeHeaders(streamId, headerBlock, endStream = not hasBody,
                         endHeaders = true)
  if hasBody:
    var i = 0
    while i < body.len:
      let n = min(c.maxFrameSize, body.len - i)
      result.add encodeData(streamId, body[i ..< i + n], endStream = i + n >= body.len)
      i += n

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
      outbuf.add encodeSettingsAck()
  of uint8(ftPing):
    if (f.flags and flagAck) == 0:
      outbuf.add encodePing(f.payload, ack = true)
  of uint8(ftGoAway):
    c.goneAway = true
    c.goAwayLastId = readU32(f.payload, 0) and 0x7fffffff'u32
  of uint8(ftHeaders), uint8(ftContinuation):
    let s = c.streams.getOrDefault(f.streamId)
    if s != nil:
      s.hdrBuf.add f.payload
      if f.typ == uint8(ftHeaders):
        s.hdrEndStream = (f.flags and flagEndStream) != 0
      if (f.flags and flagEndHeaders) != 0: c.applyHeaders(s)
  of uint8(ftData):
    let s = c.streams.getOrDefault(f.streamId)
    if s != nil:
      s.resp.body.add f.payload
      if f.payload.len > 0:
        outbuf.add encodeWindowUpdate(0, uint32(f.payload.len))
        outbuf.add encodeWindowUpdate(f.streamId, uint32(f.payload.len))
      if (f.flags and flagEndStream) != 0: s.ended = true
  of uint8(ftRstStream):
    let s = c.streams.getOrDefault(f.streamId)
    if s != nil:
      s.reset = true
      s.ended = true
  else:
    discard # PRIORITY, WINDOW_UPDATE, PUSH_PROMISE (push disabled): ignore

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

proc streamReset*(c: H2Conn, streamId: uint32): bool =
  let s = c.streams.getOrDefault(streamId)
  s != nil and s.reset

proc takeResponse*(c: H2Conn, streamId: uint32): H2Response =
  ## Return the stream's response and drop the stream.
  let s = c.streams.getOrDefault(streamId)
  if s != nil:
    result = s.resp
    c.streams.del(streamId)

proc canReuse*(c: H2Conn): bool = not c.goneAway
