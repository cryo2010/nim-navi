## Sans-io HTTP/2 client connection (RFC 9113).
##
## Drives one request/response exchange on a single stream. No I/O: the caller
## sends `clientPreamble()` and `encodeRequest(...)`, then feeds received bytes
## into `feed(...)` (which returns control bytes to send back) until `done`.
##
## This first version does one request per connection (no multiplexing or
## connection reuse) and assumes request headers and bodies fit within the peer's
## limits; it handles the full control-frame set needed to talk to real servers.

import std/strutils
import ./frame, ./hpack

type
  H2Response* = object
    status*: int
    headers*: seq[(string, string)]
    body*: string

  H2Client* = object
    enc: HpackEncoder
    dec: HpackDecoder
    frames: FrameDecoder
    streamId: uint32
    maxFrameSize: int
    resp: H2Response
    ended*: bool        ## our stream received END_STREAM
    failed*: bool
    failMsg*: string
    hdrBuf: string      ## accumulates a header block across CONTINUATION
    hdrEndStream: bool

proc initH2Client*(): H2Client =
  result.dec = initHpackDecoder()
  result.streamId = 1
  result.maxFrameSize = defaultMaxFrameSize

proc clientPreamble*(c: var H2Client): string =
  ## Connection preface, our SETTINGS (server push disabled), and a large
  ## connection-level WINDOW_UPDATE so downloads are not throttled.
  result = connectionPreface
  result.add encodeSettings({settingsEnablePush: 0'u32})
  result.add encodeWindowUpdate(0, 0x3fff0000'u32)

proc encodeRequest*(c: var H2Client, headers: openArray[HeaderPair],
                    body: string): string =
  ## `headers` must start with the pseudo-headers (:method, :scheme, :path,
  ## :authority) in order, followed by regular headers.
  let headerBlock = c.enc.encode(headers)
  let hasBody = body.len > 0
  result = encodeHeaders(c.streamId, headerBlock, endStream = not hasBody,
                         endHeaders = true)
  if hasBody:
    var i = 0
    while i < body.len:
      let n = min(c.maxFrameSize, body.len - i)
      result.add encodeData(c.streamId, body[i ..< i + n], endStream = i + n >= body.len)
      i += n

proc applyHeaderBlock(c: var H2Client) =
  for (name, value) in c.dec.decode(c.hdrBuf):
    if name == ":status":
      try: c.resp.status = parseInt(value)
      except ValueError: discard
    elif not name.startsWith(":"):
      c.resp.headers.add((name, value))
  c.hdrBuf.setLen(0)
  if c.hdrEndStream: c.ended = true

proc handle(c: var H2Client, f: Frame, outbuf: var string) =
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
    # Streams above lastStreamId were not processed; ours (1) usually was.
    let lastStreamId = readU32(f.payload, 0) and 0x7fffffff'u32
    if c.streamId > lastStreamId and not c.ended:
      c.failed = true
      c.failMsg = "server sent GOAWAY before handling the request"
  of uint8(ftHeaders):
    if f.streamId == c.streamId:
      c.hdrBuf.add f.payload
      c.hdrEndStream = (f.flags and flagEndStream) != 0
      if (f.flags and flagEndHeaders) != 0: c.applyHeaderBlock()
  of uint8(ftContinuation):
    if f.streamId == c.streamId:
      c.hdrBuf.add f.payload
      if (f.flags and flagEndHeaders) != 0: c.applyHeaderBlock()
  of uint8(ftData):
    if f.streamId == c.streamId:
      c.resp.body.add f.payload
      if f.payload.len > 0:
        outbuf.add encodeWindowUpdate(0, uint32(f.payload.len))
        outbuf.add encodeWindowUpdate(c.streamId, uint32(f.payload.len))
      if (f.flags and flagEndStream) != 0: c.ended = true
  of uint8(ftRstStream):
    if f.streamId == c.streamId:
      c.failed = true
      c.failMsg = "stream reset by server"
  else:
    discard # PRIORITY, WINDOW_UPDATE, PUSH_PROMISE (push disabled): ignore

proc feed*(c: var H2Client, data: string): string =
  ## Consume received bytes; return control bytes (ACKs, window updates) to send.
  c.frames.feed(data)
  var f: Frame
  while c.frames.next(f):
    c.handle(f, result)

proc done*(c: H2Client): bool = c.ended or c.failed

proc response*(c: H2Client): H2Response = c.resp
