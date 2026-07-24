## Sans-io HTTP/2 framing (RFC 9113 section 4-6).
##
## Encodes frames to bytes and decodes them incrementally from a byte stream,
## with no I/O. The connection layer drives this over any transport.

import std/strutils

type
  FrameType* = enum
    ftData = 0x0
    ftHeaders = 0x1
    ftPriority = 0x2
    ftRstStream = 0x3
    ftSettings = 0x4
    ftPushPromise = 0x5
    ftPing = 0x6
    ftGoAway = 0x7
    ftWindowUpdate = 0x8
    ftContinuation = 0x9

  Frame* = object
    typ*: uint8        ## raw type; unknown types must be ignored, not enum-forced
    flags*: uint8
    streamId*: uint32
    payload*: string

  FrameDecoder* = object
    buf: string
    frameSizeError: bool   ## a peer frame declared a length over the max frame size

const
  # Frame flags (meaning depends on frame type).
  flagEndStream* = 0x01'u8
  flagAck* = 0x01'u8
  flagEndHeaders* = 0x04'u8
  flagPadded* = 0x08'u8
  flagPriority* = 0x20'u8

  # SETTINGS parameter identifiers.
  settingsHeaderTableSize* = 0x1'u16
  settingsEnablePush* = 0x2'u16
  settingsMaxConcurrentStreams* = 0x3'u16
  settingsInitialWindowSize* = 0x4'u16
  settingsMaxFrameSize* = 0x5'u16
  settingsMaxHeaderListSize* = 0x6'u16

  connectionPreface* = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
  defaultMaxFrameSize* = 16384

  # HTTP/2 error codes (RFC 9113 section 7), used in RST_STREAM / GOAWAY.
  errNoError* = 0x0'u32
  errProtocolError* = 0x1'u32
  errFlowControlError* = 0x3'u32
  errFrameSizeError* = 0x6'u32
  errRefusedStream* = 0x7'u32
  errCancel* = 0x8'u32
  errEnhanceYourCalm* = 0xb'u32

proc u24(n: int): string =
  result = newString(3)
  result[0] = char((n shr 16) and 0xff)
  result[1] = char((n shr 8) and 0xff)
  result[2] = char(n and 0xff)

proc u32(n: uint32): string =
  result = newString(4)
  result[0] = char((n shr 24) and 0xff)
  result[1] = char((n shr 16) and 0xff)
  result[2] = char((n shr 8) and 0xff)
  result[3] = char(n and 0xff)

proc readU24(s: openArray[char], i: int): int =
  (int(uint8(s[i])) shl 16) or (int(uint8(s[i + 1])) shl 8) or int(uint8(s[i + 2]))

proc readU32*(s: openArray[char], i: int): uint32 =
  (uint32(uint8(s[i])) shl 24) or (uint32(uint8(s[i + 1])) shl 16) or
  (uint32(uint8(s[i + 2])) shl 8) or uint32(uint8(s[i + 3]))

proc encodeFrame*(typ: uint8, flags: uint8, streamId: uint32, payload: string): string =
  ## Serialize one frame: 9-byte header (length, type, flags, stream id) + payload.
  result = u24(payload.len)
  result.add char(typ)
  result.add char(flags)
  result.add u32(streamId and 0x7fffffff'u32)
  result.add payload

proc encodeFrame*(typ: FrameType, flags: uint8, streamId: uint32, payload = ""): string =
  encodeFrame(uint8(typ), flags, streamId, payload)

proc feed*(d: var FrameDecoder, data: openArray[char]) =
  let start = d.buf.len
  d.buf.setLen(start + data.len)
  for i in 0 ..< data.len:
    d.buf[start + i] = data[i]

proc frameSizeError*(d: FrameDecoder): bool = d.frameSizeError
  ## A peer frame declared a length over `defaultMaxFrameSize` (FRAME_SIZE_ERROR).

proc next*(d: var FrameDecoder, frame: var Frame): bool =
  ## Pop the next complete frame, if one is fully buffered.
  if d.buf.len < 9: return false
  let length = readU24(d.buf, 0)
  if length > defaultMaxFrameSize:      # reject before buffering the oversized payload
    d.frameSizeError = true
    return false
  let total = 9 + length
  if d.buf.len < total: return false
  frame.typ = uint8(d.buf[3])
  frame.flags = uint8(d.buf[4])
  frame.streamId = readU32(d.buf, 5) and 0x7fffffff'u32
  frame.payload = d.buf[9 ..< total]
  d.buf.delete(0 ..< total)
  true

# --- Payload builders for the frames a client sends ---

proc encodeSettings*(params: openArray[(uint16, uint32)]): string =
  var payload = ""
  for (id, value) in params:
    payload.add char((id shr 8) and 0xff)
    payload.add char(id and 0xff)
    payload.add u32(value)
  encodeFrame(ftSettings, 0, 0, payload)

proc encodeSettingsAck*(): string =
  encodeFrame(ftSettings, flagAck, 0)

proc encodeWindowUpdate*(streamId: uint32, increment: uint32): string =
  encodeFrame(ftWindowUpdate, 0, streamId, u32(increment and 0x7fffffff'u32))

proc encodePing*(data: string, ack = false): string =
  ## `data` must be 8 bytes of opaque payload.
  encodeFrame(ftPing, if ack: flagAck else: 0, 0, data)

proc encodeRstStream*(streamId: uint32, errorCode: uint32): string =
  encodeFrame(ftRstStream, 0, streamId, u32(errorCode))

proc encodeGoAway*(lastStreamId: uint32, errorCode: uint32): string =
  encodeFrame(ftGoAway, 0, 0, u32(lastStreamId and 0x7fffffff'u32) & u32(errorCode))

proc encodeData*(streamId: uint32, data: string, endStream: bool): string =
  encodeFrame(ftData, if endStream: flagEndStream else: 0, streamId, data)

proc encodeHeaders*(streamId: uint32, headerBlock: string,
                    endStream, endHeaders: bool): string =
  var flags = 0'u8
  if endStream: flags = flags or flagEndStream
  if endHeaders: flags = flags or flagEndHeaders
  encodeFrame(ftHeaders, flags, streamId, headerBlock)

proc encodeContinuation*(streamId: uint32, headerBlock: string,
                         endHeaders: bool): string =
  encodeFrame(ftContinuation, if endHeaders: flagEndHeaders else: 0'u8,
              streamId, headerBlock)

proc parseSettings*(payload: string): seq[(uint16, uint32)] =
  ## Decode a SETTINGS payload into id/value pairs.
  var i = 0
  while i + 6 <= payload.len:
    let id = (uint16(uint8(payload[i])) shl 8) or uint16(uint8(payload[i + 1]))
    result.add((id, readU32(payload, i + 2)))
    i += 6
