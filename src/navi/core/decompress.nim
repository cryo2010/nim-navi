## Transparent response-body decompression.
##
## Binds the stable decode ABIs of the system codec libraries directly (no Nim
## package dependency): zlib for gzip/deflate, libbrotlidec for brotli, and
## libzstd for zstd. Only decompression is bound; navi decodes response bodies,
## it does not compress requests. zlib is present everywhere; brotli and zstd
## are loaded lazily, so `br`/`zstd` decoding requires libbrotlidec/libzstd at
## runtime (advertised in Accept-Encoding regardless).

import std/strutils
import ./headers, ./request, ./response

when defined(windows):
  const zlibDll = "zlib1.dll"
elif defined(macosx):
  const zlibDll = "libz.1.dylib"
else:
  const zlibDll = "libz.so.1"

type
  ZStream {.pure.} = object
    nextIn: ptr uint8
    availIn: cuint
    totalIn: culong
    nextOut: ptr uint8
    availOut: cuint
    totalOut: culong
    msg: cstring
    state: pointer
    zalloc: pointer
    zfree: pointer
    opaque: pointer
    dataType: cint
    adler: culong
    reserved: culong

const
  zNoFlush = cint(0)
  zOk = cint(0)
  zStreamEnd = cint(1)
  # windowBits: +32 auto-detects a gzip or zlib header; -15 is raw deflate.
  wbAuto = cint(15 + 32)
  wbRaw = cint(-15)

proc inflateInit2(strm: ptr ZStream, windowBits: cint, version: cstring,
                  streamSize: cint): cint
  {.cdecl, importc: "inflateInit2_", dynlib: zlibDll.}
proc inflate(strm: ptr ZStream, flush: cint): cint
  {.cdecl, importc: "inflate", dynlib: zlibDll.}
proc inflateEnd(strm: ptr ZStream): cint
  {.cdecl, importc: "inflateEnd", dynlib: zlibDll.}

proc inflateBytes(src: string, windowBits: cint): string =
  if src.len == 0: return ""
  var strm = ZStream()
  # zlib only checks the major version character, so "1" is sufficient.
  if inflateInit2(addr strm, windowBits, "1", cint(sizeof(ZStream))) != zOk:
    raise newException(ValueError, "navi: zlib inflateInit failed")
  defer: discard inflateEnd(addr strm)
  strm.nextIn = cast[ptr uint8](unsafeAddr src[0])
  strm.availIn = cuint(src.len)
  var chunk = newString(16384)
  while true:
    strm.nextOut = cast[ptr uint8](addr chunk[0])
    strm.availOut = cuint(chunk.len)
    let ret = inflate(addr strm, zNoFlush)
    if ret != zOk and ret != zStreamEnd:
      raise newException(ValueError, "navi: malformed compressed body")
    let produced = chunk.len - int(strm.availOut)
    if produced > 0: result.add chunk[0 ..< produced]
    if ret == zStreamEnd: break
    if strm.availIn == 0 and produced == 0: break  # truncated: stop, no progress

# --- brotli (libbrotlidec), streaming decode ---
when defined(windows):
  const brotliDll = "brotlidec.dll"
elif defined(macosx):
  const brotliDll = "libbrotlidec.1.dylib"
else:
  const brotliDll = "libbrotlidec.so.1"

type BrotliState = pointer

proc brotliCreate(a, b, c: pointer): BrotliState
  {.cdecl, importc: "BrotliDecoderCreateInstance", dynlib: brotliDll.}
proc brotliDestroy(s: BrotliState)
  {.cdecl, importc: "BrotliDecoderDestroyInstance", dynlib: brotliDll.}
proc brotliStream(s: BrotliState, availIn: var csize_t, nextIn: var ptr uint8,
                  availOut: var csize_t, nextOut: var ptr uint8,
                  totalOut: pointer): cint
  {.cdecl, importc: "BrotliDecoderDecompressStream", dynlib: brotliDll.}

const
  brSuccess = cint(1)
  brNeedOutput = cint(3)

proc decodeBrotli(src: string): string =
  if src.len == 0: return ""
  let s = brotliCreate(nil, nil, nil)
  if s == nil: raise newException(ValueError, "navi: brotli init failed")
  defer: brotliDestroy(s)
  var availIn = csize_t(src.len)
  var nextIn = cast[ptr uint8](unsafeAddr src[0])
  var chunk = newString(16384)
  while true:
    var availOut = csize_t(chunk.len)
    var nextOut = cast[ptr uint8](addr chunk[0])
    let r = brotliStream(s, availIn, nextIn, availOut, nextOut, nil)
    let produced = chunk.len - int(availOut)
    if produced > 0: result.add chunk[0 ..< produced]
    if r == brSuccess: break
    if r == brNeedOutput: continue        # buffer full, keep draining
    if r < brSuccess:                     # BROTLI_DECODER_RESULT_ERROR
      raise newException(ValueError, "navi: malformed brotli body")
    break                                 # NEEDS_MORE_INPUT with no more input: truncated

# --- zstd (libzstd), streaming decode ---
when defined(windows):
  const zstdDll = "libzstd.dll"
elif defined(macosx):
  const zstdDll = "libzstd.1.dylib"
else:
  const zstdDll = "libzstd.so.1"

type
  ZstdDStream = pointer
  ZstdBuffer {.pure.} = object       ## layout matches ZSTD_inBuffer / ZSTD_outBuffer
    buf: pointer
    size: csize_t
    pos: csize_t

proc zstdCreate(): ZstdDStream {.cdecl, importc: "ZSTD_createDStream", dynlib: zstdDll.}
proc zstdFree(s: ZstdDStream): csize_t {.cdecl, importc: "ZSTD_freeDStream", dynlib: zstdDll.}
proc zstdStream(s: ZstdDStream, output: var ZstdBuffer, input: var ZstdBuffer): csize_t
  {.cdecl, importc: "ZSTD_decompressStream", dynlib: zstdDll.}
proc zstdIsError(code: csize_t): cuint {.cdecl, importc: "ZSTD_isError", dynlib: zstdDll.}

proc decodeZstd(src: string): string =
  if src.len == 0: return ""
  let s = zstdCreate()
  if s == nil: raise newException(ValueError, "navi: zstd init failed")
  defer: discard zstdFree(s)
  var input = ZstdBuffer(buf: unsafeAddr src[0], size: csize_t(src.len), pos: 0)
  var chunk = newString(16384)
  while true:
    var output = ZstdBuffer(buf: addr chunk[0], size: csize_t(chunk.len), pos: 0)
    let r = zstdStream(s, output, input)
    if zstdIsError(r) != 0:
      raise newException(ValueError, "navi: malformed zstd body")
    if output.pos > 0: result.add chunk[0 ..< int(output.pos)]
    if r == 0 and input.pos >= input.size: break          # all frames decoded
    if output.pos == 0 and input.pos >= input.size: break  # truncated, no progress

# --- incremental (streaming) decoding ---
#
# A StreamDecoder keeps the codec state alive across chunks, so a response body
# is decoded as it arrives instead of only once fully buffered. The C resources
# are released by `=destroy` (there is no end-of-stream callback on a BodySink),
# so a truncated stream still frees cleanly.

type
  DecoderKind = enum dkZlib, dkBrotli, dkZstd
  StreamDecoderObj = object
    done: bool
    case kind: DecoderKind
    of dkZlib: zs: ZStream
    of dkBrotli: brs: BrotliState
    of dkZstd: zds: ZstdDStream
  StreamDecoder* = ref StreamDecoderObj

proc `=destroy`(d: var StreamDecoderObj) =
  case d.kind
  of dkZlib: discard inflateEnd(addr d.zs)
  of dkBrotli: (if d.brs != nil: brotliDestroy(d.brs))
  of dkZstd: (if d.zds != nil: discard zstdFree(d.zds))

proc newZlibDecoder(windowBits: cint): StreamDecoder =
  result = StreamDecoder(kind: dkZlib)
  if inflateInit2(addr result.zs, windowBits, "1", cint(sizeof(ZStream))) != zOk:
    raise newException(ValueError, "navi: zlib inflateInit failed")

proc updateZlib(d: StreamDecoder, input: openArray[byte]): string =
  if d.done or input.len == 0: return ""
  var inbuf = newString(input.len)            # stable pointer for the FFI call
  copyMem(addr inbuf[0], unsafeAddr input[0], input.len)
  d.zs.nextIn = cast[ptr uint8](addr inbuf[0])
  d.zs.availIn = cuint(inbuf.len)
  var chunk = newString(16384)
  while true:
    d.zs.nextOut = cast[ptr uint8](addr chunk[0])
    d.zs.availOut = cuint(chunk.len)
    let ret = inflate(addr d.zs, zNoFlush)
    if ret != zOk and ret != zStreamEnd:
      raise newException(ValueError, "navi: malformed compressed body")
    let produced = chunk.len - int(d.zs.availOut)
    if produced > 0: result.add chunk[0 ..< produced]
    if ret == zStreamEnd: d.done = true; break
    if d.zs.availIn == 0: break                # all of this chunk consumed

proc updateBrotli(d: StreamDecoder, input: openArray[byte]): string =
  if d.done or input.len == 0: return ""
  var inbuf = newString(input.len)
  copyMem(addr inbuf[0], unsafeAddr input[0], input.len)
  var availIn = csize_t(inbuf.len)
  var nextIn = cast[ptr uint8](addr inbuf[0])
  var chunk = newString(16384)
  while true:
    var availOut = csize_t(chunk.len)
    var nextOut = cast[ptr uint8](addr chunk[0])
    let r = brotliStream(d.brs, availIn, nextIn, availOut, nextOut, nil)
    let produced = chunk.len - int(availOut)
    if produced > 0: result.add chunk[0 ..< produced]
    if r == brSuccess: d.done = true; break
    if r == brNeedOutput: continue             # output full, keep draining
    if r < brSuccess: raise newException(ValueError, "navi: malformed brotli body")
    break                                       # needs more input: wait for the next chunk

proc updateZstd(d: StreamDecoder, input: openArray[byte]): string =
  if d.done or input.len == 0: return ""
  var inbuf = newString(input.len)
  copyMem(addr inbuf[0], unsafeAddr input[0], input.len)
  var inb = ZstdBuffer(buf: addr inbuf[0], size: csize_t(inbuf.len), pos: 0)
  var chunk = newString(16384)
  while inb.pos < inb.size:
    var outb = ZstdBuffer(buf: addr chunk[0], size: csize_t(chunk.len), pos: 0)
    let r = zstdStream(d.zds, outb, inb)
    if zstdIsError(r) != 0:
      raise newException(ValueError, "navi: malformed zstd body")
    if outb.pos > 0: result.add chunk[0 ..< int(outb.pos)]
    if r == 0: d.done = true; break            # a full frame completed
    if outb.pos == 0: break                     # no progress: needs more input

proc update*(d: StreamDecoder, input: openArray[byte]): string =
  ## Decode a chunk of compressed input into as much plaintext as it yields now.
  case d.kind
  of dkZlib: updateZlib(d, input)
  of dkBrotli: updateBrotli(d, input)
  of dkZstd: updateZstd(d, input)

proc newStreamDecoder*(encoding: string): StreamDecoder =
  ## A decoder for `encoding`, or nil for identity/unknown (pass bytes through).
  case encoding.strip.toLowerAscii
  of "gzip", "x-gzip", "deflate":
    # wbAuto detects gzip or zlib-wrapped deflate. Raw (headerless) deflate is
    # not auto-detectable mid-stream; that rare form is left to the buffered path.
    newZlibDecoder(wbAuto)
  of "br":
    let s = brotliCreate(nil, nil, nil)
    if s == nil: raise newException(ValueError, "navi: brotli init failed")
    StreamDecoder(kind: dkBrotli, brs: s)
  of "zstd":
    let s = zstdCreate()
    if s == nil: raise newException(ValueError, "navi: zstd init failed")
    StreamDecoder(kind: dkZstd, zds: s)
  else:
    nil

proc decodingSink*(encoding: string, inner: BodySink): BodySink =
  ## Wrap `inner` so streamed body chunks are decompressed per `encoding` before
  ## delivery. Returns `inner` unchanged for identity/unknown encodings.
  if inner == nil: return nil
  let dec = newStreamDecoder(encoding)
  if dec == nil: return inner
  result = proc(data: openArray[byte]) =
    let plain = dec.update(data)
    if plain.len > 0: inner(plain.toOpenArrayByte(0, plain.high))

proc decodeBody*(resp: var Response, opts: NaviOptionsBase) =
  ## Decompress the body in place per Content-Encoding, then drop the headers
  ## that described the encoded form. No-op when decompression is disabled or
  ## the encoding is identity/unknown.
  if not opts.wantsDecompress: return
  case resp.headers.get("content-encoding").strip.toLowerAscii
  of "gzip", "x-gzip":
    resp.body = inflateBytes(resp.body, wbAuto)
  of "deflate":
    # "deflate" is officially zlib-wrapped, but some servers send raw deflate.
    try: resp.body = inflateBytes(resp.body, wbAuto)
    except ValueError: resp.body = inflateBytes(resp.body, wbRaw)
  of "br":
    resp.body = decodeBrotli(resp.body)
  of "zstd":
    resp.body = decodeZstd(resp.body)
  else:
    return
  resp.headers.del("content-encoding")
  resp.headers["content-length"] = $resp.body.len
