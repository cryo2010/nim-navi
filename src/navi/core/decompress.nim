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
