## Minimal zlib gzip/deflate for the stress harness. navi only *decodes* response
## bodies, so to put compression on the wire in both directions the stress server
## encodes responses and decodes request bodies, and the native clients encode
## their request bodies. gzip and zlib-wrapped deflate are used -- both detected
## by the inflate auto-header, which is exactly navi's response-decode path.
##
## Native only (dynlib FFI); the js client stays plain, its codec being the
## runtime's. zlib (libz) ships everywhere navi's own decompress.nim needs it.

when defined(windows):
  const zlibDll = "zlib1.dll"
elif defined(macosx):
  const zlibDll = "libz.1.dylib"
else:
  const zlibDll = "libz.so.1"

type ZStream {.pure.} = object
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

proc deflateInit2(strm: ptr ZStream, level, meth, windowBits, memLevel,
                  strategy: cint, version: cstring, streamSize: cint): cint
  {.cdecl, dynlib: zlibDll, importc: "deflateInit2_".}
proc deflate(strm: ptr ZStream, flush: cint): cint
  {.cdecl, dynlib: zlibDll, importc.}
proc deflateEnd(strm: ptr ZStream): cint {.cdecl, dynlib: zlibDll, importc.}
proc inflateInit2(strm: ptr ZStream, windowBits: cint, version: cstring,
                  streamSize: cint): cint
  {.cdecl, dynlib: zlibDll, importc: "inflateInit2_".}
proc inflate(strm: ptr ZStream, flush: cint): cint
  {.cdecl, dynlib: zlibDll, importc.}
proc inflateEnd(strm: ptr ZStream): cint {.cdecl, dynlib: zlibDll, importc.}

const
  zFinish = cint(4)
  zNoFlush = cint(0)
  zOk = cint(0)
  zStreamEnd = cint(1)
  wbGzip = cint(15 + 16)   # gzip wrapper
  wbZlib = cint(15)        # zlib wrapper ("deflate")
  wbAuto = cint(15 + 32)   # inflate: auto-detect gzip or zlib

proc zcompress*(src, encoding: string): string {.raises: [].} =
  ## Compress `src` as "gzip" or "deflate" (zlib-wrapped).
  let wb = if encoding == "gzip": wbGzip else: wbZlib
  var strm = ZStream()
  doAssert deflateInit2(addr strm, 6, 8, wb, 8, 0, "1", cint(sizeof(ZStream))) == zOk
  defer: discard deflateEnd(addr strm)
  if src.len > 0:
    strm.nextIn = cast[ptr uint8](unsafeAddr src[0])
    strm.availIn = cuint(src.len)
  var chunk = newString(16384)
  while true:
    strm.nextOut = cast[ptr uint8](addr chunk[0])
    strm.availOut = cuint(chunk.len)
    let ret = deflate(addr strm, zFinish)
    let produced = chunk.len - int(strm.availOut)
    if produced > 0: result.add chunk[0 ..< produced]
    if ret == zStreamEnd: break
    doAssert ret == zOk, "deflate failed"

proc zdecompress*(src: string): string {.raises: [].} =
  ## Decompress a gzip or zlib ("deflate") stream (auto-detected).
  if src.len == 0: return ""
  var strm = ZStream()
  doAssert inflateInit2(addr strm, wbAuto, "1", cint(sizeof(ZStream))) == zOk
  defer: discard inflateEnd(addr strm)
  strm.nextIn = cast[ptr uint8](unsafeAddr src[0])
  strm.availIn = cuint(src.len)
  var chunk = newString(16384)
  while true:
    strm.nextOut = cast[ptr uint8](addr chunk[0])
    strm.availOut = cuint(chunk.len)
    let ret = inflate(addr strm, zNoFlush)
    let produced = chunk.len - int(strm.availOut)
    if produced > 0: result.add chunk[0 ..< produced]
    if ret == zStreamEnd: break
    if strm.availIn == 0 and produced == 0: break
