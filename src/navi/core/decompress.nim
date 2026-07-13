## Transparent response-body decompression via the system zlib (libz).
##
## We bind zlib's stable inflate ABI directly rather than depend on a package,
## since libz is present on every target platform. Only decompression is bound;
## navi decodes gzip/deflate response bodies, it does not compress requests.

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

proc decodeBody*(resp: var Response, opts: NaviOptions) =
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
  else:
    return
  resp.headers.del("content-encoding")
  resp.headers["content-length"] = $resp.body.len
