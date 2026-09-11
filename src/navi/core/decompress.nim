## Transparent response-body decompression.
##
## Binds the stable decode ABIs of the system codec libraries directly (no Nim
## package dependency): zlib for gzip/deflate, libbrotlidec for brotli, and
## libzstd for zstd. Only decompression is bound; navi decodes response bodies,
## it does not compress requests. zlib is present everywhere; brotli and zstd
## are loaded lazily, so `br`/`zstd` decoding requires libbrotlidec/libzstd at
## runtime (advertised in Accept-Encoding regardless).

import std/[strutils, dynlib]
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
proc inflateReset(strm: ptr ZStream): cint
  {.cdecl, importc: "inflateReset", dynlib: zlibDll.}

proc checkDecompressLimit(produced, limit: int) =
  ## Abort a buffered decode the moment its output passes `maxResponseBytes`
  ## (0 = off), so a compression bomb is never fully materialized. The streaming
  ## path enforces the same cap per chunk in the engine.
  if limit > 0 and produced > limit:
    raise newException(ResponseTooLargeError,
      "navi: decompressed response exceeds maxResponseBytes (" & $limit & ")")

proc addBytes(dst: var string, src: string, n: int) {.inline.} =
  ## Append the first `n` bytes of `src` to `dst` in place, no intermediate slice.
  if n <= 0: return
  let old = dst.len
  dst.setLen(old + n)
  copyMem(addr dst[old], unsafeAddr src[0], n)

proc inflateBytes(src: string, windowBits: cint, limit: int): string =
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
    if produced > 0:
      result.addBytes(chunk, produced)
      checkDecompressLimit(result.len, limit)
    if ret == zStreamEnd:
      # A gzip body may be several concatenated members (RFC 1952); zlib returns
      # Z_STREAM_END at each boundary. If input remains, reset and decode the next
      # member instead of stopping at the first (curl/Go do the same). Trailing
      # bytes that are not a valid member then surface as a malformed body.
      if strm.availIn == 0: break
      if inflateReset(addr strm) != zOk:
        raise newException(ValueError, "navi: zlib inflateReset failed")
      continue
    if strm.availIn == 0 and produced == 0: break  # truncated: stop, no progress

# --- brotli (libbrotlidec), streaming decode ---
when defined(windows):
  const brotliDll = "brotlidec.dll"
elif defined(macosx):
  const brotliDll = "libbrotlidec.1.dylib"
else:
  const brotliDll = "libbrotlidec.so.1"

type BrotliState = pointer

# Loaded on first use (see loadBrotli), not at process startup, so a program
# that never receives a `br` body runs without libbrotlidec installed.
type
  BrotliCreateFn = proc(a, b, c: pointer): BrotliState {.cdecl, gcsafe, raises: [].}
  BrotliDestroyFn = proc(s: BrotliState) {.cdecl, gcsafe, raises: [].}
  BrotliStreamFn = proc(s: BrotliState, availIn: var csize_t, nextIn: var ptr uint8,
                        availOut: var csize_t, nextOut: var ptr uint8,
                        totalOut: pointer): cint {.cdecl, gcsafe, raises: [].}

var
  brotliCreate {.threadvar.}: BrotliCreateFn
  brotliDestroy {.threadvar.}: BrotliDestroyFn
  brotliStream {.threadvar.}: BrotliStreamFn

proc loadCodec(dll: string): LibHandle =
  ## Load `dll` by name, falling back to common absolute locations. On macOS the
  ## Homebrew lib dir is not on the loader's default search path, and `nim c -r`
  ## drops DYLD_* under SIP, so a bare-name load fails there; try those dirs
  ## directly (which also lets the lib resolve its own @rpath dependencies).
  ## Harmless on other platforms: the paths simply do not exist.
  result = loadLib(dll)
  when defined(macosx):
    if result == nil: result = loadLib("/opt/homebrew/lib/" & dll)
    if result == nil: result = loadLib("/usr/local/lib/" & dll)
  when defined(windows):
    # Windows builds disagree on the `lib` prefix: vcpkg ships brotlidec.dll and
    # zstd.dll, MSYS2/mingw ships libbrotlidec.dll and libzstd.dll. Try the other
    # spelling so either toolchain's DLLs are found under one build.
    if result == nil:
      let alt = if dll.startsWith("lib"): dll[3 .. ^1] else: "lib" & dll
      result = loadLib(alt)

proc loadBrotli() =
  ## Resolve libbrotlidec's decode symbols on first use. Raises a clear error if
  ## the library is not present (rather than crashing the whole process at start,
  ## which an eager `dynlib` pragma would).
  {.cast(gcsafe).}:      # per-thread fn pointers (threadvar): resolved once per thread
    if brotliCreate != nil: return
    let lib = loadCodec(brotliDll)
    if lib == nil:
      raise newException(ValueError, "navi: decoding a 'br' response needs " &
        brotliDll & " (install brotli), which could not be loaded")
    brotliCreate = cast[BrotliCreateFn](lib.symAddr("BrotliDecoderCreateInstance"))
    brotliDestroy = cast[BrotliDestroyFn](lib.symAddr("BrotliDecoderDestroyInstance"))
    brotliStream = cast[BrotliStreamFn](lib.symAddr("BrotliDecoderDecompressStream"))
    if brotliCreate == nil or brotliDestroy == nil or brotliStream == nil:
      raise newException(ValueError, "navi: " & brotliDll & " lacks expected symbols")

const
  brSuccess = cint(1)
  brNeedOutput = cint(3)

proc decodeBrotli(src: string, limit: int): string =
  if src.len == 0: return ""
  loadBrotli()
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
    if produced > 0:
      result.addBytes(chunk, produced)
      checkDecompressLimit(result.len, limit)
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

# Loaded on first use (see loadZstd), not at process startup.
type
  ZstdCreateFn = proc(): ZstdDStream {.cdecl, gcsafe, raises: [].}
  ZstdFreeFn = proc(s: ZstdDStream): csize_t {.cdecl, gcsafe, raises: [].}
  ZstdStreamFn = proc(s: ZstdDStream, output: var ZstdBuffer,
                      input: var ZstdBuffer): csize_t {.cdecl, gcsafe, raises: [].}
  ZstdIsErrorFn = proc(code: csize_t): cuint {.cdecl, gcsafe, raises: [].}

var
  zstdCreate {.threadvar.}: ZstdCreateFn
  zstdFree {.threadvar.}: ZstdFreeFn
  zstdStream {.threadvar.}: ZstdStreamFn
  zstdIsError {.threadvar.}: ZstdIsErrorFn

proc loadZstd() =
  ## Resolve libzstd's decode symbols on first use; a clear error if absent.
  {.cast(gcsafe).}:      # per-thread fn pointers (threadvar): resolved once per thread
    if zstdCreate != nil: return
    let lib = loadCodec(zstdDll)
    if lib == nil:
      raise newException(ValueError, "navi: decoding a 'zstd' response needs " &
        zstdDll & " (install zstd), which could not be loaded")
    zstdCreate = cast[ZstdCreateFn](lib.symAddr("ZSTD_createDStream"))
    zstdFree = cast[ZstdFreeFn](lib.symAddr("ZSTD_freeDStream"))
    zstdStream = cast[ZstdStreamFn](lib.symAddr("ZSTD_decompressStream"))
    zstdIsError = cast[ZstdIsErrorFn](lib.symAddr("ZSTD_isError"))
    if zstdCreate == nil or zstdFree == nil or zstdStream == nil or zstdIsError == nil:
      raise newException(ValueError, "navi: " & zstdDll & " lacks expected symbols")

proc decodeZstd(src: string, limit: int): string =
  if src.len == 0: return ""
  loadZstd()
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
    if output.pos > 0:
      result.addBytes(chunk, int(output.pos))
      checkDecompressLimit(result.len, limit)
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
    scratch: string          ## reused decode-output buffer (grown once, not per chunk)
    lastOut: int             ## size of the previous decode output, to pre-reserve the
                             ## next one and avoid growing it from empty every chunk
    case kind: DecoderKind
    of dkZlib: zs: ZStream
    of dkBrotli: brs: BrotliState
    of dkZstd: zds: ZstdDStream
  StreamDecoder* = ref StreamDecoderObj

const decodeScratchSize = 16384

proc `=destroy`(d: var StreamDecoderObj) =
  # A custom `=destroy` suppresses the compiler's field destruction, so the managed
  # `scratch` string must be freed explicitly or it leaks (one buffer per decoder).
  `=destroy`(d.scratch)
  case d.kind
  of dkZlib: discard inflateEnd(addr d.zs)
  of dkBrotli: (if d.brs != nil: brotliDestroy(d.brs))
  of dkZstd: (if d.zds != nil: discard zstdFree(d.zds))

proc newZlibDecoder(windowBits: cint): StreamDecoder =
  result = StreamDecoder(kind: dkZlib, scratch: newString(decodeScratchSize))
  if inflateInit2(addr result.zs, windowBits, "1", cint(sizeof(ZStream))) != zOk:
    raise newException(ValueError, "navi: zlib inflateInit failed")

proc updateZlib(d: StreamDecoder, input: openArray[byte]): string =
  if d.done or input.len == 0: return ""
  # `input` is a contiguous, GC-owned buffer that is stable for this synchronous
  # call, so point the FFI straight at it -- no throwaway copy.
  d.zs.nextIn = cast[ptr uint8](unsafeAddr input[0])
  d.zs.availIn = cuint(input.len)
  result = newStringOfCap(max(decodeScratchSize, d.lastOut))
  while true:
    d.zs.nextOut = cast[ptr uint8](addr d.scratch[0])
    d.zs.availOut = cuint(d.scratch.len)
    let ret = inflate(addr d.zs, zNoFlush)
    if ret != zOk and ret != zStreamEnd:
      raise newException(ValueError, "navi: malformed compressed body")
    result.addBytes(d.scratch, d.scratch.len - int(d.zs.availOut))
    if ret == zStreamEnd:
      # Multi-member gzip (RFC 1952): reset and decode any following member rather
      # than latching `done`. A member can end exactly on a chunk boundary
      # (availIn == 0); the next member (if any) then arrives in a later chunk and
      # the reset state decodes it, instead of being silently truncated.
      if inflateReset(addr d.zs) != zOk:
        raise newException(ValueError, "navi: zlib inflateReset failed")
      if d.zs.availIn == 0: break
      continue
    if d.zs.availIn == 0: break                # all of this chunk consumed
  d.lastOut = result.len

proc updateBrotli(d: StreamDecoder, input: openArray[byte]): string =
  if d.done or input.len == 0: return ""
  var availIn = csize_t(input.len)
  var nextIn = cast[ptr uint8](unsafeAddr input[0])
  result = newStringOfCap(max(decodeScratchSize, d.lastOut))
  while true:
    var availOut = csize_t(d.scratch.len)
    var nextOut = cast[ptr uint8](addr d.scratch[0])
    let r = brotliStream(d.brs, availIn, nextIn, availOut, nextOut, nil)
    result.addBytes(d.scratch, d.scratch.len - int(availOut))
    if r == brSuccess: d.done = true; break
    if r == brNeedOutput: continue             # output full, keep draining
    if r < brSuccess: raise newException(ValueError, "navi: malformed brotli body")
    break                                       # needs more input: wait for the next chunk
  d.lastOut = result.len

proc updateZstd(d: StreamDecoder, input: openArray[byte]): string =
  if d.done or input.len == 0: return ""
  var inb = ZstdBuffer(buf: cast[typeof(ZstdBuffer().buf)](unsafeAddr input[0]),
                       size: csize_t(input.len), pos: 0)
  result = newStringOfCap(max(decodeScratchSize, d.lastOut))
  while inb.pos < inb.size:
    var outb = ZstdBuffer(buf: cast[typeof(ZstdBuffer().buf)](addr d.scratch[0]),
                          size: csize_t(d.scratch.len), pos: 0)
    let r = zstdStream(d.zds, outb, inb)
    if zstdIsError(r) != 0:
      raise newException(ValueError, "navi: malformed zstd body")
    result.addBytes(d.scratch, int(outb.pos))
    if r == 0: d.done = true; break            # a full frame completed
    if outb.pos == 0: break                     # no progress: needs more input
  d.lastOut = result.len

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
    loadBrotli()                       # resolve the lazily-bound symbols first
    let s = brotliCreate(nil, nil, nil)
    if s == nil: raise newException(ValueError, "navi: brotli init failed")
    StreamDecoder(kind: dkBrotli, brs: s, scratch: newString(decodeScratchSize))
  of "zstd":
    loadZstd()
    let s = zstdCreate()
    if s == nil: raise newException(ValueError, "navi: zstd init failed")
    StreamDecoder(kind: dkZstd, zds: s, scratch: newString(decodeScratchSize))
  else:
    nil

type CappedDecoder* = object
  ## The decode-and-size-cap dance the engine repeats at every streamed-body read:
  ## pick the decoder once the content-encoding is known, decode each raw chunk,
  ## and abort when the decoded total passes the cap. Collapsing it into one type
  ## keeps the several body-read sites from drifting.
  dec: StreamDecoder
  ready: bool            ## the decoder has been chosen (once the headers are in)
  seen: int             ## decoded bytes so far, for the cap
  cap: int              ## max decoded bytes; 0 disables
  decompress: bool      ## whether to build a decoder at all

proc initCappedDecoder*(decompress: bool, cap: int): CappedDecoder =
  CappedDecoder(decompress: decompress, cap: cap)

proc encodingResolved*(cd: CappedDecoder): bool = cd.ready
  ## Whether the decoder has already been chosen (on the first non-empty chunk).
  ## Once true the content-encoding has been read, so callers can skip the per-chunk
  ## header lookup they would otherwise pass to `feed` for the whole download.

proc feed*(cd: var CappedDecoder, raw: string, encoding: string): string =
  ## Decode one raw body chunk. On the first non-empty chunk the decoder is built
  ## from `encoding` (read only then). Returns the decoded bytes, or "" when the
  ## input was empty or the decoder buffered it without output yet. Raises
  ## ResponseTooLargeError once the decoded total passes the cap.
  if raw.len == 0: return ""
  if not cd.ready:
    cd.dec = if cd.decompress: newStreamDecoder(encoding) else: nil
    cd.ready = true
  let decoded =
    if cd.dec != nil: cd.dec.update(raw.toOpenArrayByte(0, raw.high)) else: raw
  if decoded.len == 0: return ""
  cd.seen += decoded.len
  if cd.cap > 0 and cd.seen > cd.cap:
    raise newException(ResponseTooLargeError,
      "navi: response exceeded maxResponseBytes")
  decoded

proc decodeBody*(resp: var Response, opts: NaviConfigBase) =
  ## Decompress the body in place per Content-Encoding, then drop the headers that
  ## described the encoded form. Handles a stacked encoding (e.g. `gzip, br`) by
  ## decoding in reverse of the applied order (RFC 9110 8.4). No-op when
  ## decompression is disabled, the encoding is identity, or any layer is one we
  ## cannot decode (the body is then left untouched, header intact).
  if not opts.wantsDecompress: return
  let ce = resp.headers.get("content-encoding")
  if ce.len == 0: return
  var encodings: seq[string]
  for part in ce.split(','):
    let e = part.strip.toLowerAscii
    if e.len == 0 or e == "identity": continue
    if e notin ["gzip", "x-gzip", "deflate", "br", "zstd"]:
      return   # an encoding we cannot decode: hand back the body as received
    encodings.add e
  if encodings.len == 0: return
  # The header lists encodings in the order they were applied, so undo them from
  # the last one back to the first. `limit` (maxResponseBytes) is enforced inside
  # each decoder so a compression bomb is aborted mid-inflate, not after the whole
  # body is materialized.
  let limit = opts.maxResponseBytes
  for i in countdown(encodings.high, 0):
    case encodings[i]
    of "gzip", "x-gzip":
      resp.body = inflateBytes(resp.body, wbAuto, limit)
    of "deflate":
      # "deflate" is officially zlib-wrapped, but some servers send raw deflate.
      try: resp.body = inflateBytes(resp.body, wbAuto, limit)
      except ValueError: resp.body = inflateBytes(resp.body, wbRaw, limit)
    of "br":
      resp.body = decodeBrotli(resp.body, limit)
    of "zstd":
      resp.body = decodeZstd(resp.body, limit)
    else: discard
  resp.headers.del("content-encoding")
  resp.headers["content-length"] = $resp.body.len
