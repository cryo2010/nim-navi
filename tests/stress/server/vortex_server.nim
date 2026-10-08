## The `NAVI_SERVER=vortex` origin for the navi stress workloads: one vortex
## process serving h1 + h2 over TLS on a TCP port and, with `NAVI_HTTP3=1`, h3
## over QUIC on the same UDP port, with its own `Alt-Svc` advertisement. The
## alternative to `server/app.py` + hypercorn + Caddy + aioquic, chosen with
## `NAVI_SERVER=vortex` (see tests/stress/README.md, "Choosing the server").
##
## `server/app.py` is the SPEC, not this file: every route, status code, header
## and canonicalisation rule below exists because a navi client asserts it, so
## read app.py beside this when changing either. The routes:
##
##   ANY /echo             echoes the method (`x-echo-method`) and the `x-stress`
##                         middleware header (`x-echo-stress`), decodes a
##                         `Content-Encoding` request body, canonicalises JSON
##                         (sorted keys, compact) and urlencoded form bodies, and
##                         re-encodes the reply per `x-want-encoding`.
##   GET/CONNECT /ws       WebSocket echo (text + binary, frame kind preserved)
##                         over an h1 Upgrade AND over h2/h3 Extended CONNECT
##                         (RFC 8441 / RFC 9220) -- the reason vortex needs no
##                         aioquic sidecar for the h3 ws cells.
##   GET /events           SSE: `id: N` / `data: event-N` forever, dropping the
##                         connection every `NAVI_SSE_DROP_EVERY` events so the
##                         client's reconnect + `Last-Event-ID` resume is
##                         exercised; resumes at `Last-Event-ID + 1`.
##   POST /upload          streaming SHA-1 of the body in constant memory, reply
##                         `{"sha1": "<lowercase hex>", "size": N}`.
##   GET /download?size=N  `size` bytes (default 1 MiB) of one fixed 1 MiB block
##                         repeated, block `k` stamped with `k` as a big-endian
##                         uint64 in its first 8 bytes, the whole stream's SHA-1
##                         in `x-sha1`. Constant memory, with backpressure.
##   coverage routes       `/status/{code}`, `/redirect/{n}`, `/needs-auth`,
##                         `/setcookie`, `/needs-cookie` -- what `featureChecks`
##                         in clients/parts/requests_part.nim asserts once per
##                         cell (redirect following, error statuses, the cookie
##                         jar, Basic auth).
##
## Two deliberate departures from vortex's own `conformance/stress/stress_server.nim`
## (which is a reference for the vortex API here, not a drop-in: its route
## contract is the vortex Python client's):
##
##  - **The codecs are zlib/brotli/zstd directly, not vortex's.** `compress` and
##    `decompressRequest` are both off and `/echo` does both directions itself.
##    navi asks for a specific codec with `x-want-encoding` and the payload
##    catalogue expects exactly that codec back, which `Accept-Encoding`
##    negotiation cannot promise; vortex also has no raw-`deflate` response
##    encoder, which `NAVI_RESP_COMPRESSION=deflate` needs. Going straight to the
##    C libraries is also what app.py does (Python's `gzip`/`zlib`/`brotli`/
##    `zstandard` are these same libraries), so the two servers put the same
##    bytes on the wire and a compression cell compares like with like.
##  - **The download block is a deterministic LCG, not `os.urandom`.** Equally
##    incompressible, but identical across restarts, so a checksum failure can be
##    reproduced against a fresh server.
##
## Build (see tests/stress/Dockerfile.h3; one compile at image build time, never
## per cell) with `--mm:orc --threads:on` and vortex's `soak` profile flags, so a
## server crash prints where it died instead of reaching navi as an unexplained
## ConnectError. `-d:naviVortexAsync` / `-d:naviVortexChronos` select the handler
## runtime (`NAVI_VORTEX_RUNTIME`); the default build is the synchronous one.

import std/[os, strutils, json, algorithm, uri, base64, tables]
import vortex
import nimcrypto/[sha, hash]          # incremental SHA-1 (vortex's own core dep)

when defined(naviVortexAsync):
  import vortex/asyncdispatch
elif defined(naviVortexChronos):
  import vortex/chronos

const asyncMode = defined(naviVortexAsync) or defined(naviVortexChronos)

# --- codecs (zlib / brotli / zstd by dynlib; see the header note) ------------
# Loaded by soname rather than linked, so the server needs only the runtime
# libraries in the image and no --passL. Mirrors tests/stress/zlibcodec.nim,
# which the native clients use for the request side of the same exchange.

const
  zlibDll = "libz.so(.1|)"
  brotliEncDll = "libbrotlienc.so(.1|)"
  zstdDll = "libzstd.so(.1|)"

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

proc brotliEncoderMaxCompressedSize(inputSize: csize_t): csize_t
  {.cdecl, dynlib: brotliEncDll, importc: "BrotliEncoderMaxCompressedSize".}
proc brotliEncoderCompress(quality, lgwin, mode: cint, inputSize: csize_t,
                           inputBuffer: ptr uint8, encodedSize: ptr csize_t,
                           encodedBuffer: ptr uint8): cint
  {.cdecl, dynlib: brotliEncDll, importc: "BrotliEncoderCompress".}

proc zstdCompressBound(srcSize: csize_t): csize_t
  {.cdecl, dynlib: zstdDll, importc: "ZSTD_compressBound".}
proc zstdCompress(dst: ptr uint8, dstCapacity: csize_t, src: ptr uint8,
                  srcSize: csize_t, level: cint): csize_t
  {.cdecl, dynlib: zstdDll, importc: "ZSTD_compress".}
proc zstdIsError(code: csize_t): cuint
  {.cdecl, dynlib: zstdDll, importc: "ZSTD_isError".}

const
  zFinish = cint(4)
  zNoFlush = cint(0)
  zOk = cint(0)
  zStreamEnd = cint(1)
  wbGzip = cint(15 + 16)   # gzip wrapper
  wbZlib = cint(15)        # zlib wrapper, i.e. HTTP "deflate" as app.py sends it
  wbAuto = cint(15 + 32)   # inflate: auto-detect gzip or zlib

proc zcompress(src: string, gzipWrapper: bool): string {.gcsafe.} =
  ## gzip- or zlib-wrapped deflate of `src`, at the level Python's matching call
  ## uses: `gzip.compress(data)` defaults to `compresslevel=9`, while
  ## `zlib.compress(data)` defaults to `Z_DEFAULT_COMPRESSION`, i.e. 6. Matching
  ## each is what puts the same bytes on the wire as app.py.
  let wb = if gzipWrapper: wbGzip else: wbZlib
  let level = if gzipWrapper: cint(9) else: cint(6)
  var strm = ZStream()
  if deflateInit2(addr strm, level, 8, wb, 8, 0, "1",
                  cint(sizeof(ZStream))) != zOk:
    return src
  defer: discard deflateEnd(addr strm)
  if src.len > 0:
    strm.nextIn = cast[ptr uint8](unsafeAddr src[0])
    strm.availIn = cuint(src.len)
  var chunk = newString(65536)
  while true:
    strm.nextOut = cast[ptr uint8](addr chunk[0])
    strm.availOut = cuint(chunk.len)
    let ret = deflate(addr strm, zFinish)
    let produced = chunk.len - int(strm.availOut)
    if produced > 0: result.add chunk[0 ..< produced]
    if ret == zStreamEnd: break
    if ret != zOk: return src           # give up: send the body uncompressed

proc zdecompress(src: string, ok: var bool): string {.gcsafe.} =
  ## Inflate a gzip or zlib ("deflate") stream, header auto-detected -- the same
  ## pair `zcompress` above and the clients' zlibcodec produce. `ok` comes back
  ## false on a stream zlib refuses or one that ends before `Z_STREAM_END`, and
  ## the caller must then fail the request instead of using the partial output.
  ##
  ## Failing closed is not tidiness. On `Z_DATA_ERROR` (a body that is not a
  ## gzip/zlib stream at all) zlib consumes nothing and produces nothing, so a
  ## loop whose only exit was "input exhausted and nothing produced" spins the
  ## loop thread forever -- one `curl -k -X POST https://127.0.0.1:9443/echo
  ## -H 'content-encoding: gzip' --data-binary notgzip` would wedge every
  ## connection that thread owns.
  ok = true
  if src.len == 0: return ""
  var strm = ZStream()
  if inflateInit2(addr strm, wbAuto, "1", cint(sizeof(ZStream))) != zOk:
    ok = false
    return ""
  defer: discard inflateEnd(addr strm)
  strm.nextIn = cast[ptr uint8](unsafeAddr src[0])
  strm.availIn = cuint(src.len)
  var chunk = newString(65536)
  while true:
    strm.nextOut = cast[ptr uint8](addr chunk[0])
    strm.availOut = cuint(chunk.len)
    let ret = inflate(addr strm, zNoFlush)
    let produced = chunk.len - int(strm.availOut)
    if produced > 0: result.add chunk[0 ..< produced]
    if ret == zStreamEnd: return                 # the whole stream, decoded
    if ret != zOk:
      # `ret < zOk` is a hard zlib error (Z_DATA_ERROR, Z_BUF_ERROR,
      # Z_MEM_ERROR, Z_STREAM_ERROR); the one positive non-end code,
      # Z_NEED_DICT, asks for a preset dictionary nobody here has. All of them
      # leave the stream stuck, so this is the branch that must not be missing.
      ok = false
      result = ""
      return
    if strm.availIn == 0 and produced == 0:
      ok = false                                 # truncated: no Z_STREAM_END
      result = ""
      return

proc brcompress(src: string): string {.gcsafe.} =
  ## Brotli, quality 11 / lgwin 22 (the library defaults Python's `brotli`
  ## module also uses), one shot.
  let cap = brotliEncoderMaxCompressedSize(csize_t(max(src.len, 1)))
  var buf = newString(int(cap))
  var outLen = cap
  let inPtr =
    if src.len > 0: cast[ptr uint8](unsafeAddr src[0]) else: nil
  if brotliEncoderCompress(11, 22, 0, csize_t(src.len), inPtr,
                           addr outLen, cast[ptr uint8](addr buf[0])) == 0:
    return src                          # encoder refused: send it uncompressed
  buf.setLen(int(outLen))
  buf

proc zstdcompress(src: string): string {.gcsafe.} =
  ## zstd at level 3, the default `zstandard.ZstdCompressor()` uses.
  let cap = zstdCompressBound(csize_t(max(src.len, 1)))
  var buf = newString(int(cap))
  let inPtr =
    if src.len > 0: cast[ptr uint8](unsafeAddr src[0]) else: nil
  let n = zstdCompress(cast[ptr uint8](addr buf[0]), cap, inPtr,
                       csize_t(src.len), 3)
  if zstdIsError(n) != 0: return src
  buf.setLen(int(n))
  buf

proc encodeBody(src, how: string): string {.gcsafe.} =
  ## app.py's `encode`: the codec `x-want-encoding` asked for, or the body
  ## unchanged for an unknown one (which then carries no `Content-Encoding`).
  ## Every encoder fails closed by returning `src`, which `echoReply` detects
  ## (the reply then carries no `Content-Encoding`) exactly as app.py's
  ## `if want and out is not body` does.
  case how
  of "gzip": zcompress(src, gzipWrapper = true)
  of "deflate": zcompress(src, gzipWrapper = false)
  of "br": brcompress(src)
  of "zstd": zstdcompress(src)
  else: src

proc decodeBody(src, how: string, ok: var bool): string {.gcsafe.} =
  ## app.py's `decode`: gzip and zlib-wrapped deflate are the only request
  ## encodings navi's clients send (NAVI_REQ_COMPRESSION); anything else is
  ## passed through untouched, as app.py does -- including `br` and `zstd`,
  ## which app.py's `decode` does not handle either, so there is no brotli or
  ## zstd decoder here to fail open. `ok` is false only when a gzip/deflate body
  ## would not inflate.
  ok = true
  case how.strip().toLowerAscii
  of "", "identity": src
  of "gzip", "deflate", "x-gzip": zdecompress(src, ok)
  else: src

# --- knobs (read once at startup; ints/strings are read-only after start) ----

let
  srvPort = parseInt(getEnv("NAVI_SERVER_PORT", "9443"))
  srvCert = getEnv("NAVI_CERT", "")
  srvKey = getEnv("NAVI_KEY", "")
  srvHttp3 = getEnv("NAVI_HTTP3", "0") == "1"
  sseDropEvery = parseInt(getEnv("NAVI_SSE_DROP_EVERY", "1000"))
  streamBytes = parseInt(getEnv("NAVI_STREAM_BYTES", "1073741824"))
  # One loop thread per instance by default, matching the one hypercorn worker
  # per instance the hypercorn branch starts: run.sh already fans out over
  # NAVI_SERVER_COUNT processes, and vortex's own default (countProcessors())
  # would put N x cores loop threads on the box and make a throughput
  # comparison against hypercorn meaningless.
  srvThreads = parseInt(getEnv("NAVI_VORTEX_THREADS", "1"))
  # vortex's 10 s default measures from accept and counts the TLS handshake and
  # any protocol upgrade. These cells run deliberately oversubscribed, where a
  # loop thread can be descheduled for longer than that and a slow WebSocket
  # upgrade is reset by the deadline rather than by any defect. Same 60 s
  # reasoning (and value) as vortex's own STRESS_HEADER_TIMEOUT.
  srvHeaderTimeout = parseInt(getEnv("NAVI_VORTEX_HEADER_TIMEOUT", "60"))
  # Which workload this cell is running (run.sh gets it as NAVI_WORKLOAD from
  # `nimble stress<Workload>` and the server inherits it). Only used to decide
  # whether the big download digest is worth prewarming; see `dlShaWarm`.
  srvWorkload = getEnv("NAVI_WORKLOAD", "requests")

const
  dlBlockSize = 1 shl 20              # 1 MiB, as app.py's BLOCK
  dlChunk = 64 * 1024                 # bytes per res.write (see pumpDownload)

proc buildBlock(): string =
  ## One fixed, incompressible 1 MiB block (LCG; see the header note on why this
  ## is deterministic where app.py uses os.urandom).
  result = newString(dlBlockSize)
  var x = 0x9e3779b9'u32
  for i in 0 ..< dlBlockSize:
    x = x * 1664525'u32 + 1013904223'u32
    result[i] = char(x shr 24)

let dlBlock = buildBlock()

var dlScratch {.threadvar.}: string
  ## Per-loop-thread copy of `dlBlock`, re-stamped immediately before each
  ## write: constant memory regardless of how many downloads a thread is
  ## serving, and safe to share between them because nothing yields between the
  ## stamp and the write (vortex's write paths copy the bytes out).

proc stampScratch(idx: int) {.gcsafe.} =
  ## Prepare `dlScratch` as block `idx`: the shared block with its first 8 bytes
  ## replaced by `idx` as a big-endian uint64. Index-stamping makes a
  ## whole-block reorder or duplication on the wire change the stream's SHA-1
  ## (every block would otherwise hash alike). Callers then write
  ## `dlScratch.toOpenArray(0, n - 1)`, truncating the final partial block.
  if dlScratch.len != dlBlockSize:
    {.cast(gcsafe).}: dlScratch = dlBlock
  var v = uint64(idx)
  for k in countdown(7, 0):
    dlScratch[k] = char(v and 0xff'u64)
    v = v shr 8

proc writeSlice(res: Response, offset, n: int): bool {.gcsafe, discardable.} =
  ## Write `n` download bytes starting at global offset `offset`, returning
  ## vortex's writable flag (false = the send backlog is full, wait for the
  ## drain). `dlChunk` divides `dlBlockSize`, so a slice never straddles a block
  ## boundary and one stamp covers it.
  ##
  ## 64 KiB per write, not a whole 1 MiB block: that is exactly vortex's
  ## `respHighWater` (`src/vortex/connection.nim`), so a 1 MiB write parks the
  ## producer with ~1 MiB still queued on the stream, and vortex's h2
  ## producer-resume scan only re-arms a stream whose own pending body has
  ## fallen back under `respHighWater`. Measured with curl against this server:
  ##
  ##   curl -k --http2 -o /dev/null 'https://127.0.0.1:9443/download?size=4194304'
  ##
  ## stalls for good after ~2 MiB when the handler writes 1 MiB at a time, and
  ## completes when it writes 64 KiB. 64 KiB is also the chunk vortex's own
  ## streaming download uses. The root cause is open upstream:
  ## https://github.com/cryo2010/nim-vortex/issues/399 -- until it is fixed,
  ## treat `respHighWater` as the largest useful `res.write`.
  let idx = offset div dlBlockSize
  let inBlk = offset mod dlBlockSize
  stampScratch(idx)
  res.write(dlScratch.toOpenArray(inBlk, inBlk + n - 1))

proc computeDlSha(size: int): string =
  ## SHA-1 of the whole `size`-byte download stream.
  var ctx: sha1
  ctx.init()
  var remaining = size
  var idx = 0
  while remaining > 0:
    let n = min(dlBlockSize, remaining)
    stampScratch(idx)
    ctx.update(dlScratch.toOpenArray(0, n - 1))
    remaining -= n
    inc idx
  ($ctx.finish()).toLowerAscii

let dlShaWarm = block:
  ## `x-sha1` for the sizes the clients actually ask for, hashed before the
  ## loops start. Hashing 1 GiB on first request would park a loop thread for a
  ## second or more -- harmless on a single-transfer cell, but in `mixed` that
  ## thread also owns two dozen other workers' connections.
  ##
  ## Only the two workloads that request `NAVI_STREAM_BYTES` pay for warming it,
  ## though: every other cell would hash a gigabyte at startup for a size no
  ## request ever names, delaying each of the five servers' readiness for
  ## nothing. `dlSha` still computes and caches any off-menu size lazily.
  var t: seq[(int, string)] = @[(dlBlockSize, computeDlSha(dlBlockSize))]
  if srvWorkload in ["streamDownload", "mixed"] and
     streamBytes != dlBlockSize and streamBytes > 0:
    t.add (streamBytes, computeDlSha(streamBytes))
  t

var dlShaCache {.threadvar.}: seq[(int, string)]

proc dlSha(size: int): string {.gcsafe.} =
  ## The cached digest for `size`: the prewarmed table, then this thread's
  ## cache, then compute (an off-menu `?size=` only run by hand).
  {.cast(gcsafe).}:
    for (sz, sha) in dlShaWarm:
      if sz == size: return sha
  for (sz, sha) in dlShaCache:
    if sz == size: return sha
  result = computeDlSha(size)
  dlShaCache.add (size, result)

# --- /echo canonicalisation --------------------------------------------------

proc sortedJson(n: JsonNode): JsonNode {.gcsafe.} =
  ## A copy of `n` with every object's keys in sorted order. Serialised with
  ## `$` this is Python's `json.dumps(doc, sort_keys=True,
  ## separators=(",", ":"), ensure_ascii=False)`: std/json's `$` is already
  ## compact and leaves non-ASCII as UTF-8. Sorting is not cosmetic -- for the
  ## catalogue's unsorted-key documents the client asserts the echo is NOT the
  ## bytes it sent, which is what proves the server parsed rather than
  ## byte-echoed.
  case n.kind
  of JObject:
    result = newJObject()
    var keys: seq[string]
    for k in n.keys: keys.add k
    sort(keys)
    for k in keys: result[k] = sortedJson(n[k])
  of JArray:
    result = newJArray()
    for item in n.items: result.add sortedJson(item)
  else: result = n

proc canonicalForm(body: string): string {.gcsafe.} =
  ## Python's `urlencode(sorted(parse_qsl(body, keep_blank_values=True)))`:
  ## decode the pairs, sort them, re-encode with `+` for space. The client
  ## decodes both sides and compares sorted pairs, so this is verified as
  ## pairs, not as bytes.
  var pairs: seq[(string, string)]
  try:
    for k, v in body.decodeQuery: pairs.add (k, v)
  except CatchableError:
    return body                         # not decodable: echo the bytes
  sort(pairs)
  var parts: seq[string]
  for (k, v) in pairs: parts.add encodeUrl(k) & "=" & encodeUrl(v)
  parts.join("&")

proc echoReply(req: Request, res: Response) {.gcsafe.} =
  ## app.py's `/echo`, end to end: decode, canonicalise per content type,
  ## re-encode, and answer. `x-echo-method` and `x-echo-stress` are asserted on
  ## every request by clients/parts/requests_part.nim.
  let raw = req.body
  var decodeOk = true
  var body = decodeBody(raw, req.header("content-encoding"), decodeOk)
  let media =
    block:
      let ct = req.header("content-type")
      if ct.len > 0: ct else: "application/octet-stream"
  let base = media.split(';', 1)[0].strip().toLowerAscii
  var hdrs = @[("x-echo-method", $req.method),
               ("x-echo-stress", req.header("x-stress"))]
  if not decodeOk:
    # A body that does not match its own Content-Encoding. app.py would raise
    # out of `decode` and Starlette would turn that into a 500; a named 400 says
    # the same thing more clearly and is just as hard a client failure (every
    # client asserts 200). The point is that this never silently echoes the
    # undecoded bytes.
    hdrs.add ("x-echo-error", "bad-encoding")
    res.send(Http400, "undecodable content-encoding", hdrs)
    return
  if base == "application/json" and body.len > 0:
    var doc: JsonNode
    try:
      doc = parseJson(body)
    except CatchableError as e:
      # A navi encoder bug becomes an immediate hard client failure (a non-200
      # with a diagnostic body), not a confusing byte mismatch.
      hdrs.add ("x-echo-error", "bad-json")
      res.send(Http400, e.msg, hdrs)
      return
    body = $sortedJson(doc)
  elif base == "application/x-www-form-urlencoded" and body.len > 0:
    body = canonicalForm(body)
  if req.method == HttpHead:
    # app.py answers HEAD with the headers and no body. vortex routes HEAD to
    # the GET handler and its codec drops the body, so all this branch does is
    # skip the re-encode (there is nothing to encode) and the explicit
    # Content-Type. The reply still carries one: vortex's `send` defaults
    # Content-Type to text/plain when no header supplies it. No client asserts
    # the Content-Type of a HEAD, so leave the default rather than invent a
    # header app.py does not send either.
    res.send(Http200, "", hdrs)
    return
  let want = req.header("x-want-encoding")
  let wire = if want.len > 0: encodeBody(body, want) else: body
  # Only claim a Content-Encoding when something was actually encoded -- an
  # unknown or unavailable codec falls back to the plain body, exactly as
  # app.py's `if want and out is not body` does.
  if want.len > 0 and wire != body:
    hdrs.add ("content-encoding", want)
  hdrs.add ("content-type", media)
  res.send(Http200, wire, hdrs)

# --- coverage routes ---------------------------------------------------------

proc statusReply(req: Request, res: Response) {.gcsafe.} =
  ## `/status/{code}`: return exactly `code`, with app.py's empty-body rule for
  ## 1xx/204/304. (featureChecks only drives 404 and 503; the rest are here so
  ## the two servers answer the same, and a 1xx final response is as malformed
  ## on one as on the other.)
  let code = try: parseInt(req.param("code")) except ValueError: 400
  if code < 200 or code == 204 or code == 304:
    res.send(HttpCode(code))
  else:
    res.send(HttpCode(code), "status-" & $code)

proc redirectReply(req: Request, res: Response) {.gcsafe.} =
  ## `/redirect/{n}`: a 302 chain down to `/redirect/0`, which answers 200
  ## `redirect-done`. featureChecks follows 3 hops and asserts the body.
  let n = try: parseInt(req.param("n")) except ValueError: 0
  if n <= 0:
    res.send(Http200, "redirect-done", @[("content-type", "text/plain")])
  else:
    res.redirect("/redirect/" & $(n - 1))

proc authReply(req: Request, res: Response) {.gcsafe.} =
  ## `/needs-auth`: 401 + `WWW-Authenticate` without Basic credentials, 403
  ## with the wrong ones, 200 `authed` with `stress:secret`.
  let auth = req.header("authorization")
  if not auth.startsWith("Basic "):
    res.send(Http401, "", @[("www-authenticate", "Basic realm=\"stress\"")])
    return
  var userpass = ""
  try:
    userpass = base64.decode(auth[len("Basic ") .. ^1])
  except CatchableError:
    userpass = ""
  if userpass != "stress:secret": res.send(Http403, "")
  else: res.send(Http200, "authed")

proc setCookieReply(req: Request, res: Response) {.gcsafe.} =
  ## `/setcookie`: Starlette's `set_cookie("stress-cookie", "abc123")`, i.e.
  ## `Path=/; SameSite=lax` and neither Secure nor HttpOnly. vortex's secure
  ## defaults are turned off here on purpose: the point is to hand navi's jar
  ## the same cookie both servers send.
  res.send(Http200, "ok",
           @[setCookie("stress-cookie", "abc123", secure = false,
                       httpOnly = false, sameSite = "lax")])

proc needsCookieReply(req: Request, res: Response) {.gcsafe.} =
  ## `/needs-cookie`: 400 unless the jar carried `/setcookie`'s cookie back.
  if req.cookies["stress-cookie"] != "abc123":
    res.send(Http400, "missing cookie")
  else:
    res.send(Http200, "cookie-ok")

# --- /events, /upload, /download --------------------------------------------

proc sseStart(req: Request): int {.gcsafe.} =
  ## The first event id to emit: `Last-Event-ID + 1` on a resume, else 1.
  ## app.py is `int(last) if (last and last.isdigit()) else 0`, then `+ 1`, so
  ## only a run of ASCII digits resumes: a negative, signed, spaced or
  ## non-numeric header restarts at 1 rather than being parsed. `parseInt` would
  ## accept `-5` and `+5`, which is the one way the two servers could disagree.
  let last = req.lastEventId
  result = 1
  if last.len > 0 and last.allCharsInSet({'0' .. '9'}):
    try: result = parseInt(last) + 1
    except ValueError: result = 1       # digits, but past int range

proc downloadSize(req: Request): int {.gcsafe.} =
  ## `?size=N`, defaulting to 1 MiB as app.py's signature does.
  let v = req.query.getOrDefault("size")
  if v.len == 0: return dlBlockSize
  try: max(0, parseInt(v)) except ValueError: dlBlockSize

type UploadBox = ref object
  ## Heap-held SHA-1 state for the synchronous upload: `req.onBody` runs after
  ## the handler frame has returned, so this cannot live on its stack.
  ctx: sha1
  size: int

proc uploadDone(res: Response, box: UploadBox) {.gcsafe.} =
  res.send(Http200, %*{"sha1": ($box.ctx.finish()).toLowerAscii,
                       "size": box.size})

when asyncMode:
  proc hEcho(req: Request, res: Response) {.async.} = echoReply(req, res)
  proc hStatus(req: Request, res: Response) {.async.} = statusReply(req, res)
  proc hRedirect(req: Request, res: Response) {.async.} = redirectReply(req, res)
  proc hAuth(req: Request, res: Response) {.async.} = authReply(req, res)
  proc hSetCookie(req: Request, res: Response) {.async.} = setCookieReply(req, res)
  proc hNeedsCookie(req: Request, res: Response) {.async.} = needsCookieReply(req, res)

  proc hWs(req: Request, res: Response) {.async.} =
    ## Registered twice: with `rt.ws` for the h1 Upgrade (a GET), and by hand
    ## with `addRoute(HttpConnect, ...)` + `toHandler` for the h2/h3 Extended
    ## CONNECT, because vortex's correct wrapper for that leg, `wsToHandler`, is
    ## not exported (https://github.com/cryo2010/nim-vortex/issues/400). On a
    ## failed Future `toHandler` writes an HTTP 500 into a stream that has
    ## already been upgraded; `wsToHandler` would instead close with 1011. So
    ## this body catches everything itself and closes the socket, which leaves
    ## `toHandler`'s failure path unreachable and makes the two legs behave
    ## alike. Drop the try and the hand-registered route once the upstream issue
    ## is fixed.
    var ws: WebSocket
    var accepted = false
    try:
      ws = req.acceptWebSocket()
      accepted = true
      ws.messages(msg, kind):
        ws.send(msg, kind)              # echo, frame kind preserved
    except CatchableError:
      if accepted: ws.close(1011)       # 1011 = internal error, as wsToHandler

  proc hEvents(req: Request, res: Response) {.async.} =
    var eid = sseStart(req)
    let s = res.sse()
    var sent = 0
    while true:
      let ok = s.send("event-" & $eid, id = $eid)
      inc eid
      inc sent
      if sseDropEvery > 0 and sent >= sseDropEvery:
        s.close()                       # the periodic drop; the client resumes
        return
      if not ok:
        if not s.alive: return
        await s.drained()

  proc hUpload(req: Request, res: Response) {.async.} =
    var ctx: sha1
    ctx.init()
    var size = 0
    while true:
      let chunk = await req.read()
      if chunk.len == 0: break
      ctx.update(chunk)
      size += chunk.len
    res.send(Http200, %*{"sha1": ($ctx.finish()).toLowerAscii, "size": size})

  proc hDownload(req: Request, res: Response) {.async.} =
    let size = downloadSize(req)
    res.sendHead(Http200, "application/octet-stream",
                 @[("x-sha1", dlSha(size))])
    var sent = 0
    while sent < size:
      let n = min(dlChunk, size - sent)
      let ok = writeSlice(res, sent, n)
      sent += n
      if not ok and sent < size:
        if not req.isAlive: return
        await res.drained()
    res.finish()

else:
  proc hEcho(req: Request, res: Response) {.gcsafe.} = echoReply(req, res)
  proc hStatus(req: Request, res: Response) {.gcsafe.} = statusReply(req, res)
  proc hRedirect(req: Request, res: Response) {.gcsafe.} = redirectReply(req, res)
  proc hAuth(req: Request, res: Response) {.gcsafe.} = authReply(req, res)
  proc hSetCookie(req: Request, res: Response) {.gcsafe.} = setCookieReply(req, res)
  proc hNeedsCookie(req: Request, res: Response) {.gcsafe.} = needsCookieReply(req, res)

  proc hWs(req: Request, res: Response) {.gcsafe.} =
    let ws = req.acceptWebSocket()
    ws.onMessage = proc(ws: WebSocket, data: string, kind: WsKind) {.gcsafe.} =
      ws.send(data, kind)               # echo, frame kind preserved

  # The synchronous SSE and download producers are onDrain trampolines: write
  # until vortex reports the send backlog is full, then hand the loop back and
  # resume from the drain callback. Writing past that point would grow the
  # buffer without bound, and looping without yielding would starve the loop
  # thread of every other connection it owns.
  type SseBox = ref object
    s: SseStream
    eid: int
    sent: int

  type DlBox = ref object
    req: Request                        # only for req.isAlive; see pumpDownload
    res: Response
    size: int
    sent: int

  proc pumpSse(box: SseBox) {.gcsafe.}
  proc pumpSse(box: SseBox) {.gcsafe.} =
    while true:
      let ok = box.s.send("event-" & $box.eid, id = $box.eid)
      inc box.eid
      inc box.sent
      if sseDropEvery > 0 and box.sent >= sseDropEvery:
        box.s.close()
        return
      if not ok:
        if not box.s.alive: return
        box.s.onDrain(proc(s: SseStream) {.gcsafe.} = pumpSse(box))
        return

  proc hEvents(req: Request, res: Response) {.gcsafe.} =
    pumpSse(SseBox(s: res.sse(), eid: sseStart(req), sent: 0))

  proc pumpDownload(box: DlBox) {.gcsafe.}
  proc pumpDownload(box: DlBox) {.gcsafe.} =
    while box.sent < box.size:
      let n = min(dlChunk, box.size - box.sent)
      let ok = writeSlice(box.res, box.sent, n)
      box.sent += n
      if not ok and box.sent < box.size:
        # Same liveness check as pumpSse's `box.s.alive` and the async
        # hDownload's `req.isAlive`: a client that walks away mid-transfer (the
        # recycle cells do, every few seconds) must not have a drain callback
        # re-registered against its gone connection.
        if not box.req.isAlive: return
        box.res.onDrain(proc(r: Response) {.gcsafe.} = pumpDownload(box))
        return
    box.res.finish()

  proc hDownload(req: Request, res: Response) {.gcsafe.} =
    let size = downloadSize(req)
    res.sendHead(Http200, "application/octet-stream",
                 @[("x-sha1", dlSha(size))])
    pumpDownload(DlBox(req: req, res: res, size: size, sent: 0))

  proc hUpload(req: Request, res: Response) {.gcsafe.} =
    let box = UploadBox(size: 0)
    box.ctx.init()
    req.onBody proc(chunk: openArray[char], last: bool) {.gcsafe.} =
      if chunk.len > 0:
        box.ctx.update(chunk)
        box.size += chunk.len
      if last: uploadDone(req.response, box)

when isMainModule:
  var rt = newRouter()
  # /echo answers every verb app.py registers. HEAD derives from GET inside
  # vortex's router; OPTIONS must be registered explicitly, because the router
  # otherwise auto-answers it with a bodiless 204 that carries neither
  # x-echo-method nor x-echo-stress.
  for m in [HttpGet, HttpPost, HttpPut, HttpPatch, HttpDelete, HttpOptions]:
    when asyncMode: rt.addRoute(m, "/echo", toHandler(hEcho))
    else: rt.addRoute(m, "/echo", hEcho)
  # /ws over an h1 Upgrade (a GET) and over an h2/h3 Extended CONNECT. In an
  # async build the GET leg must go through `rt.ws`, not `rt.get`: `ws` wraps
  # the handler so a failed Future closes the socket with 1011, where `get`
  # would write an HTTP 500 into the already upgraded stream. The CONNECT leg
  # has no such wrapper available (wsToHandler is not exported upstream,
  # https://github.com/cryo2010/nim-vortex/issues/400), so it stays a hand
  # `toHandler` route and hWs catches its own failures instead.
  when asyncMode:
    rt.ws("/ws", hWs)                                 # h1 Upgrade
    rt.addRoute(HttpConnect, "/ws", toHandler(hWs))   # h2/h3 Extended CONNECT
  else:
    rt.get("/ws", hWs)
    rt.addRoute(HttpConnect, "/ws", hWs)
  rt.get("/events", hEvents)
  rt.post("/upload", hUpload, streaming = true)
  rt.get("/download", hDownload)
  for m in [HttpGet, HttpPost, HttpPut, HttpPatch, HttpDelete, HttpOptions]:
    when asyncMode: rt.addRoute(m, "/status/{code}", toHandler(hStatus))
    else: rt.addRoute(m, "/status/{code}", hStatus)
  rt.get("/redirect/{n}", hRedirect)
  rt.get("/needs-auth", hAuth)
  rt.get("/setcookie", hSetCookie)
  rt.get("/needs-cookie", hNeedsCookie)

  var settings = initVortexConfig(
    port = Port(srvPort),
    address = getEnv("NAVI_HOST", "127.0.0.1"),
    numThreads = srvThreads,
    compress = false,                   # /echo encodes per x-want-encoding itself
    decompressRequest = false,          # ... and decodes the request itself
    headerTimeout = srvHeaderTimeout,
    # The harness's own lifecycle knobs. NAVI_KEEPALIVE_TIMEOUT defaults past
    # the whole soak (run.sh computes NAVI_SECONDS + 1h) so a steady-state cell
    # never idle-closes; NAVI_RECYCLE=1 lowers it so pooled connections really
    # are recycled mid-soak.
    keepAliveTimeout = parseInt(getEnv("NAVI_KEEPALIVE_TIMEOUT", "60")),
    # HTTP/1 keep-alive request cap: vortex's only per-connection request
    # counter, so it is the only thing that recycles a BUSY connection. run.sh
    # prints a per-protocol notice saying how much recycle coverage each cell
    # really has (see the README's recycle note).
    maxRequestsPerSocket = parseInt(getEnv("NAVI_KEEPALIVE_MAX", "0")),
    # The `requests` catalogue occasionally sends an 8 KiB x-big header value to
    # push HPACK/QPACK; 16 KiB (the default) leaves little room beside it.
    maxHeaderSize = 64 * 1024,
    maxBodySize = streamBytes + 1024 * 1024,   # the /upload workload
    # Leave the flow-control windows at vortex's defaults (1 MiB stream, 1 MiB
    # h2 connection, 4 MiB h3 connection). They differ from hypercorn's 64 KiB
    # initial window on purpose, and the README says so: an `up` number is not
    # comparable across the two servers.
    serverHeader = "vortex")
  settings.certFile = srvCert
  settings.keyFile = srvKey
  settings.http3 = srvHttp3             # QUIC listener + automatic Alt-Svc
  let srv = newVortex(rt.toHandler, settings, rt.streamPredicate).start()
  echo "listening on ", int(srv.port)
  flushFile(stdout)
  # run.sh tears a cell down with SIGKILL on the process group, so there is no
  # orderly-shutdown path to install here; park the main thread on the loops.
  srv.waitFor()
