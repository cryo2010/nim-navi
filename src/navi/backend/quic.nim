## HTTP/3 QUIC transport (phase 2b).
##
## Compiled ONLY in a `-d:naviHttp3` build; no default CI job enables it. It links
## ngtcp2 + nghttp3 + the OpenSSL >= 3.5 QUIC crypto binding (via pkg-config) and
## compiles the h3client.c driver: a persistent QUIC connection (`QuicConn`) that
## serves multiple blocking HTTP/3 GETs. Verified end to end by tests/interop/http3
## (a from-source toolchain image and a live Caddy h3 origin): navi completes the
## QUIC handshake, runs nghttp3/QPACK, and reads back responses.
##
## This module is the sync backend's FFI + blocking drivers (buffered `request`,
## and the `submitStream`/`awaitHeaders`/`readStreamBody` streaming reader); the
## async backends drive the same C core from quic_async/quic_chronos. Buffered and
## streamed (`bodyStream`) request bodies and incremental response reads are all
## supported; the server certificate and hostname are verified (secure by default).

when not defined(naviHttp3):
  {.error: "navi/backend/quic is a -d:naviHttp3-only module (HTTP/3 WIP).".}

import std/strutils
import ../core/altsvc
export altsvc.AltSvcEndpoint

# Link the h3 stack via pkg-config so the build follows wherever the libraries
# are installed (the interop image exposes them via PKG_CONFIG_PATH). A
# -d:naviHttp3 build requires ngtcp2, nghttp3, and OpenSSL >= 3.5 present.
{.passC: "-std=c++20".}   # h3client.cpp is C++20 (std::span, RAII); Nim's -w hides
                          # the "not valid for C" note this adds to the .c files.
{.passC: gorge("pkg-config --cflags libngtcp2 libngtcp2_crypto_ossl libnghttp3 libssl").}
{.passL: gorge("pkg-config --libs libngtcp2 libngtcp2_crypto_ossl libnghttp3 libssl libcrypto").}
{.passL: "-lstdc++".}     # the C++ standard library, for the C++ driver
{.compile: "h3client.cpp".}

const naviHttp3MinOpenSsl* = "3.5.0"
  ## Minimum OpenSSL for the QUIC crypto binding (ngtcp2_crypto_ossl).

type
  Ngtcp2Info {.importc: "ngtcp2_info", header: "ngtcp2/ngtcp2.h".} = object
    age: cint
    version_num: cint
    version_str: cstring
  Nghttp3Info {.importc: "nghttp3_info", header: "nghttp3/nghttp3.h".} = object
    age: cint
    version_num: cint
    version_str: cstring

  QuicError* = object of CatchableError
    ## QUIC/h3 transport failure (handshake, stream reset, timeout). The engine
    ## will treat it as a signal to fall back to h2/h1 for the origin.

  Http3Response* = object
    status*: int
    body*: string
    headers*: seq[(string, string)]   ## response fields (lowercased names)
    trailers*: seq[(string, string)]  ## trailing fields after the body (RFC 9114 4.1)

  QuicConn* = ref object
    ## A persistent QUIC/h3 connection to one origin. Open it once and issue
    ## multiple `get` requests, then `close`. One request in flight at a time
    ## (blocking); the handle owns the UDP socket and the ngtcp2/nghttp3 state.
    handle: pointer

proc ngtcp2_version(least: cint): ptr Ngtcp2Info
  {.importc, cdecl, header: "ngtcp2/ngtcp2.h".}
proc nghttp3_version(least: cint): ptr Nghttp3Info
  {.importc, cdecl, header: "nghttp3/nghttp3.h".}

# From h3client.c. navi_h3_open completes the handshake (verifying the peer cert
# unless verify=0; caFile "" uses the system store) and returns an opaque
# connection, or nil on failure. navi_h3_request runs one GET on it (0 ok, filling
# status and up to outCap body bytes). navi_h3_close frees it.
type H3BodyPull* = proc(env: pointer, outPtr: ptr cstring): int {.cdecl.}
  ## C-callable pull for a streamed request body: returns the next chunk's length
  ## and sets `outPtr[]` to its bytes (valid only for the call); 0 = end, < 0 = error.

proc navi_h3_open(host, port, sni, caFile: cstring, verify: cint): pointer
  {.importc, cdecl.}
proc navi_h3_request(c: pointer, verb, path, reqHeaders: cstring, body: ptr char,
                     bodyLen: csize_t, pull: H3BodyPull, pullEnv: pointer,
                     reqTrailers: cstring, outStatus: ptr clong, outBody: ptr char,
                     outCap: csize_t, outLen: ptr csize_t, outHeaders: ptr char,
                     hdrCap: csize_t, hdrLen: ptr csize_t, outTrailers: ptr char,
                     trlCap: csize_t, trlLen: ptr csize_t): cint {.importc, cdecl.}
proc navi_h3_close*(c: pointer) {.importc, cdecl.}

# --- streamed request body (navi bodyStream) over h3 -------------------------
# A small env carries navi's synchronous producer across the FFI: the C data reader
# calls `h3PullThunk` to fetch each chunk. Keep the env alive (a live ref on the
# caller's stack / async frame) for the whole request -- the reader borrows it.
type H3PullEnv* = ref object
  producer*: proc(): string {.closure, raises: [CatchableError].}
  current: string                 ## holds the current chunk alive across the C call

proc h3PullThunk*(env: pointer, outPtr: ptr cstring): int {.cdecl.} =
  let pe = cast[H3PullEnv](env)
  {.cast(gcsafe).}:               # single-threaded stress/driver; producer touches the GC
    try:
      pe.current = pe.producer()
    except CatchableError:
      return -1
  if pe.current.len == 0:
    outPtr[] = nil
    return 0
  outPtr[] = cast[cstring](addr pe.current[0])
  pe.current.len

# Non-blocking step functions (exported for the asyncdispatch driver in
# quic_async.nim): create without driving the handshake, then pump send/recv/timer
# from the caller's event loop until handshake / request completion.
proc navi_h3_new*(host, port, sni, caFile: cstring, verify: cint): pointer
  {.importc, cdecl.}
proc navi_h3_fd*(c: pointer): cint {.importc, cdecl.}
proc navi_h3_send*(c: pointer, buf: pointer, buflen: csize_t): int {.importc, cdecl.}
proc navi_h3_recv*(c: pointer, pkt: pointer, len: csize_t): cint {.importc, cdecl.}
proc navi_h3_timeout_ms*(c: pointer): uint64 {.importc, cdecl.}
proc navi_h3_handle_timeout*(c: pointer): cint {.importc, cdecl.}
proc navi_h3_handshake_done*(c: pointer): cint {.importc, cdecl.}
proc navi_h3_bind*(c: pointer): cint {.importc, cdecl.}
proc navi_h3_pump*(c: pointer): cint {.importc, cdecl.}
  ## One blocking send/recv/timer cycle (for the sync buffered + streaming drivers).
proc navi_h3_submit*(c: pointer, verb, path, reqHeaders: cstring, body: ptr char,
                     bodyLen: csize_t, pull: H3BodyPull, pullEnv: pointer,
                     reqTrailers: cstring): int64 {.importc, cdecl.}   ## stream id, or -1
proc navi_h3_stream_done*(c: pointer, sid: int64): cint {.importc, cdecl.}
proc navi_h3_stream_reset*(c: pointer, sid: int64): cint {.importc, cdecl.}
  ## 1 if the stream ended by reset/abort rather than a normal response.
proc navi_h3_stream_length_mismatch*(c: pointer, sid: int64): cint {.importc, cdecl.}
  ## 1 if the stream ended cleanly but its body length disagreed with Content-Length.

const h3BodyLengthErr* =
  "navi: response body length does not match the declared Content-Length"
proc navi_h3_take_response*(c: pointer, sid: int64, outStatus: ptr clong,
                            outBody: ptr char, outCap: csize_t, outLen: ptr csize_t,
                            outHeaders: ptr char, hdrCap: csize_t, hdrLen: ptr csize_t,
                            outTrailers: ptr char, trlCap: csize_t,
                            trlLen: ptr csize_t): cint {.importc, cdecl.}
proc navi_h3_response_trailers*(c: pointer, sid: int64, outTrailers: ptr char,
                                trlCap: csize_t, trlLen: ptr csize_t): cint
  {.importc, cdecl.}   ## response trailers (after body EOF); does not drop the stream
# Streaming read path (stream()/SSE): headers first, then body chunks as they land.
proc navi_h3_response_headers*(c: pointer, sid: int64, outStatus: ptr clong,
                               outHeaders: ptr char, hdrCap: csize_t,
                               hdrLen: ptr csize_t, outReady: ptr cint): cint
  {.importc, cdecl.}   ## outReady=1 once all headers are in (does not drop the stream)
proc navi_h3_read_body*(c: pointer, sid: int64, buf: ptr char, cap: csize_t,
                        outEof: ptr cint): int {.importc, cdecl.}
  ## bytes drained (0 + eof=0 means "nothing yet"); does not drop the stream on eof
proc navi_h3_stream_free*(c: pointer, sid: int64) {.importc, cdecl.}
  ## drop a stream (after eof+reset check, or to abandon a handle)

proc ngtcp2VersionStr*(): string = $ngtcp2_version(0).version_str
proc nghttp3VersionStr*(): string = $nghttp3_version(0).version_str

proc h3Open*(host: string, port: int, sni = "", caFile = "",
             verify = true): QuicConn =
  ## Open a persistent HTTP/3 connection and complete the handshake. `sni`
  ## defaults to `host`; the server certificate and hostname are verified by
  ## default (`caFile` adds a custom CA, `verify=false` disables checking).
  ## Raises `QuicError` on connect or verification failure.
  let name = if sni.len > 0: sni else: host
  let h = navi_h3_open(host.cstring, ($port).cstring, name.cstring,
                       caFile.cstring, cint(verify))
  if h == nil:
    raise newException(QuicError,
      "navi HTTP/3 connect to " & host & ":" & $port & " failed")
  QuicConn(handle: h)

proc encodeH3Fields*(fields: openArray[(string, string)]): string =
  ## Serialize name/value pairs to the "name\nvalue\n..." wire form the C driver reads
  ## (request headers and request trailers). "" when there are none.
  for (k, v) in fields:
    result.add k; result.add '\n'; result.add v; result.add '\n'

proc parseH3Fields*(buf: string): seq[(string, string)] =
  ## Parse the "name\nvalue\n..." form the C driver returns (response headers and
  ## response trailers) back into pairs.
  let parts = buf.split('\n')
  var i = 0
  while i + 1 < parts.len:
    result.add((parts[i], parts[i + 1]))
    i += 2

proc request*(c: QuicConn, verb: string, path = "/",
              headers: openArray[(string, string)] = [], body = "",
              producer: proc(): string {.closure, raises: [CatchableError].} = nil,
              trailers: openArray[(string, string)] = []):
              Http3Response =
  ## Issue an HTTP/3 request on an open connection and return status, body, and
  ## response headers and trailers. `verb` is the method (GET/POST/PUT/...), `headers`
  ## are extra request fields (names must be lowercase, per HTTP/3, and free of
  ## connection-specific fields). The body is either buffered (`body`) or streamed
  ## from `producer` (navi's `bodyStream`, pulled chunk by chunk); `trailers` are sent
  ## after the body as a trailing HEADERS section. Raises `QuicError` on transport
  ## failure.
  if c.handle == nil:
    raise newException(QuicError, "navi HTTP/3: connection is closed")
  let reqHdr = encodeH3Fields(headers)
  let reqTrl = encodeH3Fields(trailers)
  var status: clong
  var blen, hlen, tlen: csize_t
  var rbody = newString(64 * 1024)
  var hbuf = newString(16 * 1024)
  var tbuf = newString(16 * 1024)
  var b = body
  let streamed = producer != nil
  let pe = if streamed: H3PullEnv(producer: producer) else: nil
  let pull = if streamed: h3PullThunk else: nil
  let bp = if not streamed and b.len > 0: cast[ptr char](addr b[0]) else: nil
  let rv = navi_h3_request(c.handle, verb.cstring, path.cstring, reqHdr.cstring,
                           bp, csize_t(if streamed: 0 else: b.len), pull,
                           cast[pointer](pe), reqTrl.cstring, addr status,
                           cast[ptr char](addr rbody[0]), csize_t(rbody.len),
                           addr blen, cast[ptr char](addr hbuf[0]),
                           csize_t(hbuf.len), addr hlen,
                           cast[ptr char](addr tbuf[0]), csize_t(tbuf.len), addr tlen)
  if rv == -2:   # body length disagreed with Content-Length: a real (received) response,
    raise newException(IOError, h3BodyLengthErr)   # so raise instead of QuicError (no h2 fallback)
  if rv != 0:
    raise newException(QuicError, "navi HTTP/3 " & verb & " " & path & " failed")
  rbody.setLen(int(blen))
  hbuf.setLen(int(hlen))
  tbuf.setLen(int(tlen))
  Http3Response(status: int(status), body: rbody, headers: parseH3Fields(hbuf),
                trailers: parseH3Fields(tbuf))

proc get*(c: QuicConn, path = "/",
          headers: openArray[(string, string)] = []): Http3Response =
  ## Issue an HTTP/3 GET (convenience over `request`).
  c.request("GET", path, headers)

proc close*(c: QuicConn) =
  ## Close the connection and release its socket and library state. Idempotent.
  if c.handle != nil:
    navi_h3_close(c.handle)
    c.handle = nil

# --- blocking streaming read (stream()/SSE over h3, sync backend) -----------
# The sync twin of quic_async's submitStream/awaitHeaders/readStreamBody: submit a
# request, then drive the connection with navi_h3_pump between reads and pull the
# response incrementally. One stream in flight per sync QuicConn.

proc submitStream*(c: QuicConn, verb, path: string,
                   headers: openArray[(string, string)] = [];
                   trailers: openArray[(string, string)] = []): int64 =
  ## Submit an h3 request for a streaming read; returns the stream id (< 0 on error).
  if c.handle == nil: return -1
  let reqHdr = encodeH3Fields(headers)
  let reqTrl = encodeH3Fields(trailers)
  navi_h3_submit(c.handle, verb.cstring, path.cstring, reqHdr.cstring, nil,
                 csize_t(0), nil, nil, reqTrl.cstring)

proc awaitHeaders*(c: QuicConn, sid: int64):
    tuple[status: int, headers: seq[(string, string)]] =
  ## Drive the connection until `sid`'s response headers are in, then return status +
  ## headers (the stream stays open for the body). Raises on reset / transport error.
  if c.handle == nil: raise newException(QuicError, "navi HTTP/3: connection is closed")
  var status: clong
  var hbuf = newString(16 * 1024)
  var ready: cint
  while true:
    var hlen: csize_t
    if navi_h3_response_headers(c.handle, sid, addr status,
                               cast[ptr char](addr hbuf[0]), csize_t(hbuf.len),
                               addr hlen, addr ready) != 0:
      raise newException(QuicError, "navi HTTP/3 stream gone")
    if ready != 0:
      hbuf.setLen(int(hlen))
      var hs: seq[(string, string)]
      let parts = hbuf.split('\n')
      var i = 0
      while i + 1 < parts.len: hs.add((parts[i], parts[i + 1])); i += 2
      return (int(status), hs)
    if navi_h3_stream_done(c.handle, sid) != 0:   # ended before any headers => reset
      raise newException(QuicError, "navi HTTP/3 stream ended before headers")
    if navi_h3_pump(c.handle) != 0:
      raise newException(QuicError, "navi HTTP/3 pump failed")

proc readStreamBody*(c: QuicConn, sid: int64): string =
  ## The next body chunk of `sid`, or "" at end of body (driving the connection until
  ## a chunk lands or the stream ends). Raises on a transport error.
  if c.handle == nil: raise newException(QuicError, "navi HTTP/3: connection is closed")
  var buf = newString(64 * 1024)
  var eof: cint
  while true:
    let n = navi_h3_read_body(c.handle, sid, cast[ptr char](addr buf[0]),
                              csize_t(buf.len), addr eof)
    if n < 0: raise newException(QuicError, "navi HTTP/3 stream gone")
    if n > 0: buf.setLen(int(n)); return buf
    if eof != 0: return ""
    if navi_h3_pump(c.handle) != 0:
      raise newException(QuicError, "navi HTTP/3 pump failed")

proc streamTrailers*(c: QuicConn, sid: int64): seq[(string, string)] =
  ## The response trailer fields of `sid` (they land after the body EOF); "" if none.
  ## Read after `readStreamBody` returns "" and before `freeStream`.
  if c.handle == nil: return
  var tbuf = newString(16 * 1024)
  var tlen: csize_t
  if navi_h3_response_trailers(c.handle, sid, cast[ptr char](addr tbuf[0]),
                               csize_t(tbuf.len), addr tlen) != 0:
    return
  tbuf.setLen(int(tlen))
  parseH3Fields(tbuf)

proc streamWasReset*(c: QuicConn, sid: int64): bool =
  ## Whether `sid` ended by reset/abort rather than a clean end (check at EOF).
  c.handle != nil and navi_h3_stream_reset(c.handle, sid) != 0

proc streamLengthMismatch*(c: QuicConn, sid: int64): bool =
  ## Whether `sid` ended cleanly but its body length disagreed with Content-Length
  ## (check at EOF, before freeStream).
  c.handle != nil and navi_h3_stream_length_mismatch(c.handle, sid) != 0

proc freeStream*(c: QuicConn, sid: int64) =
  ## Drop `sid` (STOP_SENDING + RESET_STREAM), abandoning an undrained handle.
  if c.handle != nil: navi_h3_stream_free(c.handle, sid)

proc h3Get*(host: string, port: int, sni = "", path = "/", caFile = "",
            verify = true): Http3Response =
  ## One-shot convenience: open a connection, issue one GET, and close. For
  ## several requests to one origin, use `h3Open` + `get` + `close`.
  let c = h3Open(host, port, sni, caFile, verify)
  defer: c.close()
  result = c.get(path)
