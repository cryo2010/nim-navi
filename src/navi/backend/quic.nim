## HTTP/3 QUIC transport (phase 2b).
##
## Compiled ONLY in a `-d:naviHttp3` build; no default CI job enables it. It links
## ngtcp2 + nghttp3 + the OpenSSL >= 3.5 QUIC crypto binding (via pkg-config) and
## compiles the h3client.c driver: a persistent QUIC connection (`QuicConn`) that
## serves multiple blocking HTTP/3 GETs. Verified end to end by tests/interop/http3
## (a from-source toolchain image and a live Caddy h3 origin): navi completes the
## QUIC handshake, runs nghttp3/QPACK, and reads back responses.
##
## Scope: blocking, one request in flight at a time (sync path); the server
## certificate and hostname are verified (secure by default). TODO: async +
## multiplexed streams, and streaming request/response bodies (see docs/http3.md).

when not defined(naviHttp3):
  {.error: "navi/backend/quic is a -d:naviHttp3-only module (HTTP/3 WIP).".}

import ../core/altsvc
export altsvc.AltSvcEndpoint

# Link the h3 stack via pkg-config so the build follows wherever the libraries
# are installed (the interop image exposes them via PKG_CONFIG_PATH). A
# -d:naviHttp3 build requires ngtcp2, nghttp3, and OpenSSL >= 3.5 present.
{.passC: gorge("pkg-config --cflags libngtcp2 libngtcp2_crypto_ossl libnghttp3 libssl").}
{.passL: gorge("pkg-config --libs libngtcp2 libngtcp2_crypto_ossl libnghttp3 libssl libcrypto").}
{.compile: "h3client.c".}

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
proc navi_h3_open(host, port, sni, caFile: cstring, verify: cint): pointer
  {.importc, cdecl.}
proc navi_h3_request(c: pointer, path: cstring, outStatus: ptr clong,
                     outBody: ptr char, outCap: csize_t,
                     outLen: ptr csize_t): cint {.importc, cdecl.}
proc navi_h3_close(c: pointer) {.importc, cdecl.}

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

proc get*(c: QuicConn, path = "/"): Http3Response =
  ## Issue an HTTP/3 GET on an open connection and return the status and body.
  ## Raises `QuicError` on transport failure.
  if c.handle == nil:
    raise newException(QuicError, "navi HTTP/3: connection is closed")
  var status: clong
  var blen: csize_t
  var buf = newString(64 * 1024)
  let rv = navi_h3_request(c.handle, path.cstring, addr status,
                           cast[ptr char](addr buf[0]), csize_t(buf.len),
                           addr blen)
  if rv != 0:
    raise newException(QuicError, "navi HTTP/3 GET " & path & " failed")
  buf.setLen(int(blen))
  Http3Response(status: int(status), body: buf)

proc close*(c: QuicConn) =
  ## Close the connection and release its socket and library state. Idempotent.
  if c.handle != nil:
    navi_h3_close(c.handle)
    c.handle = nil

proc h3Get*(host: string, port: int, sni = "", path = "/", caFile = "",
            verify = true): Http3Response =
  ## One-shot convenience: open a connection, issue one GET, and close. For
  ## several requests to one origin, use `h3Open` + `get` + `close`.
  let c = h3Open(host, port, sni, caFile, verify)
  try:
    result = c.get(path)
  finally:
    c.close()
