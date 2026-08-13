## HTTP/3 QUIC transport (phase 2b).
##
## Compiled ONLY in a `-d:naviHttp3` build; no default CI job enables it. It links
## ngtcp2 + nghttp3 + the OpenSSL >= 3.5 QUIC crypto binding (via pkg-config) and
## compiles the h3client.c driver, which performs a blocking HTTP/3 GET over QUIC.
## Verified end to end by tests/interop/http3 (a from-source toolchain image and a
## live Caddy h3 origin): navi completes the QUIC handshake, runs nghttp3/QPACK,
## and reads back the response.
##
## Scope: blocking, one GET per call (sync path). TODO (phase 2c): server
## certificate verification, a persistent/multiplexed connection object, async
## integration, and streaming bodies (see docs/http3.md).

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

proc ngtcp2_version(least: cint): ptr Ngtcp2Info
  {.importc, cdecl, header: "ngtcp2/ngtcp2.h".}
proc nghttp3_version(least: cint): ptr Nghttp3Info
  {.importc, cdecl, header: "nghttp3/nghttp3.h".}

# From h3client.c: a blocking HTTP/3 GET. Returns 0 on success, negative on
# failure; fills status and up to out_cap body bytes (out_len = bytes written).
proc navi_h3_get(host, port, sni, path: cstring, outStatus: ptr clong,
                 outBody: ptr char, outCap: csize_t, outLen: ptr csize_t): cint
  {.importc, cdecl.}

proc ngtcp2VersionStr*(): string = $ngtcp2_version(0).version_str
proc nghttp3VersionStr*(): string = $nghttp3_version(0).version_str

proc h3Get*(host: string, port: int, sni = "", path = "/"): Http3Response =
  ## Perform a blocking HTTP/3 GET over QUIC and return the status and body.
  ## `sni` defaults to `host`. Raises `QuicError` on transport failure. Sync path;
  ## the server certificate is not yet verified (phase 2c).
  let name = if sni.len > 0: sni else: host
  var status: clong
  var blen: csize_t
  var buf = newString(64 * 1024)
  let rv = navi_h3_get(host.cstring, ($port).cstring, name.cstring, path.cstring,
                       addr status, cast[ptr char](addr buf[0]),
                       csize_t(buf.len), addr blen)
  if rv != 0:
    raise newException(QuicError,
      "navi HTTP/3 GET to " & host & ":" & $port & path & " failed")
  buf.setLen(int(blen))
  Http3Response(status: int(status), body: buf)
