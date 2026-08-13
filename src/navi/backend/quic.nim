## HTTP/3 QUIC transport (phase 2, work in progress).
##
## Compiled ONLY in a `-d:naviHttp3` build; no CI job enables it yet. The FFI
## below is verified to link and initialize against the real libraries (ngtcp2 +
## nghttp3 + the OpenSSL >= 3.5 QUIC crypto binding) by tests/interop/http3, whose
## Dockerfile builds the toolchain and whose probe exercises exactly these calls.
##
## What works here: the binding layer (versions, crypto-ossl init, coupling an
## OpenSSL SSL to ngtcp2's crypto). What is still TODO (phase 2b): the client
## `ngtcp2_conn` + the ~15 callbacks, the UDP/timer event loop, the nghttp3 h3
## session, and request/response mapping. Those need a live handshake against the
## tests/interop/http3 Caddy origin to validate and are not guessed here.

when not defined(naviHttp3):
  {.error: "navi/backend/quic is a -d:naviHttp3-only module (HTTP/3 WIP).".}

import ../core/altsvc
export altsvc.AltSvcEndpoint

# Link the h3 stack via pkg-config, so the build follows wherever the libraries
# are installed (the interop image puts them under /opt and exposes them via
# PKG_CONFIG_PATH). A -d:naviHttp3 build requires these libraries present.
{.passC: gorge("pkg-config --cflags libngtcp2 libngtcp2_crypto_ossl libnghttp3 libssl").}
{.passL: gorge("pkg-config --libs libngtcp2 libngtcp2_crypto_ossl libnghttp3 libssl libcrypto").}

const naviHttp3MinOpenSsl* = "3.5.0"
  ## Minimum OpenSSL for the QUIC crypto binding (ngtcp2_crypto_ossl). Older
  ## OpenSSL/quictls/BoringSSL paths are out of scope.

type
  Ngtcp2Info {.importc: "ngtcp2_info", header: "ngtcp2/ngtcp2.h".} = object
    age: cint
    version_num: cint
    version_str: cstring
  Nghttp3Info {.importc: "nghttp3_info", header: "nghttp3/nghttp3.h".} = object
    age: cint
    version_num: cint
    version_str: cstring
  Ssl = pointer
  SslCtx = pointer
  Ngtcp2CryptoOsslCtxObj {.importc: "ngtcp2_crypto_ossl_ctx",
    header: "ngtcp2/ngtcp2_crypto_ossl.h", incompleteStruct.} = object
  Ngtcp2CryptoOsslCtx = ptr Ngtcp2CryptoOsslCtxObj

proc ngtcp2_version(least: cint): ptr Ngtcp2Info
  {.importc, cdecl, header: "ngtcp2/ngtcp2.h".}
proc nghttp3_version(least: cint): ptr Nghttp3Info
  {.importc, cdecl, header: "nghttp3/nghttp3.h".}
proc ngtcp2_crypto_ossl_init(): cint
  {.importc, cdecl, header: "ngtcp2/ngtcp2_crypto_ossl.h".}
proc ngtcp2_crypto_ossl_ctx_new(pctx: ptr Ngtcp2CryptoOsslCtx, ssl: Ssl): cint
  {.importc, cdecl, header: "ngtcp2/ngtcp2_crypto_ossl.h".}
proc ngtcp2_crypto_ossl_ctx_del(ctx: Ngtcp2CryptoOsslCtx)
  {.importc, cdecl, header: "ngtcp2/ngtcp2_crypto_ossl.h".}

type
  QuicConn* = ref object
    ## Owns one QUIC connection to an origin: the connected UDP socket, the
    ## ngtcp2 connection, the nghttp3 h3 session, the OpenSSL QUIC crypto context,
    ## and the loss-recovery timer. Fields are added as phase 2b lands.
    host*: string
    port*: int

  QuicError* = object of CatchableError
    ## QUIC/h3 transport failure (handshake, stream reset, timeout). The engine
    ## treats it as a signal to fall back to h2/h1 for the origin.

var cryptoInited = false

proc ngtcp2VersionStr*(): string = $ngtcp2_version(0).version_str
proc nghttp3VersionStr*(): string = $nghttp3_version(0).version_str

proc initQuicCrypto*() =
  ## Initialize the ngtcp2 OpenSSL crypto binding once per process. Verified by
  ## tests/interop/http3 (the probe runs exactly this path).
  if cryptoInited: return
  if ngtcp2_crypto_ossl_init() != 0:
    raise newException(QuicError, "navi HTTP/3: ngtcp2_crypto_ossl_init failed")
  cryptoInited = true

template notYet(): untyped =
  raise newException(QuicError,
    "navi HTTP/3: QUIC handshake not yet implemented (see docs/http3.md phase 2b)")

proc connectQuic*(endpoint: AltSvcEndpoint, serverName: string): QuicConn =
  ## Open a QUIC connection to a discovered h3 endpoint and complete the handshake
  ## (ALPN "h3", TLS verification against `serverName`). Phase 2b: builds on the
  ## verified crypto binding above (initQuicCrypto + ngtcp2_crypto_ossl_ctx_new).
  initQuicCrypto()
  notYet()

proc close*(c: QuicConn) =
  ## Close the QUIC connection and release the UDP socket and library state.
  discard   # nothing to release until the connection owns resources
