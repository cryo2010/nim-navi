## HTTP/3 binding-layer probe (phase 2a).
##
## Verifies that navi's ngtcp2 + nghttp3 + OpenSSL 3.5 QUIC FFI links and
## initializes against the real libraries: it reads the library versions, runs
## `ngtcp2_crypto_ossl_init`, builds an OpenSSL client `SSL`, and hands it to
## `ngtcp2_crypto_ossl_ctx_new` (the binding that couples ngtcp2's crypto to
## OpenSSL). This is the linkage checkpoint the full QUIC handshake + h3 GET
## driver builds on. Run inside the tests/interop/http3 image.

{.passC: "-I/opt/ngtcp2/include -I/opt/nghttp3/include -I/opt/ossl/include".}
{.passL: "-L/opt/ngtcp2/lib -lngtcp2 -lngtcp2_crypto_ossl " &
         "-L/opt/nghttp3/lib -lnghttp3 -L/opt/ossl/lib -lssl -lcrypto " &
         "-Wl,-rpath,/opt/ngtcp2/lib -Wl,-rpath,/opt/nghttp3/lib -Wl,-rpath,/opt/ossl/lib".}

type
  Ngtcp2Info {.importc: "ngtcp2_info", header: "ngtcp2/ngtcp2.h".} = object
    age: cint
    version_num: cint
    version_str: cstring
  Nghttp3Info {.importc: "nghttp3_info", header: "nghttp3/nghttp3.h".} = object
    age: cint
    version_num: cint
    version_str: cstring
  SslCtx = pointer
  Ssl = pointer
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

proc TLS_method(): pointer {.importc, cdecl, header: "openssl/ssl.h".}
proc SSL_CTX_new(m: pointer): SslCtx {.importc, cdecl, header: "openssl/ssl.h".}
proc SSL_CTX_free(c: SslCtx) {.importc, cdecl, header: "openssl/ssl.h".}
proc SSL_new(c: SslCtx): Ssl {.importc, cdecl, header: "openssl/ssl.h".}
proc SSL_free(s: Ssl) {.importc, cdecl, header: "openssl/ssl.h".}
proc OpenSSL_version(t: cint): cstring {.importc, cdecl, header: "openssl/crypto.h".}

when isMainModule:
  let ni = ngtcp2_version(0)
  let hi = nghttp3_version(0)
  doAssert ni != nil and hi != nil
  echo "ngtcp2  ", ni.version_str
  echo "nghttp3 ", hi.version_str
  echo "openssl ", OpenSSL_version(0)

  doAssert ngtcp2_crypto_ossl_init() == 0, "ngtcp2_crypto_ossl_init failed"
  echo "ok: ngtcp2_crypto_ossl_init"

  let ctx = SSL_CTX_new(TLS_method())
  doAssert ctx != nil, "SSL_CTX_new failed"
  let ssl = SSL_new(ctx)
  doAssert ssl != nil, "SSL_new failed"

  var octx: Ngtcp2CryptoOsslCtx
  doAssert ngtcp2_crypto_ossl_ctx_new(addr octx, ssl) == 0,
    "ngtcp2_crypto_ossl_ctx_new failed"
  echo "ok: ngtcp2_crypto_ossl_ctx_new (crypto binding coupled to OpenSSL SSL)"

  ngtcp2_crypto_ossl_ctx_del(octx)
  SSL_free(ssl)
  SSL_CTX_free(ctx)
  echo "PROBE OK: HTTP/3 binding layer links and initializes"
