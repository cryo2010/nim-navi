## Mutual-TLS (client certificate) interop against `openssl s_server -Verify`.
##
## Driven by tests/interop/mtls.sh, which generates a CA plus server and client
## certs, starts an OpenSSL server that *requires* a client certificate, and
## exports NAVI_MTLS_URL / NAVI_MTLS_CA / NAVI_MTLS_CERT / NAVI_MTLS_KEY.
## Validates that navi presents its client cert (handshake succeeds) and that
## omitting it is rejected by the server.

import unittest
import std/os
import navi

let
  base = getEnv("NAVI_MTLS_URL")
  ca = getEnv("NAVI_MTLS_CA")
  cert = getEnv("NAVI_MTLS_CERT")
  key = getEnv("NAVI_MTLS_KEY")

suite "mTLS interop (sync, client certificate)":
  test "presents a client certificate and completes the handshake":
    var cfg = newNaviConfig()
    cfg.tls.caFile = ca
    cfg.tls.certFile = cert
    cfg.tls.keyFile = key
    cfg.throwHttpErrors = false
    let api = newNavi(cfg)
    let res = api.get(base & "/")
    check res.status == 200        # openssl s_server -www answers 200

  test "a client without a certificate is rejected at the handshake":
    var cfg = newNaviConfig()
    cfg.tls.caFile = ca
    let api = newNavi(cfg)
    var rejected = false
    try:
      discard api.get(base & "/")
    except CatchableError:
      rejected = true              # server aborts the TLS handshake (no client cert)
    check rejected
