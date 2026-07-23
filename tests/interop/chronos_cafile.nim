## Custom-CA (TlsConfig.caFile) interop for the chronos/BearSSL backend.
##
## Driven by tests/interop/chronos_cafile.sh, which generates a CA, signs a
## server cert with it, starts an OpenSSL HTTPS server, and exports
## NAVI_CAFILE_URL / NAVI_CAFILE_CA. Validates that navi/chronos verifies the
## server against the supplied CA, and that the same server is rejected when it
## falls back to BearSSL's bundled Mozilla anchors.

import unittest
import std/os
import pkg/chronos
import navi/chronos

let
  base = getEnv("NAVI_CAFILE_URL")   # https://127.0.0.1:port
  ca = getEnv("NAVI_CAFILE_CA")

proc statusWithCa(url, caFile: string): Future[int] {.async.} =
  var cfg = newNaviConfig()
  cfg.tls.caFile = caFile
  cfg.throwHttpErrors = false
  let api = newNavi(cfg)
  (await api.get(url)).status

proc rejectedWithoutCa(url: string): Future[bool] {.async.} =
  ## verify:true with no custom CA: our CA is not among BearSSL's Mozilla anchors,
  ## so the handshake is rejected at connect.
  let api = newNavi(newNaviConfig())   # verify on (default), no caFile
  try:
    discard await api.get(url)
    return false                    # handshake unexpectedly succeeded
  except CatchableError:
    return true                     # TLS verify error -> rejected

suite "chronos custom-CA (caFile) interop":
  test "verifies the server against a custom CA and completes the handshake":
    check waitFor(statusWithCa(base & "/", ca)) == 200   # openssl s_server -www answers 200

  test "the same server is rejected without the custom CA (default anchors)":
    check waitFor rejectedWithoutCa(base & "/")
