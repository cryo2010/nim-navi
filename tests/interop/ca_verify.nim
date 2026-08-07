## Custom-CA (TlsConfig.caFile) verification on the sync (OpenSSL) backend.
##
## Driven by tests/interop/ca_verify.sh, which generates a CA, signs a server
## cert with it, starts an OpenSSL HTTPS server, and exports NAVI_CAFILE_URL /
## NAVI_CAFILE_CA. Validates that navi verifies the server against the supplied
## CA (positive), and rejects the same server when it falls back to the system
## trust store, which does not contain our private CA (negative).
import unittest
import std/os
import navi

let
  base = getEnv("NAVI_CAFILE_URL")   # https://127.0.0.1:port
  ca = getEnv("NAVI_CAFILE_CA")

proc statusWithCa(url, caFile: string): int =
  var cfg = initNaviConfig()
  cfg.tls.caFile = caFile
  cfg.throwHttpErrors = false
  cfg.retry.limit = 0
  let api = newNavi(cfg)
  result = api.get(url).status
  api.close()

proc rejectedWithoutCa(url: string): bool =
  ## verify:true with no custom CA: our CA is not in the system trust store, so
  ## the chain does not validate and the handshake is rejected at connect.
  var cfg = initNaviConfig()          # verify on (default), no caFile
  cfg.retry.limit = 0
  let api = newNavi(cfg)
  try:
    discard api.get(url)
    result = false                    # handshake unexpectedly succeeded
  except CatchableError:
    result = true                     # TLS verify error -> rejected
  api.close()

suite "sync custom-CA (caFile) verification":
  test "verifies the server against a custom CA and completes the handshake":
    check statusWithCa(base & "/", ca) == 200   # openssl s_server -www answers 200

  test "the same server is rejected without the custom CA (system trust)":
    check rejectedWithoutCa(base & "/")
