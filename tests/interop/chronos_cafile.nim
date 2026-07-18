## Custom-CA (TlsConfig.caFile) interop for the chronos/BearSSL backend.
##
## Driven by tests/interop/chronos_cafile.sh, which generates a CA, signs a
## server cert with it, starts an OpenSSL HTTPS server, and exports
## NAVI_CAFILE_URL / NAVI_CAFILE_WSS / NAVI_CAFILE_CA. Validates that navi/chronos
## verifies the server against the supplied CA, and that the same server is
## rejected when it falls back to BearSSL's bundled Mozilla anchors.

import unittest
import std/os
import pkg/chronos
import navi/chronos

let
  base = getEnv("NAVI_CAFILE_URL")   # https://127.0.0.1:port
  wss = getEnv("NAVI_CAFILE_WSS")    # wss://127.0.0.1:port (same server)
  ca = getEnv("NAVI_CAFILE_CA")

proc statusWithCa(url, caFile: string): Future[int] {.async.} =
  let api = newNavi(NaviOptions(
    tls: TlsConfig(verify: some(true), caFile: caFile),
    throwHttpErrors: some(false)))
  (await api.get(url)).status

proc tlsRejectedWithoutCa(url: string): Future[bool] {.async.} =
  ## verify:true with no custom CA: our CA is not among BearSSL's Mozilla anchors,
  ## so the handshake to this server must be rejected. Checked over wss because
  ## TLS trust is decided before any application data, so a rejected handshake
  ## surfaces immediately. (The plain-HTTP path can instead hang on the resulting
  ## dead connection -- navi's `timeout` does not yet bound a failed chronos
  ## handshake, tracked separately.)
  let api = newNavi(NaviOptions(tls: TlsConfig(verify: some(true))))
  try:
    let ws = await api.websocket(url)
    await ws.close()
    return false                    # handshake unexpectedly succeeded
  except CatchableError:
    return true                     # TLS verify error -> rejected

suite "chronos custom-CA (caFile) interop":
  test "verifies the server against a custom CA and completes the handshake":
    check waitFor(statusWithCa(base & "/", ca)) == 200   # openssl s_server -www answers 200

  test "the same server is rejected without the custom CA (default anchors)":
    check waitFor tlsRejectedWithoutCa(wss)
