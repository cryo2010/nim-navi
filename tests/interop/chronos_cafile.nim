## Custom-CA (TlsConfig.caFile) interop for the chronos backend.
##
## Driven by tests/interop/chronos_cafile.sh, which generates a CA, signs a
## server cert with it, starts an OpenSSL HTTPS server, and exports
## NAVI_CAFILE_URL / NAVI_CAFILE_CA. Validates that navi/chronos verifies the
## server against the supplied CA, and that the same server is rejected when it
## falls back to the default system trust store (chronos now runs OpenSSL).

import unittest
import std/[os, strutils]
import pkg/chronos
import navi/chronos

let
  base = getEnv("NAVI_CAFILE_URL")   # https://127.0.0.1:port
  ca = getEnv("NAVI_CAFILE_CA")

proc statusWithCa(url, caFile: string): Future[int] {.async.} =
  var cfg = initNaviConfig()
  cfg.tls.caFile = caFile
  cfg.throwHttpErrors = false
  let api = newNavi(cfg)
  (await api.get(url)).status

proc rejectedWithoutCa(url: string): Future[bool] {.async.} =
  ## verify:true with no custom CA: our test CA is not in the default system trust
  ## store, so the handshake is rejected at connect.
  let api = newNavi(initNaviConfig())   # verify on (default), no caFile
  try:
    discard await api.get(url)
    return false                    # handshake unexpectedly succeeded
  except CatchableError:
    return true                     # TLS verify error -> rejected

proc errorWithoutCa(url: string, connectMs: int): Future[string] {.async.} =
  ## The message navi surfaces for a server whose chain does not verify, with a
  ## connect timeout armed. chronos's `withTimeout` completes `true` when the inner
  ## future FAILED (asyncfutures `completeFuture`), so `connect` used to discard the
  ## TLS error and return a half-built Conn; the caller then saw a generic "send on
  ## a closed connection" from `sendAll` and the engine reclassified it as a
  ## keep-alive race (#420).
  var cfg = initNaviConfig()
  cfg.timeouts.connect = connectMs
  cfg.retry.limit = 0
  let api = newNavi(cfg)
  try:
    discard await api.get(url)
    return ""                       # handshake unexpectedly succeeded
  except CatchableError as e:
    return e.msg

suite "chronos custom-CA (caFile) interop":
  test "verifies the server against a custom CA and completes the handshake":
    check waitFor(statusWithCa(base & "/", ca)) == 200   # openssl s_server -www answers 200

  test "the same server is rejected without the custom CA (default anchors)":
    check waitFor rejectedWithoutCa(base & "/")

  test "a connect timeout must not swallow the TLS failure":
    let msg = waitFor errorWithoutCa(base & "/", 5000)
    check msg.len > 0                             # it must still be rejected
    check "closed connection" notin msg           # the generic symptom of a lost error
    check ("TLS" in msg or "certificate" in msg)  # the real reason reaches the caller
