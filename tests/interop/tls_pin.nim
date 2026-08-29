## In-memory CA bundle, SPKI certificate pinning, and the custom verify callback
## on the sync (OpenSSL) backend.
##
## Driven by tests/interop/tls_pin.sh, which generates a private CA, signs a
## server cert with it, starts an OpenSSL HTTPS server, and exports NAVI_TLS_URL,
## NAVI_TLS_CA (the CA PEM path) and NAVI_TLS_PIN (the server's base64 SPKI pin).
## The server connects to the 127.0.0.1 literal, so chain verification (not the
## hostname match, which is skipped for IP literals) is what is exercised.
import unittest
import std/os
import navi

let
  base = getEnv("NAVI_TLS_URL")
  caPem = readFile(getEnv("NAVI_TLS_CA"))
  goodPin = getEnv("NAVI_TLS_PIN")

proc get(cfg: NaviConfig, url: string): Response =
  let api = newNavi(cfg)
  defer: api.close()
  api.get(url)

proc rejects(cfg: NaviConfig, url: string): bool =
  let api = newNavi(cfg)
  defer: api.close()
  try:
    discard api.get(url); false
  except CatchableError:
    true

proc base0(): NaviConfig =
  result = initNaviConfig()
  result.throwHttpErrors = false
  result.retry.limit = 0

suite "in-memory CA bundle (caBundle)":
  test "an in-memory CA bundle should verify the server":
    var cfg = base0()
    cfg.tls.caBundle = caPem
    check cfg.get(base & "/").status == 200

  test "an unrelated in-memory CA bundle should reject the server":
    var cfg = base0()
    cfg.tls.caBundle = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
    # Malformed/empty-of-the-right-CA bundle: server chain does not anchor here and
    # the private root is not in the system store, so the handshake is rejected.
    check cfg.rejects(base & "/")

suite "SPKI certificate pinning (pinnedKeys)":
  test "a matching SPKI pin should complete the handshake":
    var cfg = base0()
    cfg.tls.caBundle = caPem
    cfg.tls.pinnedKeys = @[goodPin]
    check cfg.get(base & "/").status == 200

  test "a non-matching SPKI pin should reject the connection":
    var cfg = base0()
    cfg.tls.caBundle = caPem
    cfg.tls.pinnedKeys = @["AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="]
    check cfg.rejects(base & "/")

  test "a pin set including the right pin among wrong ones should pass":
    var cfg = base0()
    cfg.tls.caBundle = caPem
    cfg.tls.pinnedKeys = @["AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", goodPin]
    check cfg.get(base & "/").status == 200

suite "custom verify callback (verifyCallback)":
  test "a callback that accepts should complete the handshake":
    var cfg = base0()
    cfg.tls.caBundle = caPem
    cfg.tls.verifyCallback = proc(leafDer: string): bool =
      leafDer.len > 0     # a real DER cert was passed through
    check cfg.get(base & "/").status == 200

  test "a callback that rejects should reject the connection":
    var cfg = base0()
    cfg.tls.caBundle = caPem
    cfg.tls.verifyCallback = proc(leafDer: string): bool = false
    check cfg.rejects(base & "/")

  test "the callback should run even with chain verification disabled":
    var cfg = base0()
    cfg.tls.verify = false                # no chain/hostname check
    var sawCert = false
    cfg.tls.verifyCallback = proc(leafDer: string): bool =
      sawCert = leafDer.len > 0
      sawCert
    check cfg.get(base & "/").status == 200
    check sawCert
