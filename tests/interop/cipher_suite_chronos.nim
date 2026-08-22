## Cipher-suite selection enforcement on the chronos backend. The chronos analog
## of cipher_suite.nim, driven by the same cipher_suite.sh servers (a TLS 1.2
## server pinned to one cipher, a TLS 1.3 server pinned to one ciphersuite). Now
## that chronos runs OpenSSL it honors `TlsConfig.ciphers` / `cipherSuites` exactly
## like the sync/asyncdispatch backends (BearSSL had a fixed profile and rejected
## these outright).
import std/[os, strutils]
import pkg/chronos
import navi/chronos

let p12 = parseInt(getEnv("NAVI_CS_PORT12"))   # TLS 1.2, cipher ECDHE-RSA-AES256-GCM-SHA384
let p13 = parseInt(getEnv("NAVI_CS_PORT13"))   # TLS 1.3, ciphersuite TLS_AES_256_GCM_SHA384

proc connects(port: int, ciphers = "", cipherSuites = ""): Future[bool] {.async.} =
  var cfg = initNaviConfig()
  cfg.tls.verify = false                        # self-signed test cert
  cfg.tls.ciphers = ciphers
  cfg.tls.cipherSuites = cipherSuites
  cfg.retry.limit = 0
  cfg.headers["connection"] = "close"
  let api = newNavi(cfg)
  try:
    result = (await api.get("https://127.0.0.1:" & $port & "/")).status == 200
  except CatchableError:
    result = false
  await api.close()

proc main() {.async.} =
  # TLS 1.2 cipher list, against a server that only offers ECDHE-RSA-AES256-GCM-SHA384.
  doAssert (await connects(p12, ciphers = "ECDHE-RSA-AES256-GCM-SHA384")),
    "a matching TLS 1.2 cipher should connect"
  doAssert not (await connects(p12, ciphers = "ECDHE-RSA-AES128-GCM-SHA256")),
    "a non-matching TLS 1.2 cipher must fail the handshake"
  echo "OK  ciphers (TLS 1.2) enforced (chronos): server accepts only AES256, client AES128 rejected"

  # TLS 1.3 ciphersuites, against a server that only offers TLS_AES_256_GCM_SHA384.
  doAssert (await connects(p13, cipherSuites = "TLS_AES_256_GCM_SHA384")),
    "a matching TLS 1.3 ciphersuite should connect"
  doAssert not (await connects(p13, cipherSuites = "TLS_AES_128_GCM_SHA256")),
    "a non-matching TLS 1.3 ciphersuite must fail the handshake"
  echo "OK  cipherSuites (TLS 1.3) enforced (chronos): server accepts only AES256, client AES128 rejected"

  echo "== cipher selection passed (chronos) =="

waitFor main()
