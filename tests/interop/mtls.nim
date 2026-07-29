## Mutual-TLS client-certificate interop across every credential format navi
## accepts, on the OpenSSL backends. Driven by mtls.sh, which stands up an
## `openssl s_server -Verify 1` that *requires* a client certificate and exports
## the same credential as PEM files, an encrypted PEM key, a PKCS#12 bundle, DER
## files, and (read here) in-memory PEM. Built two ways:
##   nim c ...              -> navi (sync)
##   nim c -d:useAsync ...  -> navi/asyncdispatch
## chronos (BearSSL) and js do not present client certificates, so they are out
## of scope here.

import std/[os, strutils]
when defined(useAsync):
  import navi/asyncdispatch
  const backend = "asyncdispatch"
else:
  import navi
  template await(x: untyped): untyped = x
  const backend = "sync"

proc mtlsCfg(): NaviConfig =
  ## A config that trusts the test CA but presents no client cert yet.
  result = initNaviConfig()
  result.tls.caFile = getEnv("NAVI_MTLS_CA")
  result.throwHttpErrors = false

template runAll() =
  var passed = 0
  var failures: seq[string]
  let base = getEnv("NAVI_MTLS_URL")

  template check(name: string, cond: untyped) =
    block:
      try:
        if cond: inc passed
        else: (failures.add name; echo "FAIL ", name)
      except CatchableError as e:
        failures.add name & " [" & e.msg & "]"
        echo "FAIL ", name, "  (", e.msg, ")"

  # -www answers 200; reaching it at all means the client cert was accepted.
  check "PEM cert and key files":
    var cfg = mtlsCfg()
    cfg.tls.certFile = getEnv("NAVI_MTLS_CERT")
    cfg.tls.keyFile = getEnv("NAVI_MTLS_KEY")
    (await initNavi(cfg).get(base & "/")).status == 200

  check "encrypted PEM key with a passphrase":
    var cfg = mtlsCfg()
    cfg.tls.certFile = getEnv("NAVI_MTLS_CERT")
    cfg.tls.keyFile = getEnv("NAVI_MTLS_ENCKEY")
    cfg.tls.password = getEnv("NAVI_MTLS_PASS")
    (await initNavi(cfg).get(base & "/")).status == 200

  check "wrong key passphrase is rejected":
    var cfg = mtlsCfg()
    cfg.tls.certFile = getEnv("NAVI_MTLS_CERT")
    cfg.tls.keyFile = getEnv("NAVI_MTLS_ENCKEY")
    cfg.tls.password = "not-the-password"
    var raised = false
    try: discard await initNavi(cfg).get(base & "/")
    except CatchableError: raised = true
    raised

  check "PKCS#12 / PFX bundle":
    var cfg = mtlsCfg()
    cfg.tls.pkcs12File = getEnv("NAVI_MTLS_P12")
    cfg.tls.password = getEnv("NAVI_MTLS_PASS")
    (await initNavi(cfg).get(base & "/")).status == 200

  check "DER-encoded cert and key (auto-detected)":
    var cfg = mtlsCfg()
    cfg.tls.certFile = getEnv("NAVI_MTLS_DERCERT")
    cfg.tls.keyFile = getEnv("NAVI_MTLS_DERKEY")
    (await initNavi(cfg).get(base & "/")).status == 200

  check "in-memory PEM cert and key strings":
    var cfg = mtlsCfg()
    cfg.tls.certPem = readFile(getEnv("NAVI_MTLS_CERT"))
    cfg.tls.keyPem = readFile(getEnv("NAVI_MTLS_KEY"))
    (await initNavi(cfg).get(base & "/")).status == 200

  check "a client with no certificate is not served":
    # The server (-Verify 1) refuses without a client cert. Sync surfaces that as
    # a handshake exception; asyncdispatch may instead yield an empty, non-200
    # response -- either way the page is not served.
    var ok = false
    try:
      ok = (await initNavi(mtlsCfg()).get(base & "/")).status != 200
    except CatchableError:
      ok = true
    ok

  echo "mTLS interop [", backend, "]: ", passed, " passed, ", failures.len, " failed"
  if failures.len > 0:
    for f in failures: echo "  - ", f
    quit(1)

when defined(useAsync):
  proc main() {.async.} = runAll()
  waitFor main()
else:
  proc main() = runAll()
  main()
