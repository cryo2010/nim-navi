## Bounded HTTPS request loop for Valgrind memcheck (see tests/valgrind/run.sh
## and the Docker image). Exercises the OpenSSL client path -- context, session,
## handshake, teardown -- that getOccupiedMem and the codec LeakSanitizer target
## cannot see. Kept small because Valgrind runs ~20-50x slower than native.
##
## `api.close()` drains the pool so Valgrind sees a clean shutdown. The guards
## below make a misconfigured run (missing env, zero iterations, a request that
## did not complete) fail loudly rather than pass with a leak-free empty run.

import std/[os, strutils]
import navi

const minIters = 10   # enough churn to matter; guards against a no-op run

let
  url = getEnv("NAVI_VG_URL")
  cert = getEnv("NAVI_VG_CERT")
  iters = parseInt(getEnv("NAVI_VG_ITERS", "50"))

doAssert url.len > 0 and cert.len > 0, "NAVI_VG_URL and NAVI_VG_CERT must be set"
doAssert iters >= minIters, "NAVI_VG_ITERS must be >= " & $minIters & " (got " & $iters & ")"

proc exercise(): int =
  ## Returns the number of requests that completed with 200.
  var cfg = newNaviConfig()
  cfg.tls.caFile = cert
  let api = newNavi(cfg)
  for _ in 0 ..< iters:
    let r = api.get(url)
    doAssert r.status == 200, "unexpected status " & $r.status
    inc result
  api.close()            # drain pooled connections for a leak-clean shutdown

proc exerciseFailedHandshake(): int =
  ## Verification on but no CA, so the server's self-signed cert is rejected and
  ## connect() raises during the TLS handshake. The SSL_CTX it allocated must be
  ## freed on that path too -- a regression guard for the connect-cleanup defer.
  var cfg = newNaviConfig()
  cfg.maxRetries = 0   # verify on (default), no caFile -> cert untrusted; one attempt
  let api = newNavi(cfg)
  for _ in 0 ..< iters:
    var raised = false
    try: discard api.get(url)
    except CatchableError: raised = true
    doAssert raised, "expected the untrusted-cert handshake to fail"
    inc result
  api.close()

let completed = exercise()
doAssert completed == iters,
  "only " & $completed & " of " & $iters & " requests completed"
let refused = exerciseFailedHandshake()
doAssert refused == iters, "only " & $refused & " of " & $iters & " handshakes failed"
GC_fullCollect()
echo "completed ", completed, " https requests + ", refused, " rejected handshakes"
