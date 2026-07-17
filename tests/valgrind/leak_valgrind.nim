## Bounded HTTPS request loop for Valgrind memcheck (see tests/valgrind/run.sh
## and the Docker image). Exercises the OpenSSL client path -- context, session,
## handshake, teardown -- that getOccupiedMem and the codec LeakSanitizer target
## cannot see. Kept small because Valgrind runs ~20-50x slower than native.
##
## If navi gains a client-close API, call it before exit so pooled connections
## are torn down and Valgrind sees a clean shutdown.

import std/[os, strutils]
import navi

let
  url = getEnv("NAVI_VG_URL")
  cert = getEnv("NAVI_VG_CERT")
  iters = parseInt(getEnv("NAVI_VG_ITERS", "50"))

proc exercise() =
  let api = newNavi(NaviOptions(tls: TlsConfig(verify: some(true), caFile: cert)))
  for _ in 0 ..< iters:
    let r = api.get(url)
    doAssert r.status == 200, "unexpected status " & $r.status
  when compiles(api.close()):
    api.close()          # drain pooled connections for a leak-clean shutdown

exercise()
GC_fullCollect()
echo "completed ", iters, " https requests"
