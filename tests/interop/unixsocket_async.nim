## Unix domain socket transport on the async backends. Built twice (asyncdispatch
## and, with -d:useChronos, chronos). Driven by tests/interop/unixsocket.sh.
import std/os
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"

proc main() {.async.} =
  let sock = getEnv("NAVI_UDS_PATH")     # read locally: GC-safe under chronos
  var cfg = initNaviConfig()
  cfg.unixSocket = sock
  cfg.throwHttpErrors = false
  cfg.retry.limit = 0
  let api = newNavi(cfg)
  let r = await api.get("http://example.test/")
  doAssert r.status == 200, backend & ": status " & $r.status
  doAssert r.body == "example.test", backend & ": Host echo " & r.body
  await api.close()
  echo "== ", backend, ": Unix socket round trip + Host header OK =="

waitFor main()
