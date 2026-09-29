## HTTP CONNECT proxy tunnelling on the async backends. Built twice (asyncdispatch
## and, with -d:useChronos, chronos) so both per-backend read primitives are
## exercised by the CONNECT reply loop. Driven by tests/interop/http_connect.sh
## (same env as the sync http_connect.nim).
import std/[os, strutils]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"

proc client(proxy, ca: string): Navi =
  var cfg = initNaviConfig()
  cfg.proxy = proxy
  cfg.tls.caFile = ca
  cfg.throwHttpErrors = false
  cfg.retry.limit = 0
  newNavi(cfg)

proc main() {.async.} =
  # Read env locally (not module-level globals) so the closure stays GC-safe under
  # the chronos async macro.
  let
    target = getEnv("NAVI_CONNECT_TARGET")
    ca = getEnv("NAVI_CONNECT_CA")
    split = getEnv("NAVI_CONNECT_SPLIT")
    big = getEnv("NAVI_CONNECT_BIG")
    deny = getEnv("NAVI_CONNECT_DENY")
  for (label, proxy) in [("split", split), ("big", big)]:
    let api = client(proxy, ca)
    let r = await api.get(target)
    doAssert r.status == 200, backend & " " & label & ": status " & $r.status
    await api.close()
  block:
    let api = client(deny, ca)
    var msg = ""
    try: discard await api.get(target)
    except CatchableError as e: msg = e.msg
    doAssert "proxy CONNECT failed" in msg, backend & " 407: got " & msg
    doAssert "407" in msg, backend & " 407: status line missing: " & msg
    await api.close()
  echo "== ", backend, ": CONNECT split reply + oversized reply + 407 OK =="

waitFor main()
