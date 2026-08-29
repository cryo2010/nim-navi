## SOCKS5 proxy support on the async backends. Built twice (asyncdispatch and, with
## -d:useChronos, chronos) so both per-backend SOCKS handshakes are exercised.
## Driven by tests/interop/socks5.sh (same env as the sync socks5.nim).
import std/os
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"

proc client(proxy: string): Navi =
  var cfg = initNaviConfig()
  cfg.proxy = proxy
  cfg.throwHttpErrors = false
  cfg.retry.limit = 0
  newNavi(cfg)

proc main() {.async.} =
  # Read env locally (not module-level globals) so the closure stays GC-safe under
  # the chronos async macro.
  let
    target = getEnv("NAVI_SOCKS_TARGET")
    noAuth = getEnv("NAVI_SOCKS_NOAUTH")
    auth = getEnv("NAVI_SOCKS_AUTH")
    authBad = getEnv("NAVI_SOCKS_AUTH_BAD")
  block:
    let api = client(noAuth)
    let r = await api.get(target)
    doAssert r.status == 200, backend & " no-auth: status " & $r.status
    await api.close()
  block:
    let api = client(auth)
    let r = await api.get(target)
    doAssert r.status == 200, backend & " auth: status " & $r.status
    await api.close()
  block:
    let api = client(authBad)
    var rejected = false
    try: discard await api.get(target)
    except CatchableError: rejected = true
    doAssert rejected, backend & " wrong-creds: expected rejection"
    await api.close()
  echo "== ", backend, ": SOCKS5 no-auth + auth + wrong-creds OK =="

waitFor main()
