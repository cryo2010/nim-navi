## SOCKS5 proxy support on the sync backend.
##
## Driven by tests/interop/socks5.sh, which starts a plain HTTP origin and two
## SOCKS5 proxies (one no-auth, one requiring username/password) and exports the
## origin URL plus the three proxy URLs. Validates that navi tunnels through the
## no-auth proxy, authenticates to the auth proxy, and is rejected with wrong
## credentials.
import unittest
import std/os
import navi

let
  target = getEnv("NAVI_SOCKS_TARGET")     # http://127.0.0.1:port/
  noAuth = getEnv("NAVI_SOCKS_NOAUTH")     # socks5://127.0.0.1:port
  auth = getEnv("NAVI_SOCKS_AUTH")         # socks5://user:pass@127.0.0.1:port
  authBad = getEnv("NAVI_SOCKS_AUTH_BAD")  # socks5://user:wrong@127.0.0.1:port

proc statusVia(proxy: string): int =
  var cfg = initNaviConfig()
  cfg.proxy = proxy
  cfg.throwHttpErrors = false
  cfg.retry.limit = 0
  let api = newNavi(cfg)
  defer: api.close()
  api.get(target).status

proc rejectsVia(proxy: string): bool =
  var cfg = initNaviConfig()
  cfg.proxy = proxy
  cfg.retry.limit = 0
  let api = newNavi(cfg)
  defer: api.close()
  try:
    discard api.get(target); false
  except CatchableError:
    true

suite "SOCKS5 proxy (sync backend)":
  test "a request should tunnel through a no-auth SOCKS5 proxy":
    check statusVia(noAuth) == 200

  test "a request should authenticate to a user/pass SOCKS5 proxy":
    check statusVia(auth) == 200

  test "a request with wrong SOCKS5 credentials should be rejected":
    check rejectsVia(authBad)
