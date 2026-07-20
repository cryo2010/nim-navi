## navi/js opt-in cookie jar, run under Node against a small HTTP server that
## sets a cookie and echoes the Cookie header it receives (see js_cookiejar.sh).
## With the jar on, the cookie set on request 1 is replayed on request 2; with
## the default (no jar), Node's fetch/undici keeps no store, so it is not.

import navi/js

const url = "http://127.0.0.1:9521/"

proc replaysWithJar(): Future[bool] {.async.} =
  let api = newNavi(NaviOptions(cookieJar: some(true)))
  discard await api.get(url)               # server sends Set-Cookie: sid=abc123
  let r2 = await api.get(url)              # jar should attach it
  return r2.body == "cookie:sid=abc123"

proc noPersistWithoutJar(): Future[bool] {.async.} =
  let api = newNavi()                       # default: no navi jar, undici has none
  discard await api.get(url)
  let r2 = await api.get(url)
  return r2.body == "cookie:none"

proc main() {.async.} =
  let replayed = await replaysWithJar()
  let notPersisted = await noPersistWithoutJar()
  doAssert replayed, "cookie jar should replay the cookie on the second request"
  doAssert notPersisted, "without the jar, undici should not persist cookies"
  echo "OK: navi/js cookie jar persists on Node; the default does not"

discard main()
