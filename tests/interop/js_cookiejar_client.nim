## navi/js cookie jar, run under Node against a small HTTP server that sets a
## cookie and echoes the Cookie header it receives (see js_cookiejar.sh). On Node
## (no browser cookie store) the jar is on by default, so the cookie set on
## request 1 is replayed on request 2; forcing cookieJar off disables that.

import navi/js

const url = "http://127.0.0.1:9521/"

proc autoReplays(): Future[bool] {.async.} =
  let api = newNavi()                       # no config: auto-on on Node/undici
  discard await api.get(url)                # server sends Set-Cookie: sid=abc123
  let r2 = await api.get(url)               # jar should attach it
  return r2.body == "cookie:sid=abc123"

proc forcedOffDoesNotPersist(): Future[bool] {.async.} =
  let api = newNavi(NaviOptions(cookieJar: some(false)))
  discard await api.get(url)
  let r2 = await api.get(url)
  return r2.body == "cookie:none"

proc main() {.async.} =
  let replayed = await autoReplays()
  let notPersisted = await forcedOffDoesNotPersist()
  doAssert replayed, "on Node the jar should be on by default and replay the cookie"
  doAssert notPersisted, "cookieJar: some(false) should disable persistence"
  echo "OK: navi/js jar is auto-on on Node; some(false) disables it"

discard main()
