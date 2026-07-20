## navi/js cookie jar, run under Node against a small HTTP server that sets a
## cookie and echoes the Cookie header it receives (see js_cookiejar.sh). Off a
## browser (Node here), navi keeps its own jar automatically, so the cookie set
## on request 1 is replayed on request 2. No configuration.

import navi/js

const url = "http://127.0.0.1:9521/"

proc twoRequests(): Future[(string, string)] {.async.} =
  let api = newNavi()                       # no config: jar is kept off-browser
  let r1 = await api.get(url)               # nothing stored yet
  let r2 = await api.get(url)               # Set-Cookie from r1 is replayed
  return (r1.body, r2.body)

proc main() {.async.} =
  let (first, second) = await twoRequests()
  doAssert first == "cookie:none", "the first request should send no cookie yet"
  doAssert second == "cookie:sid=abc123", "the jar should replay the cookie on Node"
  echo "OK: navi/js keeps cookies automatically on Node"

discard main()
