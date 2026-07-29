## navi/js runtime checks, run under Node against a small HTTP server (see
## js_cookiejar.sh):
##   1. the cookie jar: off a browser navi keeps its own jar, so a cookie set on
##      request 1 is replayed on request 2.
##   2. middleware: a stamped header reaches the server. This guards a regression
##      where navi/js `request` built the middleware context with `unsafeAddr
##      client`, which miscompiles under `nim js` and broke the whole middleware
##      path at runtime (the `NaviContext` now holds the client by value).

import navi/js

const url = "http://127.0.0.1:9521/"

proc twoRequests(): Future[(string, string)] {.async.} =
  let api = initNavi()                       # no config: jar is kept off-browser
  let r1 = await api.get(url)               # nothing stored yet
  let r2 = await api.get(url)               # Set-Cookie from r1 is replayed
  return (r1.body, r2.body)

proc middlewareRuns(): Future[bool] {.async.} =
  var ran = 0
  proc stamp(): NaviMiddleware =
    result = proc(ctx: NaviContext) {.async.} =
      ctx.req.headers["x-stress"] = "1"
      inc ran
      await ctx.next()
  var cfg = initNaviConfig()
  cfg.middleware = @[stamp()]
  let api = initNavi(cfg)
  let r = await api.get(url)                 # pre-fix this threw: client was undefined
  doAssert ran == 1, "middleware did not run"
  doAssert r.headers.get("x-echo-stress") == "1",
    "server did not see the stamped header: " & r.headers.get("x-echo-stress")
  return true

proc main() {.async.} =
  let (first, second) = await twoRequests()
  doAssert first == "cookie:none", "the first request should send no cookie yet"
  doAssert second == "cookie:sid=abc123", "the jar should replay the cookie on Node"
  echo "OK: navi/js keeps cookies automatically on Node"
  discard await middlewareRuns()
  echo "OK: navi/js middleware runs and mutates the request"

discard main()
