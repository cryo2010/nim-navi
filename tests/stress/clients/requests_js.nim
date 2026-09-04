## stressRequests, navi/js backend (Node). `clients` x `concurrency` workers fire
## GET/POST/PUT at /echo across the server pool, tally status codes, and discard
## responses. Request-body compression is skipped (the js runtime owns its codec).
## The runner trusts the self-signed cert via NODE_EXTRA_CA_CERTS.

import std/strutils
import navi/js
import ../common/harness_js

const verbs = [GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS]
# Text-only body shapes (js sends the body as a string via TextEncoder, so a raw
# binary body would be mangled): empty, tiny, either side of the 16 KiB frame
# boundary, past the 64 KiB window, and highly compressible.
let jsBodies = @["", "payload", repeat("a", 16383), repeat("b", 16385),
                 repeat("c", 65536), repeat("z", 262144)]

proc stampMw(): NaviMiddleware =
  result = proc(ctx: NaviContext) {.async.} =
    ctx.req.headers["x-stress"] = "1"
    await ctx.next()

proc main() {.async.} =
  let cfg = loadJsCfg()
  var pool = initJsPool(cfg)
  let counter = newJsCounter()
  var apis: seq[Navi]
  for _ in 0 ..< cfg.clients:
    var c = initNaviConfig()
    c.middleware = @[stampMw()]
    apis.add newNavi(c)

  let start = nowMs()
  let deadline = start + cfg.seconds * 1000.0
  let label = "[requests " & cfg.proto & " js]"
  let timer = setIntervalJs(proc () = counter.report(label, start),
                            cfg.reportSeconds * 1000)

  proc worker(api: Navi, i: int) {.async.} =
    var n = i
    while nowMs() < deadline:
      let v = verbs[n mod verbs.len]
      let plain = jsBodies[n mod jsBodies.len]
      let bodied = v in {POST, PUT, PATCH}
      inc n
      var h = initHeaders()
      if bodied: h["content-type"] = "text/plain"
      try:
        let res = await api.request(v, pool.pick() & "/echo", headers = h,
                                    body = (if bodied: plain else: ""))
        if res.status != 200:
          echo label, " FAIL: ", $v, " -> status ", res.status; jsExit(1)
        if res.headers.get("x-echo-method") != $v:
          echo label, " FAIL: ", $v, " echoed method '", res.headers.get("x-echo-method"), "'"; jsExit(1)
        if res.headers.get("x-echo-stress") != "1":
          echo label, " FAIL: ", $v, " middleware header not echoed"; jsExit(1)
        let expectBody = if bodied and v != HEAD: plain else: ""
        if res.body != expectBody:
          echo label, " FAIL: ", $v, " body mismatch (expected ", expectBody.len,
               " got ", res.body.len, ")"; jsExit(1)
        counter.tally(res.status)
      except CatchableError as e:
        counter.note()
        echo label, " FAIL: ", $v, " -> ", e.name, ": ", e.msg
        jsExit(1)

  var futs: seq[Future[void]]
  for api in apis:
    for i in 0 ..< cfg.concurrency:
      futs.add worker(api, i)
  for f in futs: await f
  clearIntervalJs(timer)

  if counter.ops == 0:
    echo label, " FAIL: no request completed"
    jsExit(1)
  counter.report(label, start)
  echo "== requests js ", cfg.proto, " passed (", counter.ops, " ops) =="

discard main()
