## stressRequests, navi/js backend (Node). `clients` x `concurrency` workers fire
## GET/POST/PUT at /echo across the server pool, tally status codes, and discard
## responses. Request-body compression is skipped (the js runtime owns its codec).
## The runner trusts the self-signed cert via NODE_EXTRA_CA_CERTS.

import navi/js
import ../common/harness_js

proc jsExit(code: int) {.importjs: "process.exit(#)".}

const verbs = [GET, POST, PUT]

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
      let v = verbs[n mod verbs.len]; inc n
      var h = initHeaders()
      var body = ""
      if v in {POST, PUT}:
        body = "payload-" & $v
        h["content-type"] = "text/plain"
      try:
        let res = await api.request(v, pool.pick() & "/echo", headers = h, body = body)
        counter.tally(res.status)
      except CatchableError as e:
        counter.note()
        # Fail hard: a surfaced transport error is a bug to investigate, not noise.
        echo label, " FAIL: ", $v, " -> ", e.name, ": ", e.msg
        jsExit(1)

  var futs: seq[Future[void]]
  for api in apis:
    for i in 0 ..< cfg.concurrency:
      futs.add worker(api, i)
  for f in futs: await f
  clearIntervalJs(timer)

  counter.report(label, start)
  echo "== requests js ", cfg.proto, " passed (", counter.ops, " ops) =="

discard main()
