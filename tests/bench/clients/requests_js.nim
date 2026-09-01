## benchRequests, navi/js backend (Node; name: navi-js). `clients` x `concurrency`
## workers fire GET/POST/PUT at /echo across the pool, timing each measured request.
## Request-body compression is skipped (the js runtime owns its codec). Warmup +
## time-box + fail-hard mirror the native client. Cert trusted via NODE_EXTRA_CA_CERTS.

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
  let rec = newJsBench()
  let label = "[requests " & cfg.proto & " js]"
  var apis: seq[Navi]
  for _ in 0 ..< cfg.clients:
    var c = initNaviConfig()
    c.middleware = @[stampMw()]
    apis.add newNavi(c)

  let startMs = nowMs()
  let measureStartMs = startMs + cfg.warmupSeconds * 1000.0
  let deadlineMs = measureStartMs + cfg.seconds * 1000.0

  proc worker(api: Navi, i: int) {.async.} =
    var n = i
    while nowMs() < deadlineMs:
      let v = verbs[n mod verbs.len]; inc n
      var h = initHeaders()
      if cfg.cold: h["connection"] = "close"
      var body = ""
      if v in {POST, PUT}:
        body = "payload-" & $v
        h["content-type"] = "text/plain"
      let t0 = nowUs()
      try:
        let res = await api.request(v, pool.pick() & "/echo", headers = h, body = body)
        discard res
        if nowMs() >= measureStartMs: rec.record(nowUs() - t0)
      except CatchableError as e:
        rec.note()
        echo label, " FAIL: ", $v, " -> ", e.name, ": ", e.msg
        jsExit(1)

  var futs: seq[Future[void]]
  for api in apis:
    for i in 0 ..< cfg.concurrency:
      futs.add worker(api, i)
  for f in futs: await f

  rec.emitResult("navi-js", cfg.seconds)

discard main()
