## stressRequests, sync backend (`import navi`). The sync client is single
## in-flight by nature: it loops the clients round-robin, firing GET/POST/PUT at
## /echo across the server pool until the deadline. Same consume-and-discard +
## per-report cadence as the async client, minus the fan-out (documented: sync has
## no concurrency).

import std/times
import ../zlibcodec
import ../common/[config, reporter, servers]
import navi
include ../common/httpset

const verbs = [GET, POST, PUT]

proc stampMw(): NaviMiddleware =
  result = proc(ctx: NaviContext) =
    ctx.req.headers["x-stress"] = "1"
    ctx.next()

proc mkClient(cfg: Config): Navi =
  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)
  c.tls.caFile = cfg.cert
  c.middleware = @[stampMw()]
  newNavi(c)

proc main() =
  let cfg = loadConfig("sync")
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  var pool = initServerPool(cfg)
  let counter = newStatusCounter()
  var apis: seq[Navi]
  for _ in 0 ..< cfg.clients: apis.add mkClient(cfg)

  let start = epochTime()
  let deadline = start + cfg.seconds
  var lastReport = start
  var n = 0
  while epochTime() < deadline:
    for api in apis:
      let v = verbs[n mod verbs.len]; inc n
      let url = pool.pick() & "/echo"
      var h = initHeaders()
      var body = ""
      if v in {POST, PUT}:
        body = "payload-" & $v
        h["content-type"] = "text/plain"
        if cfg.reqCompression != "none":
          body = zcompress(body, cfg.reqCompression)
          h["content-encoding"] = cfg.reqCompression
        if cfg.respCompression != "none":
          h["x-want-encoding"] = cfg.respCompression
      try:
        let res = api.request(v, url, headers = h, body = body)
        counter.tally(res.status)
      except CatchableError:
        counter.fail()
    if epochTime() - lastReport >= cfg.reportSeconds.float:
      lastReport = epochTime()
      report(cfg.label, counter, epochTime() - start)

  report(cfg.label & " final", counter, epochTime() - start)
  echo "== requests sync ", cfg.proto, " passed (", counter.ops, " ops) =="

main()
