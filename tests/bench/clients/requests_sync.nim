## benchRequests, sync backend (`import navi`; name: navi-sync). Single in-flight by
## nature: loops the clients round-robin firing GET/POST/PUT at /echo across the
## pool until the deadline, timing each measured request. Same warmup + cold-mode +
## fail-hard as the async client, minus the fan-out (sync has no concurrency).

import std/[times, monotimes]
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
  var apis: seq[Navi]
  for _ in 0 ..< cfg.clients: apis.add mkClient(cfg)

  let expect = cfg.expectedVersion
  if expect.len > 0:
    for api in apis:
      for base in pool.all():
        for _ in 0 ..< 3:
          try:
            if api.request(GET, base & "/echo").httpVersion == expect: break
          except CatchableError: break

  let rec = newBenchRecorder()
  let start = epochTime()
  let measureStart = start + cfg.warmupSeconds
  let deadline = measureStart + cfg.seconds
  var n = 0
  while epochTime() < deadline:
    for api in apis:
      let v = verbs[n mod verbs.len]; inc n
      let url = pool.pick() & "/echo"
      var h = initHeaders()
      if cfg.cold: h["connection"] = "close"
      var body = ""
      if v in {POST, PUT}:
        body = "payload-" & $v
        h["content-type"] = "text/plain"
        if cfg.reqCompression != "none":
          body = zcompress(body, cfg.reqCompression)
          h["content-encoding"] = cfg.reqCompression
        if cfg.respCompression != "none":
          h["x-want-encoding"] = cfg.respCompression
      let t0 = getMonoTime()
      try:
        let res = api.request(v, url, headers = h, body = body)
        cfg.checkVersion(res.httpVersion)
        if epochTime() >= measureStart:
          rec.record((getMonoTime() - t0).inMicroseconds)
      except CatchableError as e:
        rec.fail()
        stderr.writeLine cfg.label & " FAIL: " & $v & " " & url & " -> " &
          $e.name & ": " & e.msg
        quit(1)

  emitResult("navi-sync", rec, cfg.seconds)

main()
