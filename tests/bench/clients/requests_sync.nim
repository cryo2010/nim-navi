## benchRequests, sync backend (`import navi`; name: navi-sync). Single in-flight per
## thread by nature (sync has no fan-out), scaled across cores with one navi client
## per THREAD: each thread loops GET/POST/PUT at /echo across the pool until the
## deadline, timing each measured request; the process merges the threads into one
## RESULT. Same warmup + cold-mode + fail-hard as the async client.

import std/[times, monotimes]
import ../zlibcodec
import ../common/[config, reporter, servers, runner]
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

proc reqThread(a: ptr BenchThread) {.thread, nimcall.} =
  # One blocking navi client per thread; navi keeps no shared mutable globals, so the
  # gcsafe complaints are false positives from indirect callbacks (middleware).
  {.gcsafe.}:
    let cfg = a.cfg
    var pool = initServerPool(cfg)
    let api = mkClient(cfg)

    let expect = cfg.expectedVersion
    if expect.len > 0:
      for base in pool.all():
        for _ in 0 ..< 3:
          try:
            if api.request(GET, base & "/echo").httpVersion == expect: break
          except CatchableError: break

    let rec = newBenchRecorder()
    var n = a.id                          # stagger the verb rotation across threads
    while epochTime() < a.deadline:
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
        if epochTime() >= a.measureStart:
          rec.record((getMonoTime() - t0).inMicroseconds)
      except CatchableError as e:
        rec.fail()
        stderr.writeLine cfg.label & " FAIL: " & $v & " " & url & " -> " &
          $e.name & ": " & e.msg
        quit(1)
    a.rec = rec

proc main() =
  let cfg = loadConfig("sync")
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  runThreaded(cfg, "navi-sync", reqThread)

main()
