## benchRequests, async backends (one source, built twice):
##   nim c -d:ssl ...                -> navi/asyncdispatch  (name: navi-async)
##   nim c -d:ssl -d:useChronos ...  -> navi/chronos        (name: navi-chronos)
##
## Time-boxed buffered request/response throughput + latency: `clients` navi
## clients each fan out `concurrency` workers looping GET/POST/PUT at /echo across
## the server pool. An unmeasured `warmupSeconds` prelude precedes the measured
## `seconds` window; each measured request's wall time is recorded into a latency
## histogram. Cold mode forces a fresh connection per request (Connection: close).
## Emits one RESULT line; fails hard on any surfaced transport error.

import std/[times, monotimes]
import ../zlibcodec
import ../common/[config, reporter, servers, runner]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
  const clientName = "navi-chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"
  const clientName = "navi-async"
include ../common/httpset

const verbs = [GET, POST, PUT]

proc stampMw(): NaviMiddleware =
  result = proc(ctx: NaviContext) {.async.} =
    ctx.req.headers["x-stress"] = "1"
    await ctx.next()

proc mkClient(cfg: Config): Navi =
  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)
  c.tls.caFile = cfg.cert
  c.middleware = @[stampMw()]
  newNavi(c)

proc worker(api: Navi, cfg: Config, pool: ptr ServerPool, rec: BenchRecorder,
            measureStart, deadline: float, i: int) {.async.} =
  var n = i
  while epochTime() < deadline:
    let v = verbs[n mod verbs.len]; inc n
    let url = pool[].pick() & "/echo"
    var h = initHeaders()
    if cfg.cold: h["connection"] = "close"   # fresh connection per request (h1)
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
      let res = await api.request(v, url, headers = h, body = body)
      cfg.checkVersion(res.httpVersion)   # hard-fail on a silent protocol downgrade
      if epochTime() >= measureStart:      # skip the warmup window
        rec.record((getMonoTime() - t0).inMicroseconds)
    except CatchableError as e:
      rec.fail()
      stderr.writeLine cfg.label & " FAIL: " & $v & " " & url & " -> " &
        $e.name & ": " & e.msg
      quit(1)

proc reqThread(a: ptr BenchThread) {.thread, nimcall.} =
  # One navi client per thread on its own event loop; navi keeps no shared mutable
  # globals, so the gcsafe complaints are false positives from indirect callbacks.
  {.gcsafe.}:
    let cfg = a.cfg
    var pool = initServerPool(cfg)
    let api = mkClient(cfg)

    # Warm up per-origin protocol discovery (h3 needs an Alt-Svc round trip) so the
    # measured window is pinned to the negotiated protocol from the first request.
    let expect = cfg.expectedVersion
    if expect.len > 0:
      for base in pool.all():
        for _ in 0 ..< 3:
          try:
            if (waitFor api.request(GET, base & "/echo")).httpVersion == expect: break
          except CatchableError: break

    let rec = newBenchRecorder()
    var futs: seq[Future[void]]
    for i in 0 ..< a.concurrency:
      futs.add worker(api, cfg, addr pool, rec, a.measureStart, a.deadline, i)
    for f in futs: waitFor f
    a.rec = rec

proc main() =
  let cfg = loadConfig(backend)
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  runThreaded(cfg, clientName, reqThread)

main()
