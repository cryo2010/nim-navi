## benchSse, async backends (navi-async / navi-chronos). Opens `clients` x
## `concurrency` SSE subscriptions across the pool and consumes events until the
## deadline, recording per-event inter-arrival latency + bytes (events/s throughput
## and MB/s) over a timed window after warmup. The version gate hard-fails if the
## pinned protocol is never negotiated (h3 SSE begins on h2 until the Alt-Svc upgrade).

import std/[times, monotimes]
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

proc worker(s: SseStream, rec: BenchRecorder, gate: ptr VersionGate,
            measureStart, deadline: float) {.async.} =
  var last = getMonoTime()
  try:
    s.each(ev):
      gate[].sample(s.httpVersion)
      let now = getMonoTime()
      if epochTime() >= measureStart:
        rec.record((now - last).inMicroseconds, ev.data.len)
      last = now
      if epochTime() >= deadline: break
  except CatchableError:
    rec.fail()
  try: await s.close()                     # closed here, not mid-read: clean teardown
  except CatchableError: discard

proc mkClient(cfg: Config): Navi =
  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)
  c.tls.caFile = cfg.cert
  newNavi(c)

proc sseThread(a: ptr BenchThread) {.thread, nimcall.} =
  # One navi client per thread on its own event loop; navi keeps no shared mutable
  # globals, so the gcsafe complaints are false positives from indirect callbacks.
  {.gcsafe.}:
    let cfg = a.cfg
    var pool = initServerPool(cfg)
    let api = mkClient(cfg)
    var streams: seq[SseStream]
    for _ in 0 ..< a.concurrency:
      streams.add waitFor api.sse(pool.pick() & "/events", retryMs = 20, maxRetryMs = 100)
    let rec = newBenchRecorder()
    var gate = initVersionGate(cfg)
    var futs: seq[Future[void]]
    for s in streams: futs.add worker(s, rec, addr gate, a.measureStart, a.deadline)
    for f in futs: waitFor f
    gate.finish()
    a.rec = rec

proc main() =
  let cfg = loadConfig(backend)
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  runThreaded(cfg, clientName, sseThread)

main()
