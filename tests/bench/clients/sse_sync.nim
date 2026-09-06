## benchSse, sync backend (name: navi-sync). One blocking SSE subscription consuming
## events until the deadline, recording per-event inter-arrival latency + bytes.

import std/[times, monotimes]
import ../common/[config, reporter, servers, runner]
import navi
include ../common/httpset

proc sseThread(a: ptr BenchThread) {.thread, nimcall.} =
  # One blocking navi client per thread; navi keeps no shared mutable globals, so any
  # gcsafe complaints are false positives from indirect callbacks.
  {.gcsafe.}:
    let cfg = a.cfg
    var pool = initServerPool(cfg)
    var c = initNaviConfig()
    c.http = httpVersions(cfg.proto)
    c.tls.caFile = cfg.cert
    let api = newNavi(c)
    let rec = newBenchRecorder()
    var gate = initVersionGate(cfg)
    var last = getMonoTime()
    try:
      let s = api.sse(pool.pick() & "/events")
      s.each(ev):
        gate.sample(s.httpVersion)
        let now = getMonoTime()
        if epochTime() >= a.measureStart:
          rec.record((now - last).inMicroseconds, ev.data.len)
        last = now
        if epochTime() >= a.deadline: break
      s.close()
    except CatchableError:
      rec.fail()
    gate.finish()
    a.rec = rec

proc main() =
  let cfg = loadConfig("sync")
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  runThreaded(cfg, "navi-sync", sseThread)

main()
