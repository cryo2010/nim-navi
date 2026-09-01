## benchSse, sync backend (name: navi-sync). One blocking SSE subscription consuming
## events until the deadline, recording per-event inter-arrival latency + bytes.

import std/[times, monotimes]
import ../common/[config, reporter, servers]
import navi
include ../common/httpset

proc main() =
  let cfg = loadConfig("sync")
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  var pool = initServerPool(cfg)
  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)
  c.tls.caFile = cfg.cert
  let api = newNavi(c)

  let rec = newBenchRecorder()
  let start = epochTime()
  let measureStart = start + cfg.warmupSeconds
  let deadline = measureStart + cfg.seconds
  var gate = initVersionGate(cfg)
  var last = getMonoTime()
  try:
    let s = api.sse(pool.pick() & "/events")
    s.each(ev):
      gate.sample(s.httpVersion)
      let now = getMonoTime()
      if epochTime() >= measureStart:
        rec.record((now - last).inMicroseconds, ev.data.len)
      last = now
      if epochTime() >= deadline: break
    s.close()
  except CatchableError:
    rec.fail()
  gate.finish()

  emitResult("navi-sync", rec, cfg.seconds)

main()
