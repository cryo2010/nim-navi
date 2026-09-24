## stressSse, sync backend (`import navi`). One blocking SSE subscription consumes
## events (navi reconnects + resumes Last-Event-ID transparently as the server
## drops mid-stream) until the deadline. Reports inline between events.

import std/[times, strutils]
import ../common/[config, reporter, servers, leakcheck]
import navi
include ../common/httpset
include ../common/chaos

proc main() =
  let cfg = loadConfig("sync")
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  let notice = cfg.chaosSkipNotice
  if notice.len > 0: echo cfg.label, " ", notice
  var leakBase = sampleBaseline(cfg.chaos)   # before ANY Navi is constructed
  var pool = initServerPool(cfg)
  let counter = newStatusCounter()

  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)
  c.tls.caFile = cfg.cert
  let api = newNavi(c)

  let start = epochTime()
  let deadline = start + cfg.seconds
  var sc = syncChaosStart(cfg, leakBase)     # no-op when chaos is off
  var lastReport = start
  # Interleave a chaos interaction on its OWN cadence, NOT the report cadence:
  # NAVI_REPORT_SECONDS defaults to 60 while a smoke cell runs 20s, so pinning chaos
  # to the report interval means the report branch (and thus every chaos step) never
  # fires in a short cell -- syncChaosFinish then hard-fails "no chaos interaction
  # completed". Events flow continuously here, so a fixed few-second cadence, capped
  # to the cell length, guarantees several interactions regardless of the report knob.
  let chaosEvery = min(2.0, max(0.5, cfg.seconds / 4.0))
  var lastChaos = start
  var gate = initVersionGate(cfg)
  var lastId = 0
  try:
    let s = api.sse(pool.pick() & "/events")
    s.each(ev):
      counter.tally(200)
      gate.sample(s.httpVersion)         # track the negotiated version (h3 after upgrade)
      if ev.id.len > 0:                  # verify Last-Event-ID resume continuity
        let id = try: parseInt(ev.id) except ValueError: -1
        if id < 0 or (lastId != 0 and id != lastId + 1):
          stderr.writeLine cfg.label & " FAIL: SSE id discontinuity: expected " &
            $(lastId + 1) & ", got " & ev.id & " (Last-Event-ID resume broken)"
          quit(1)
        lastId = id
      if epochTime() - lastReport >= cfg.reportSeconds.float:
        lastReport = epochTime()
        report(cfg.label, counter, epochTime() - start)
        syncChaosReport(sc)
      if sc.active and epochTime() - lastChaos >= chaosEvery:
        lastChaos = epochTime()
        syncChaosStep(sc)                # one interleaved chaos interaction per cadence
      if epochTime() >= deadline: break
    s.close()
  except CatchableError:
    counter.fail()
  gate.finish()   # hard-fail if the pinned protocol (h2/h3) was never negotiated

  if counter.ops == 0:
    stderr.writeLine cfg.label & " FAIL: no SSE event consumed"; quit(1)
  report(cfg.label & " final", counter, epochTime() - start)
  syncChaosFinish(sc, @[api])
  echo "== sse sync ", cfg.proto, " passed (", counter.ops, " events) =="

main()
