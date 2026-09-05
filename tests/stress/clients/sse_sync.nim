## stressSse, sync backend (`import navi`). One blocking SSE subscription consumes
## events (navi reconnects + resumes Last-Event-ID transparently as the server
## drops mid-stream) until the deadline. Reports inline between events.

import std/[times, strutils]
import ../common/[config, reporter, servers]
import navi
include ../common/httpset

proc main() =
  let cfg = loadConfig("sync")
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  var pool = initServerPool(cfg)
  let counter = newStatusCounter()

  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)
  c.tls.caFile = cfg.cert
  let api = newNavi(c)

  let start = epochTime()
  let deadline = start + cfg.seconds
  var lastReport = start
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
      if epochTime() >= deadline: break
    s.close()
  except CatchableError:
    counter.fail()
  gate.finish()   # hard-fail if the pinned protocol (h2/h3) was never negotiated

  if counter.ops == 0:
    stderr.writeLine cfg.label & " FAIL: no SSE event consumed"; quit(1)
  report(cfg.label & " final", counter, epochTime() - start)
  echo "== sse sync ", cfg.proto, " passed (", counter.ops, " events) =="

main()
