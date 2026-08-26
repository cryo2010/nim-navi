## stressSse, sync backend (`import navi`). One blocking SSE subscription consumes
## events (navi reconnects + resumes Last-Event-ID transparently as the server
## drops mid-stream) until the deadline. Reports inline between events.

import std/times
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
  try:
    let s = api.sse(pool.pick() & "/events")
    s.each(ev):
      counter.tally(200)
      if epochTime() - lastReport >= cfg.reportSeconds.float:
        lastReport = epochTime()
        report(cfg.label, counter, epochTime() - start)
      if epochTime() >= deadline: break
    s.close()
  except CatchableError:
    counter.fail()

  report(cfg.label & " final", counter, epochTime() - start)
  echo "== sse sync ", cfg.proto, " passed (", counter.ops, " events) =="

main()
