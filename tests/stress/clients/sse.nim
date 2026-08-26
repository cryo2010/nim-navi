## stressSse, async backends (built twice: asyncdispatch, -d:useChronos -> chronos).
##
## Opens `concurrency` SSE subscriptions across the server pool and consumes events
## until the deadline. The server drops mid-stream periodically, so navi's
## transparent reconnect + Last-Event-ID resume is exercised continuously. Each
## event tallies as 200; nothing is retained. Reporter prints every interval.

import std/times
import ../common/[config, reporter, servers]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"
include ../common/httpset

proc worker(api: Navi, url: string, counter: StatusCounter,
            deadline: float) {.async.} =
  try:
    let s = await api.sse(url)
    s.each(ev):
      counter.tally(200)
      if epochTime() >= deadline: break
    await s.close()
  except CatchableError:
    counter.fail()

proc reporterLoop(cfg: Config, counter: StatusCounter,
                  start, deadline: float) {.async.} =
  var last = start
  while epochTime() < deadline:
    await sleep(1000)                   # 1s granularity: stop within ~1s of the deadline
    if epochTime() - last >= cfg.reportSeconds.float:
      last = epochTime()
      report(cfg.label, counter, epochTime() - start)

proc main() {.async.} =
  let cfg = loadConfig(backend)
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
  var futs: seq[Future[void]]
  for _ in 0 ..< cfg.concurrency:
    futs.add worker(api, pool.pick() & "/events", counter, deadline)
  futs.add reporterLoop(cfg, counter, start, deadline)
  for f in futs: await f

  report(cfg.label & " final", counter, epochTime() - start)
  echo "== sse ", backend, " ", cfg.proto, " passed (", counter.ops, " events) =="

waitFor main()
