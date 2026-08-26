## stressSse, async backends (built twice: asyncdispatch, -d:useChronos -> chronos).
##
## Opens `concurrency` SSE subscriptions across the server pool and consumes events
## until the deadline, using navi's native transparent reconnect + Last-Event-ID
## resume (the server drops periodically, so this exercises it). Each event tallies
## as 200; nothing is retained.
##
## Each worker checks the deadline in the `each` body (events flow continuously, so
## it runs constantly) and breaks, then closes its own stream when NOT parked in a
## read. This avoids closing a stream out from under a parked h2 read, which orphans
## the read's future and crashes the dispatcher at teardown ("No handles or timers
## registered"). Reporter prints every interval.

import std/times
import ../common/[config, reporter, servers]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"
include ../common/httpset

proc worker(s: SseStream, counter: StatusCounter, deadline: float) {.async.} =
  try:
    s.each(ev):
      counter.tally(200)
      if epochTime() >= deadline: break   # self-terminate: events flow continuously
  except CatchableError:
    counter.fail()
  try: await s.close()                     # closed here, not mid-read: clean teardown
  except CatchableError: discard

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

  # Low retry so reconnects are fast under load (the server ends the stream often);
  # also bounds how long a worker parked in the backoff lags the deadline.
  var streams: seq[SseStream]
  for _ in 0 ..< cfg.concurrency:
    streams.add await api.sse(pool.pick() & "/events", retryMs = 20, maxRetryMs = 100)

  let start = epochTime()
  let deadline = start + cfg.seconds
  var futs: seq[Future[void]]
  for s in streams: futs.add worker(s, counter, deadline)
  futs.add reporterLoop(cfg, counter, start, deadline)
  for f in futs: await f

  report(cfg.label & " final", counter, epochTime() - start)
  echo "== sse ", backend, " ", cfg.proto, " passed (", counter.ops, " events) =="

waitFor main()
