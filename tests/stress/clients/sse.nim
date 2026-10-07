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

import std/[times, strutils]
import ../common/[config, reporter, servers, leakcheck]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"
include ../common/httpset
include ../common/chaos
include parts/sse_part   # the verified SSE consume loop (shared with mixed.nim)

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
  let notice = cfg.chaosSkipNotice
  if notice.len > 0: echo cfg.label, " ", notice
  var leakBase = sampleBaseline(cfg.chaos)   # before ANY Navi is constructed
  var pool = initServerPool(cfg)
  let counter = newStatusCounter()

  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)
  c.tls.caFile = cfg.cert
  let api = newNavi(c)

  # Low retry so reconnects are fast under load (the server ends the stream often);
  # also bounds how long a worker parked in the backoff lags the deadline. The
  # stream always delivers events before the server drops it, so the empty-connect
  # backoff never engages; minRetryMs lowers the default 100 ms floor so the 20 ms
  # base stands.
  var streams: seq[SseStream]
  for _ in 0 ..< cfg.concurrency:
    streams.add await api.sse(pool.pick() & "/events", retryMs = 20, maxRetryMs = 100,
                              minRetryMs = 20)

  let start = epochTime()
  let deadline = start + cfg.seconds
  let chaos = chaosMaybeStart(cfg, deadline, cfg.reportSeconds)  # no-op when off
  var gate = initVersionGate(cfg)
  var futs: seq[Future[void]]
  for s in streams: futs.add sseWorker(cfg, s, counter, addr gate, deadline)
  futs.add reporterLoop(cfg, counter, start, deadline)
  for f in futs: await f
  await chaosAwait(chaos)
  gate.finish()   # hard-fail if the pinned protocol (h2/h3) was never negotiated

  # `ops - errors`, not `ops`: counter.fail() increments ops too, so a stream that
  # only ever errored used to pass this check.
  if counter.ops - counter.errors == 0: cfg.failHard("no SSE event consumed")
  let elapsed = epochTime() - start      # the measured phase: also the rate divisor
  report(cfg.label & " final", counter, elapsed, final = true)
  await chaosFinish(chaos, leakBase, cfg, @[api])
  echo "== sse ", backend, " ", cfg.proto, " passed (", counter.ops, " events, ",
    fmtRate(counter.ops, elapsed), " events/s) =="

waitFor main()
