## stressSse, navi/js backend (Node). Opens `concurrency` SSE subscriptions across
## the server pool and consumes events until the deadline, using navi's native
## reconnect + Last-Event-ID resume. Each worker checks the deadline in the `each`
## body (events flow continuously) and breaks, then closes its own stream -- never
## mid-read -- mirroring the native client. Reports via setInterval.
##
## The verified loop itself lives in parts/sse_js_part, shared with
## clients/mixed_js.nim.

import std/strutils
import navi/js
import ../common/harness_js
include parts/sse_js_part   # the verified SSE consume loop (shared with mixed_js.nim)

proc main() {.async.} =
  let cfg = loadJsCfg()
  var pool = initJsPool(cfg)
  let counter = newJsCounter()
  var c = initNaviConfig()
  let api = newNavi(c)

  var streams: seq[SseStream]
  for _ in 0 ..< cfg.concurrency:
    streams.add await api.sse(pool.pick() & "/events", retryMs = 20, maxRetryMs = 100,
                              minRetryMs = 20)

  let start = nowMs()
  let deadline = start + cfg.seconds * 1000.0
  let timer = setIntervalJs(proc () = counter.report("[sse js]", start),
                            cfg.reportSeconds * 1000)

  var futs: seq[Future[void]]
  for s in streams: futs.add sseWorker("[sse js]", s, counter, deadline)
  for f in futs: await f
  clearIntervalJs(timer)

  # `ops - errors`, not `ops`: note() increments ops too (see sse.nim).
  if counter.ops - counter.errors == 0: jsFail("[sse js]", "no SSE event consumed")
  let elapsed = (nowMs() - start) / 1000.0   # the measured phase: the rate divisor
  counter.report("[sse js]", start, final = true)
  echo "== sse js passed (", counter.ops, " events, ",
    fmtRate(counter.ops, elapsed), " events/s) =="

discard main()
