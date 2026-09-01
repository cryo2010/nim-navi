## benchSse, navi/js backend (Node; name: navi-js). Opens `clients` x `concurrency`
## SSE subscriptions and consumes events until the deadline, recording per-event
## inter-arrival latency + bytes.

import navi/js
import ../common/harness_js

proc main() {.async.} =
  let cfg = loadJsCfg()
  var pool = initJsPool(cfg)
  let rec = newJsBench()
  var api = newNavi(initNaviConfig())

  var streams: seq[SseStream]
  for _ in 0 ..< cfg.clients * cfg.concurrency:
    streams.add await api.sse(pool.pick() & "/events", retryMs = 20, maxRetryMs = 100)

  let startMs = nowMs()
  let measureStartMs = startMs + cfg.warmupSeconds * 1000.0
  let deadlineMs = measureStartMs + cfg.seconds * 1000.0

  proc worker(s: SseStream) {.async.} =
    var last = nowUs()
    try:
      s.each(ev):
        let now = nowUs()
        if nowMs() >= measureStartMs:
          rec.record(now - last, ev.data.len.float)
        last = now
        if nowMs() >= deadlineMs: break
    except CatchableError:
      rec.note()
    s.close()

  var futs: seq[Future[void]]
  for s in streams: futs.add worker(s)
  for f in futs: await f

  rec.emitResult("navi-js", cfg.seconds)

discard main()
