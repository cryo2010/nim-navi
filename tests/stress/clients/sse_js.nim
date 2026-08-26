## stressSse, navi/js backend (Node). Opens `concurrency` SSE subscriptions across
## the server pool and consumes events until the deadline, using navi's native
## reconnect + Last-Event-ID resume. Each worker checks the deadline in the `each`
## body (events flow continuously) and breaks, then closes its own stream -- never
## mid-read -- mirroring the native client. Reports via setInterval.

import navi/js
import ../common/harness_js

proc main() {.async.} =
  let cfg = loadJsCfg()
  var pool = initJsPool(cfg)
  let counter = newJsCounter()
  var c = initNaviConfig()
  let api = newNavi(c)

  var streams: seq[SseStream]
  for _ in 0 ..< cfg.concurrency:
    streams.add await api.sse(pool.pick() & "/events", retryMs = 20, maxRetryMs = 100)

  let start = nowMs()
  let deadline = start + cfg.seconds * 1000.0
  let timer = setIntervalJs(proc () = counter.report("[sse js]", start),
                            cfg.reportSeconds * 1000)

  proc worker(s: SseStream) {.async.} =
    try:
      s.each(ev):
        counter.tally(200)
        if nowMs() >= deadline: break
    except CatchableError:
      counter.note()
    s.close()                          # closed after the loop, not mid-read

  var futs: seq[Future[void]]
  for s in streams: futs.add worker(s)
  for f in futs: await f
  clearIntervalJs(timer)

  counter.report("[sse js]", start)
  echo "== sse js passed (", counter.ops, " events) =="

discard main()
