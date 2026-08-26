## stressSse, navi/js backend (Node). Opens `concurrency` SSE subscriptions across
## the server pool and consumes events until the deadline; navi reconnects +
## resumes Last-Event-ID as the server drops mid-stream. Reports via setInterval.
## (The js runtime picks the HTTP version; PROTO is not a client dimension here.)

import navi/js
import ../common/harness_js

proc main() {.async.} =
  let cfg = loadJsCfg()
  var pool = initJsPool(cfg)
  let counter = newJsCounter()
  var c = initNaviConfig()
  let api = newNavi(c)

  let start = nowMs()
  let deadline = start + cfg.seconds * 1000.0
  let timer = setIntervalJs(proc () = counter.report("[sse js]", start),
                            cfg.reportSeconds * 1000)

  proc worker(url: string) {.async.} =
    try:
      let s = await api.sse(url)
      s.each(ev):
        counter.tally(200)
        if nowMs() >= deadline: break
      s.close()
    except CatchableError:
      counter.note()

  var futs: seq[Future[void]]
  for _ in 0 ..< cfg.concurrency:
    futs.add worker(pool.pick() & "/events")
  for f in futs: await f
  clearIntervalJs(timer)

  counter.report("[sse js]", start)
  echo "== sse js passed (", counter.ops, " events) =="

discard main()
