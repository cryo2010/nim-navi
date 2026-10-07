## stressWs, navi/js backend (Node). Opens `concurrency` WebSockets across the
## server pool and loops text+binary echo round-trips until the deadline. Reports
## via setInterval. The runner trusts the self-signed cert via NODE_EXTRA_CA_CERTS.
##
## The verified loop itself lives in parts/ws_js_part, shared with
## clients/mixed_js.nim.

import navi/js
import ../common/harness_js
include parts/ws_js_part   # the verified ws echo loop (shared with mixed_js.nim)

proc main() {.async.} =
  let cfg = loadJsCfg()
  var pool = initJsPool(cfg)
  let counter = newJsCounter()
  var c = initNaviConfig()
  let api = newNavi(c)

  var socks: seq[WebSocket]
  for _ in 0 ..< cfg.concurrency:
    socks.add await api.websocket(wsUrl(pool.pick()))

  let start = nowMs()
  let deadline = start + cfg.seconds * 1000.0
  let timer = setIntervalJs(proc () = counter.report("[ws js]", start),
                            cfg.reportSeconds * 1000)

  var futs: seq[Future[void]]
  for ws in socks: futs.add wsWorker(ws, counter, deadline)
  for f in futs: await f
  clearIntervalJs(timer)

  # `ops - errors`, not `ops`: note() increments ops too (see ws.nim).
  if counter.ops - counter.errors == 0:
    jsFail("[ws js]", "no WebSocket round-trip completed")
  let elapsed = (nowMs() - start) / 1000.0   # the measured phase: the rate divisor
  counter.report("[ws js]", start, final = true)
  echo "== ws js passed (", counter.ops, " round-trips, ",
    fmtRate(counter.ops, elapsed), " round-trips/s) =="

discard main()
