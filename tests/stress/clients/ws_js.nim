## stressWs, navi/js backend (Node). Opens `concurrency` WebSockets across the
## server pool and loops text+binary echo round-trips until the deadline. Reports
## via setInterval. The runner trusts the self-signed cert via NODE_EXTRA_CA_CERTS.

import navi/js
import ../common/harness_js

proc wsUrl(base: string): string =
  "wss://" & base["https://".len .. ^1] & "/ws"

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

  proc worker(ws: WebSocket) {.async.} =
    try:
      while nowMs() < deadline:
        await ws.send("ping")
        let t = await ws.receive()
        if t.kind != wmText or t.data != "ping": counter.note(); break
        await ws.send("bytes", binary = true)
        let b = await ws.receive()
        if b.kind != wmBinary or b.data != "bytes": counter.note(); break
        counter.tally(200)
      await ws.close()
    except CatchableError:
      counter.note()

  var futs: seq[Future[void]]
  for ws in socks: futs.add worker(ws)
  for f in futs: await f
  clearIntervalJs(timer)

  if counter.ops == 0: (echo "[ws js] FAIL: no WebSocket round-trip completed"; jsExit(1))
  counter.report("[ws js]", start)
  echo "== ws js passed (", counter.ops, " round-trips) =="

discard main()
