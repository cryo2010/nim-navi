## benchWs, navi/js backend (Node; name: navi-js). Opens `clients` x `concurrency`
## WebSockets and loops text echo round-trips, recording per-round-trip latency + bytes.

import navi/js
import ../common/harness_js

proc wsUrl(base: string): string =
  "wss://" & base["https://".len .. ^1] & "/ws"

proc main() {.async.} =
  let cfg = loadJsCfg()
  var pool = initJsPool(cfg)
  let rec = newJsBench()
  var api = newNavi(initNaviConfig())

  var socks: seq[WebSocket]
  for _ in 0 ..< cfg.clients * cfg.concurrency:
    socks.add await api.websocket(wsUrl(pool.pick()))

  let startMs = nowMs()
  let measureStartMs = startMs + cfg.warmupSeconds * 1000.0
  let deadlineMs = measureStartMs + cfg.seconds * 1000.0

  proc worker(ws: WebSocket) {.async.} =
    try:
      while nowMs() < deadlineMs:
        let t0 = nowUs()
        await ws.send("ping")
        let m = await ws.receive()
        if m.kind == wmClose: break
        if m.kind != wmText or m.data != "ping":
          rec.note(); break
        if nowMs() >= measureStartMs:
          rec.record(nowUs() - t0, m.data.len.float)
      await ws.close()
    except CatchableError:
      rec.note()

  var futs: seq[Future[void]]
  for ws in socks: futs.add worker(ws)
  for f in futs: await f

  rec.emitResult("navi-js", cfg.seconds)

discard main()
