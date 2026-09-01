## benchWs, sync backend (name: navi-sync). One persistent WebSocket looping text
## echo round-trips, recording per-round-trip latency + bytes.

import std/[times, monotimes]
import ../common/[config, reporter, servers]
import navi

proc wsUrl(base: string): string =
  "wss://" & base["https://".len .. ^1] & "/ws"

proc main() =
  let cfg = loadConfig("sync")
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  var pool = initServerPool(cfg)
  var c = initNaviConfig()
  c.tls.caFile = cfg.cert
  let api = newNavi(c)

  let rec = newBenchRecorder()
  let start = epochTime()
  let measureStart = start + cfg.warmupSeconds
  let deadline = measureStart + cfg.seconds
  try:
    let ws = api.websocket(wsUrl(pool.pick()))
    while epochTime() < deadline:
      let t0 = getMonoTime()
      ws.send("ping")
      let m = ws.receive()
      if m.kind == wmClose: break
      if m.kind != wmText or m.data != "ping":
        stderr.writeLine cfg.label & " FAIL: ws echo mismatch"; quit(1)
      if epochTime() >= measureStart:
        rec.record((getMonoTime() - t0).inMicroseconds, m.data.len)
    ws.close()
  except CatchableError as e:
    rec.fail()
    stderr.writeLine cfg.label & " FAIL: " & $e.name & ": " & e.msg
    quit(1)

  emitResult("navi-sync", rec, cfg.seconds)

main()
