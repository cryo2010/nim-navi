## benchWs, sync backend (name: navi-sync). One persistent WebSocket per THREAD
## looping text echo round-trips, recording per-round-trip latency + bytes; the
## process merges the threads into one RESULT. Scaled across cores with one navi
## client per thread (sync has no in-thread fan-out).

import std/[times, monotimes]
import ../common/[config, reporter, servers, runner]
import navi

proc wsUrl(base: string): string =
  "wss://" & base["https://".len .. ^1] & "/ws"

proc wsThread(a: ptr BenchThread) {.thread, nimcall.} =
  # One blocking navi client per thread; navi keeps no shared mutable globals, so any
  # gcsafe complaints are false positives from indirect callbacks.
  {.gcsafe.}:
    let cfg = a.cfg
    var pool = initServerPool(cfg)
    var c = initNaviConfig()
    c.tls.caFile = cfg.cert
    let api = newNavi(c)
    let rec = newBenchRecorder()
    try:
      let ws = api.websocket(wsUrl(pool.pick()))
      while epochTime() < a.deadline:
        let t0 = getMonoTime()
        ws.send("ping")
        let m = ws.receive()
        if m.kind == wmClose: break
        if m.kind != wmText or m.data != "ping":
          stderr.writeLine cfg.label & " FAIL: ws echo mismatch"; quit(1)
        if epochTime() >= a.measureStart:
          rec.record((getMonoTime() - t0).inMicroseconds, m.data.len)
      ws.close()
    except CatchableError as e:
      rec.fail()
      stderr.writeLine cfg.label & " FAIL: " & $e.name & ": " & e.msg
      quit(1)
    a.rec = rec

proc main() =
  let cfg = loadConfig("sync")
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  runThreaded(cfg, "navi-sync", wsThread)

main()
