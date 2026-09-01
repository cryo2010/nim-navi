## benchWs, async backends (navi-async / navi-chronos). Opens `clients` x
## `concurrency` persistent WebSockets and loops text echo round-trips, recording
## per-round-trip latency + bytes (round-trips/s + MB/s) over a timed window.
## WebSocket is an h1 upgrade, so PROTO is not a dimension (run.sh runs ws at h1).

import std/[times, monotimes]
import ../common/[config, reporter, servers]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
  const clientName = "navi-chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"
  const clientName = "navi-async"

proc wsUrl(base: string): string =
  "wss://" & base["https://".len .. ^1] & "/ws"

proc worker(ws: WebSocket, cfg: Config, rec: BenchRecorder,
            measureStart, deadline: float) {.async.} =
  try:
    while epochTime() < deadline:
      let t0 = getMonoTime()
      await ws.send("ping")
      let m = await ws.receive()
      if m.kind == wmClose: break
      if m.kind != wmText or m.data != "ping":
        stderr.writeLine cfg.label & " FAIL: ws echo mismatch (kind=" & $m.kind & ")"
        quit(1)
      if epochTime() >= measureStart:
        rec.record((getMonoTime() - t0).inMicroseconds, m.data.len)
  except CatchableError as e:
    rec.fail()
    stderr.writeLine cfg.label & " FAIL: " & $e.name & ": " & e.msg
    quit(1)
  try: await ws.close()
  except CatchableError: discard

proc main() {.async.} =
  let cfg = loadConfig(backend)
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  var pool = initServerPool(cfg)
  var c = initNaviConfig()
  c.tls.caFile = cfg.cert
  let api = newNavi(c)

  var socks: seq[WebSocket]
  for _ in 0 ..< cfg.clients * cfg.concurrency:
    socks.add await api.websocket(wsUrl(pool.pick()))

  let rec = newBenchRecorder()
  let start = epochTime()
  let measureStart = start + cfg.warmupSeconds
  let deadline = measureStart + cfg.seconds
  var futs: seq[Future[void]]
  for ws in socks: futs.add worker(ws, cfg, rec, measureStart, deadline)
  for f in futs: await f

  emitResult(clientName, rec, cfg.seconds)

waitFor main()
