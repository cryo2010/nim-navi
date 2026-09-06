## benchWs, async backends (navi-async / navi-chronos). Scales across cores with one
## navi client per THREAD (not one process per core): each thread opens `concurrency`
## persistent WebSockets on its own event loop and loops text echo round-trips,
## recording per-round-trip latency + bytes. The process merges the threads into one
## RESULT (round-trips/s + MB/s). WebSocket is an h1 upgrade, so PROTO is not a
## dimension (run.sh runs ws at h1).

import std/[times, monotimes]
import ../common/[config, reporter, servers, runner]
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

proc wsThread(a: ptr BenchThread) {.thread, nimcall.} =
  # Each thread owns its own navi client, sockets and event loop, and navi keeps no
  # shared mutable globals, so the only gcsafe complaints are false positives from
  # navi's indirect callback calls (e.g. BodyProducer); assert safety for the body.
  {.gcsafe.}:
    let cfg = a.cfg
    var pool = initServerPool(cfg)
    var c = initNaviConfig()
    c.tls.caFile = cfg.cert
    let api = newNavi(c)
    var socks: seq[WebSocket]
    for _ in 0 ..< a.concurrency:
      socks.add waitFor api.websocket(wsUrl(pool.pick()))
    let rec = newBenchRecorder()
    var futs: seq[Future[void]]
    for ws in socks: futs.add worker(ws, cfg, rec, a.measureStart, a.deadline)
    for f in futs: waitFor f
    a.rec = rec

proc main() =
  let cfg = loadConfig(backend)
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  runThreaded(cfg, clientName, wsThread)

main()
