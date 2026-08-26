## stressWs, async backends (built twice: asyncdispatch, -d:useChronos -> chronos).
##
## Opens `concurrency` persistent WebSockets across the server pool and loops
## text+binary echo round-trips on each until the deadline. A completed round-trip
## tallies as 200; an error tallies as a failure and that socket stops. Nothing is
## retained beyond the current frame, so memory stays flat. Reporter prints every
## interval. WebSocket is an h1 upgrade, so PROTO is not a dimension here.

import std/[times, strutils]
import ../common/[config, reporter, servers]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"

proc wsUrl(base: string): string =
  "wss://" & base["https://".len .. ^1] & "/ws"

proc worker(ws: WebSocket, counter: StatusCounter, deadline: float) {.async.} =
  try:
    while epochTime() < deadline:
      await ws.send("ping")
      let t = await ws.receive()
      if t.kind != wmText or t.data != "ping": counter.fail(); break
      await ws.send("bytes", binary = true)
      let b = await ws.receive()
      if b.kind != wmBinary or b.data != "bytes": counter.fail(); break
      counter.tally(200)
    await ws.close()
  except CatchableError:
    counter.fail()

proc reporterLoop(cfg: Config, counter: StatusCounter,
                  start, deadline: float) {.async.} =
  var last = start
  while epochTime() < deadline:
    await sleep(1000)                   # 1s granularity: stop within ~1s of the deadline
    if epochTime() - last >= cfg.reportSeconds.float:
      last = epochTime()
      report(cfg.label, counter, epochTime() - start)

proc main() {.async.} =
  let cfg = loadConfig(backend)
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  var pool = initServerPool(cfg)
  let counter = newStatusCounter()

  var c = initNaviConfig()
  c.tls.caFile = cfg.cert
  let api = newNavi(c)

  # Open the sockets first (sequentially), so the soak that follows isn't racing
  # WS TLS handshakes for accepts.
  var socks: seq[WebSocket]
  for _ in 0 ..< cfg.concurrency:
    socks.add await api.websocket(wsUrl(pool.pick()))

  let start = epochTime()
  let deadline = start + cfg.seconds
  var futs: seq[Future[void]]
  for ws in socks: futs.add worker(ws, counter, deadline)
  futs.add reporterLoop(cfg, counter, start, deadline)
  for f in futs: await f

  report(cfg.label & " final", counter, epochTime() - start)
  echo "== ws ", backend, " passed (", counter.ops, " round-trips) =="

waitFor main()
