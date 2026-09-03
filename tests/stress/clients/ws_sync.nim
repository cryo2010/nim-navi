## stressWs, sync backend (`import navi`). Sync WebSocket is blocking, so one
## persistent socket loops text+binary echo round-trips until the deadline
## (concurrency does not apply to the sync client). Reports inline every interval.

import std/times
import ../common/[config, reporter, servers]
import navi

proc wsUrl(base: string): string =
  "wss://" & base["https://".len .. ^1] & "/ws"

proc main() =
  let cfg = loadConfig("sync")
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  var pool = initServerPool(cfg)
  let counter = newStatusCounter()

  var c = initNaviConfig()
  c.tls.caFile = cfg.cert
  if cfg.proto == "h3": c.http = {H3}   # WebSocket over h3 Extended CONNECT (RFC 9220)
  let api = newNavi(c)
  let ws = api.websocket(wsUrl(pool.pick()))

  let start = epochTime()
  let deadline = start + cfg.seconds
  var lastReport = start
  while epochTime() < deadline:
    try:
      ws.send("ping")
      let t = ws.receive()
      if t.kind != wmText or t.data != "ping": counter.fail(); break
      ws.send("bytes", binary = true)
      let b = ws.receive()
      if b.kind != wmBinary or b.data != "bytes": counter.fail(); break
      counter.tally(200)
    except CatchableError:
      counter.fail(); break
    if epochTime() - lastReport >= cfg.reportSeconds.float:
      lastReport = epochTime()
      report(cfg.label, counter, epochTime() - start)
  ws.close()

  report(cfg.label & " final", counter, epochTime() - start)
  echo "== ws sync passed (", counter.ops, " round-trips) =="

main()
