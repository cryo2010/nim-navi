## stressWs, async backends (built twice: asyncdispatch, -d:useChronos -> chronos).
##
## Opens `concurrency` persistent WebSockets across the server pool and loops
## text+binary echo round-trips on each until the deadline. A completed round-trip
## tallies as 200; an error tallies as a failure and that socket stops. Nothing is
## retained beyond the current frame, so memory stays flat. Reporter prints every
## interval. PROTO IS a dimension: the transport is pinned via config.http, so an
## h1 cell uses the RFC 6455 Upgrade, an h2 cell RFC 8441 Extended CONNECT, and an
## h3 cell RFC 9220 -- and websocket() raises (failing the cell) if the pinned
## transport can't carry it, so a silent downgrade can't pass green.

import std/[times, strutils]
from std/os import getEnv
import ../common/[config, reporter, servers, leakcheck]

when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"
include ../common/httpset
include ../common/chaos
include parts/ws_part   # the verified ws echo loop (shared with mixed.nim)

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
  let notice = cfg.chaosSkipNotice
  if notice.len > 0: echo cfg.label, " ", notice
  var leakBase = sampleBaseline(cfg.chaos)   # before ANY Navi is constructed
  var pool = initServerPool(cfg)
  let counter = newStatusCounter()

  var c = initNaviConfig()
  c.tls.caFile = cfg.cert
  c.http = httpVersions(cfg.proto)   # pin the ws transport: {H1} upgrade / {H2} / {H3}
  let api = newNavi(c)

  # Open the sockets first (sequentially), so the soak that follows isn't racing
  # WS TLS handshakes for accepts.
  var socks: seq[WebSocket]
  for _ in 0 ..< cfg.concurrency:
    socks.add await api.websocket(wsUrl(pool.pick()))

  let start = epochTime()
  let deadline = start + cfg.seconds
  let chaos = chaosMaybeStart(cfg, deadline, cfg.reportSeconds)  # no-op when off
  var futs: seq[Future[void]]
  for ws in socks: futs.add wsWorker(ws, counter, deadline)
  futs.add reporterLoop(cfg, counter, start, deadline)
  for f in futs: await f
  await chaosAwait(chaos)

  # `ops - errors`, not `ops`: counter.fail() increments ops too, so a cell whose
  # every round-trip failed used to pass this check. A cell that did no round-trips
  # is not a pass.
  if counter.ops - counter.errors == 0:
    stderr.writeLine cfg.label & " FAIL: no WebSocket round-trip completed"
    quit(1)
  let elapsed = epochTime() - start      # the measured phase: also the rate divisor
  report(cfg.label & " final", counter, elapsed, final = true)
  await chaosFinish(chaos, leakBase, cfg, @[api])
  echo "== ws ", backend, " ", cfg.proto, " passed (", counter.ops, " round-trips, ",
    fmtRate(counter.ops, elapsed), " round-trips/s) =="

waitFor main()
