## stressMixed, async backends (one source, built twice):
##   nim c -d:ssl ...                -> navi/asyncdispatch
##   nim c -d:ssl -d:useChronos ...  -> navi/chronos
##
## All five verified workloads at once, against one set of servers, through the
## SAME `Navi` instances: `clients` instances are built once and shared
## round-robin across every slice, so a bulk `/upload` or `/download` body and
## dozens of small `/echo` streams really ride one pooled h2/h3 connection next
## to a parked SSE read and a WebSocket. That cross-workload interaction is the
## point. #444 (a buffered upload parking an h2 connection's only reader, which
## delayed every inbound frame for every other stream on it) is the shape this
## cell exists to catch; no single-workload cell can produce it.
##
## One caveat, and only under `NAVI_SERVER=hypercorn`: on h3 the ws slice shares
## the instances and the event loop but NOT a connection. Caddy's `reverse_proxy`
## cannot bridge an h3 Extended CONNECT, so that slice dials a separate aioquic
## QUIC origin on its own port band (see mixedWsUrl); the other four slices still
## share the Caddy origins' connections. `NAVI_SERVER=vortex` removes the caveat:
## one vortex process terminates the h3 Extended CONNECT on the same port as the
## rest, so the ws slice finally shares a QUIC connection with the other four --
## which is the interaction this cell exists for.
##
## Each slice keeps its own verification and its own counter, exactly as the
## single-workload client does -- per-response `checkVersion` for requests and
## the streams, the byte/JSON/form echo checks, the SSE `VersionGate` and
## Last-Event-ID continuity, the upload/download SHA-1 brackets. The counters are
## reported side by side and NEVER summed, and each slice has its own zero-work
## check, so a stalled SSE feed or a never-completing upload cannot hide behind a
## healthy `/echo` rate.
##
## The work procs themselves are the shared `parts/*_part.nim` includes the five
## single-workload clients use, so this cell drives the same verified code, not a
## re-implementation.

import std/[times, strutils, json]
from std/os import getEnv
import ../zlibcodec
import ../common/[config, reporter, servers, payloads, streamcontent, mixsplit,
                  leakcheck]

when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"
include ../common/httpset
include ../common/chaos
include parts/requests_part          # /echo: verbs x payload catalog, verified
include parts/ws_part                # ws: text+binary echo round-trips
include parts/sse_part               # sse: events + Last-Event-ID resume
include parts/stream_upload_part     # one verified streamed upload
include parts/stream_download_part   # one verified streamed download

proc mixedWsUrl(cfg: Config, pool: ptr ServerPool, i: int): string =
  ## The ws origin for ws worker `i`. Everywhere except one cell this is just
  ## another of the shared origins, so the WebSocket rides the same connections
  ## as the rest of the mix (the whole point of the cell).
  ##
  ## The exception is h3 under `NAVI_SERVER=hypercorn`: Caddy's `reverse_proxy`
  ## does not bridge an h3 Extended CONNECT to a backend WebSocket, so run.sh
  ## stands aioquic ws servers up on their own band (`NAVI_WS_H3_PORTBAND`)
  ## beside the Caddy front and the ws slice dials those. navi direct-dials QUIC
  ## for an h3 WebSocket, so the band origin needs no Alt-Svc discovery leg; the
  ## other four slices keep the Caddy origins and their /echo, /events, /upload
  ## and /download routes. Under `NAVI_SERVER=vortex` there is no band and no
  ## detour: the vortex origin on the base port terminates the h3 Extended
  ## CONNECT itself, so every protocol takes the shared-origin path.
  if cfg.proto == "h3" and cfg.server == "hypercorn":
    "wss://" & cfg.host & ":" &
      $(cfg.basePort + cfg.wsH3PortBand + i mod cfg.servers) & "/ws"
  else:
    wsUrl(pool[].pick())

proc uploadSlice(api: Navi, cfg: Config, pool: ptr ServerPool,
                 prog: StreamProgress, deadline: float) {.async.} =
  ## One upload worker: streamed /upload transfers back to back until the
  ## deadline, retrying a transient transport error (a recycled or idle-closed
  ## pooled connection mid-transfer) exactly as clients/stream_upload.nim does.
  ## A checksum, size, status or protocol miss inside oneUpload is still a hard
  ## fail. The whole slice shares one StreamProgress, so the report line's MB/s
  ## covers the slice, not one worker.
  ##
  ## `lastDur` is this worker's last observed transfer duration and gates the
  ## next start: a transfer sharing pooled connections with 20 other workers
  ## takes seconds, so one begun just before the deadline runs long past it and
  ## charges its overrun to every other slice's rate. The first transfer always
  ## starts (nothing observed yet); after that the worker stops rather than start
  ## one that cannot finish in the time left.
  var lastDur = 0.0
  while true:
    let now = epochTime()
    if now >= deadline: break
    if lastDur > 0.0 and deadline - now < lastDur: break
    let began = now
    try:
      await oneUpload(api, cfg, prog, pool[].pick() & "/upload")
      inc prog.transfers
      lastDur = epochTime() - began
    except CatchableError as e:
      inc prog.errors
      stderr.writeLine cfg.label & " upload retried: " & e.msg
      # An immediately-refused connection fails in microseconds, so retrying it
      # straight away burns the whole cell spinning. Back off a beat first.
      await sleep(250)

proc downloadSlice(api: Navi, cfg: Config, pool: ptr ServerPool,
                   prog: StreamProgress, deadline: float) {.async.} =
  ## One download worker, mirroring uploadSlice against /download: same
  ## last-duration gate on starting a transfer, same back-off after a retried
  ## transport error.
  var lastDur = 0.0
  while true:
    let now = epochTime()
    if now >= deadline: break
    if lastDur > 0.0 and deadline - now < lastDur: break
    let began = now
    try:
      await oneDownload(api, cfg, prog,
                        pool[].pick() & "/download?size=" & $cfg.streamBytes)
      inc prog.transfers
      lastDur = epochTime() - began
    except CatchableError as e:
      inc prog.errors
      stderr.writeLine cfg.label & " download retried: " & e.msg
      await sleep(250)

proc streamSeg(name: string, prog: StreamProgress, now, ran: float,
               final: bool): string =
  ## One streaming slice's field group: cumulative MB, then MB/s over the
  ## interval since the previous line (the whole-run average when `final`), then
  ## the transfer and retry counts -- the same fields the single-workload stream
  ## cells print, as a segment of the composed mixed line.
  let (mb, rate) =
    if final: (megabytes(prog.rate.total), mbPerSec(prog.rate.total, ran))
    else: prog.rate.mark(now)
  name & " " & $mb & "MB " & rate & " MB/s " & $prog.transfers & " done " &
    $prog.errors & " retried"

proc mixedReport(label: string, reqC, wsC, sseC: StatusCounter,
                 up, down: StreamProgress, start, now: float, final = false,
                 opsRan = 0.0) =
  ## ONE line for the whole mix: every slice's own numbers side by side, never a
  ## sum. Request-shaped slices render through reporter.segment (status tallies
  ## for requests, bare counts for ws/sse), the streaming slices through
  ## streamSeg; then the process-wide RSS/heap and the elapsed stamp once.
  ##
  ## Two divisors on the final line. The req/ws/sse slices stop at the deadline,
  ## so they divide by `opsRan` -- the time the deadline was reached -- and are
  ## not charged for a stream transfer that was still in flight then. The two
  ## streaming slices divide by the full elapsed, because their bytes really did
  ## take that long to move. On an interval line the two are the same window.
  let elapsed = now - start
  let opsElapsed = if final: opsRan else: elapsed
  let rss = rssBytes()
  let rssStr = if rss > 0: fmtBytes(rss) else: "n/a"
  echo label, " ",
    reqC.segment("req", "ops", opsElapsed, statuses = true, final = final), " | ",
    wsC.segment("ws", "rt", opsElapsed, final = final), " | ",
    sseC.segment("sse", "ev", opsElapsed, final = final), " | ",
    streamSeg("up", up, now, elapsed, final), " | ",
    streamSeg("down", down, now, elapsed, final),
    " | RSS ", rssStr, " | heap ", fmtBytes(getOccupiedMem()),
    " | t=", elapsed.int, "s"

type Live = ref object
  ## How many slice futures are still running. The reporter watches this instead
  ## of the deadline, so a stream transfer that is still in flight when the
  ## deadline passes keeps showing up on the interval lines instead of vanishing
  ## into a silent gap between the last line and the final one.
  workers: int

proc track(live: Live, f: Future[void]) {.async.} =
  ## Await one slice worker and drop it from the live count when it ends.
  try:
    await f
  finally:
    dec live.workers

proc reporterLoop(cfg: Config, reqC, wsC, sseC: StatusCounter,
                  up, down: StreamProgress, start: float,
                  live: Live) {.async.} =
  var last = start
  while live.workers > 0:
    await sleep(1000)                   # 1s granularity: stop within ~1s of the last worker
    let now = epochTime()
    if now - last >= cfg.reportSeconds.float:
      last = now
      mixedReport(cfg.label, reqC, wsC, sseC, up, down, start, now)

proc main() {.async.} =
  let cfg = loadConfig(backend)
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  let notice = cfg.chaosSkipNotice        # js chaos-skip notice, if any
  if notice.len > 0: echo cfg.label, " ", notice
  var leakBase = sampleBaseline(cfg.chaos)   # before ANY Navi is constructed
  var pool = initServerPool(cfg)
  let payloads = filterPayloads(allPayloads, cfg.contentTypes, jsSafe = false)
  let reqCounter = newStatusCounter()
  let wsCounter = newStatusCounter()
  let sseCounter = newStatusCounter()

  # The shared instances: requests' mkClient, middleware included (the x-stress
  # header is harmless on /ws, /events, /upload and /download). Every slice draws
  # from this one seq round-robin by worker index, so the slices share origins,
  # pools and connections rather than merely sharing a server.
  var apis: seq[Navi]
  for _ in 0 ..< cfg.clients: apis.add mkClient(cfg)

  # Warm up each client's per-origin protocol so the measured phase runs pinned
  # from the first request: h3 is discovered via an initial Alt-Svc round-trip, so
  # hit each origin until the expected version is negotiated. After this, any
  # downgrade during the soak fails hard (checkVersion).
  let expect = cfg.expectedVersion
  if expect.len > 0:
    for api in apis:
      for base in pool.all():
        for _ in 0 ..< 3:
          try:
            if (await api.request(GET, base & "/echo")).httpVersion == expect: break
          except CatchableError: break

  await featureChecks(cfg, pool.all()[0])   # redirect/status/auth/cookie coverage

  let split = mixSplit(cfg.clients * cfg.concurrency)
  echo cfg.label, " split req=", split.req, " ws=", split.ws, " sse=", split.sse,
       " up=", split.up, " down=", split.down

  # Open the ws sockets and the SSE streams up front (sequentially, like ws.nim
  # and sse.nim), so the soak that follows is not racing WS handshakes or SSE
  # connects for accepts. Both go through the shared instances.
  # `k` is ONE running worker counter across every slice, so the instances are
  # dealt out round-robin over the whole mix. Indexing each slice from 0 instead
  # piled every slice onto the first instances and left the last one carrying
  # requests only -- never a stream, which is the interaction this cell is for.
  var k = 0
  var socks: seq[WebSocket]
  for i in 0 ..< split.ws:
    socks.add await apis[k mod apis.len].websocket(mixedWsUrl(cfg, addr pool, i))
    inc k
  # Low retry so reconnects are fast under load (the server ends the stream
  # often); minRetryMs lowers the default 100 ms floor so the 20 ms base stands.
  var streams: seq[SseStream]
  for _ in 0 ..< split.sse:
    streams.add await apis[k mod apis.len].sse(pool.pick() & "/events",
      retryMs = 20, maxRetryMs = 100, minRetryMs = 20)
    inc k

  let start = epochTime()
  let deadline = start + cfg.seconds
  let chaos = chaosMaybeStart(cfg, deadline, cfg.reportSeconds)  # no-op when off
  var gate = initVersionGate(cfg)
  let up = StreamProgress(rate: newStreamRate(start))
  let down = StreamProgress(rate: newStreamRate(start))
  var slices: seq[Future[void]]
  for i in 0 ..< split.req:
    slices.add requestsWorker(apis[k mod apis.len], cfg, addr pool, payloads,
                              reqCounter, deadline, i)
    inc k
  for s in socks: slices.add wsWorker(s, wsCounter, deadline)
  for s in streams: slices.add sseWorker(cfg, s, sseCounter, addr gate, deadline)
  for _ in 0 ..< split.up:
    slices.add uploadSlice(apis[k mod apis.len], cfg, addr pool, up, deadline)
    inc k
  for _ in 0 ..< split.down:
    slices.add downloadSlice(apis[k mod apis.len], cfg, addr pool, down, deadline)
    inc k
  let live = Live(workers: slices.len)
  var futs: seq[Future[void]]
  for f in slices: futs.add track(live, f)
  futs.add reporterLoop(cfg, reqCounter, wsCounter, sseCounter, up, down,
                        start, live)
  for f in futs: await f
  # Measure the run before the chaos/leak settle phase, which moves no bytes and
  # issues no requests and would otherwise drag every average down. `elapsed` is
  # the whole measured phase, overrun included; `opsRan` stops at the deadline,
  # which is where the req/ws/sse slices stopped.
  let elapsed = epochTime() - start
  let opsRan = min(elapsed, cfg.seconds)
  await chaosAwait(chaos)                  # drain the chaos workers/watchdog
  gate.finish()   # hard-fail if the pinned protocol (h2/h3) was never negotiated

  # Per-slice zero-work checks. On the sum a dead slice would hide behind a
  # healthy one, so each slice must have done its own work.
  # `ops - errors`, not `ops`: StatusCounter.fail() increments ops too, so an
  # all-failing ws or sse slice would otherwise pass this check silently.
  if reqCounter.ops == 0: cfg.failHard("no request completed")
  if wsCounter.ops - wsCounter.errors == 0:
    cfg.failHard("no WebSocket round-trip completed")
  if sseCounter.ops - sseCounter.errors == 0:
    cfg.failHard("no SSE event consumed")
  # Bytes, not transfers: these slices share connections with 20 other workers,
  # so a healthy cell can end with a transfer still in flight and 0 completed.
  # Zero bytes moved is the real stall; the transfer counts stay on the line.
  if up.rate.total == 0:
    cfg.failHard("no upload bytes moved (" & $up.errors & " errors)")
  if down.rate.total == 0:
    cfg.failHard("no download bytes moved (" & $down.errors & " errors)")

  mixedReport(cfg.label & " final", reqCounter, wsCounter, sseCounter, up, down,
              start, start + elapsed, final = true, opsRan = opsRan)
  await chaosFinish(chaos, leakBase, cfg, apis)  # close all clients, drain, leak assert
  echo "== mixed ", backend, " ", cfg.proto, " passed (req ", reqCounter.ops,
    " ops ", fmtRate(reqCounter.ops, opsRan), " ops/s, ws ", wsCounter.ops,
    " round-trips, sse ", sseCounter.ops, " events, up ",
    megabytes(up.rate.total), "MB tx ", up.transfers, " transfers, down ",
    megabytes(down.rate.total), "MB rx ", down.transfers, " transfers) =="

waitFor main()
