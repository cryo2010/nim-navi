## stressStreamDownload, async backends (built twice: asyncdispatch, -d:useChronos).
##
## Streams `streamBytes` (default 1 GiB) down from /download and hashes each chunk
## incrementally, discarding it (never buffering the file), then compares to the
## server's `x-sha1`. A checksum mismatch FAILS HARD (exit 1). A transient transport
## error (e.g. the server recycled/idle-closed a pooled connection mid-transfer) is
## retried, not fatal. Repeats while time remains; a background reporter prints
## cumulative megabytes, the interval's MB/s and RSS on the interval, regardless of
## per-transfer duration (one 1 GiB transfer is minutes long, so a transfer count is
## far too coarse to be the headline number).

import std/[times, strutils]
import ../common/[config, reporter, servers, streamcontent, leakcheck]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"
include ../common/httpset
include ../common/chaos
include parts/stream_download_part   # one verified download (shared with mixed.nim)

proc reporterLoop(cfg: Config, prog: StreamProgress, deadline: float) {.async.} =
  while epochTime() < deadline:
    await sleep(1000)                  # 1s granularity: stop within ~1s of the deadline
    let now = epochTime()
    if prog.rate.due(now, cfg.reportSeconds):
      # MB/s is over the window since the previous line, not the whole run, so a
      # mid-soak slowdown shows up instead of being averaged away.
      let (mb, mbps) = prog.rate.mark(now)
      echo cfg.label, " ", mb, "MB rx | ", mbps, " MB/s | ",
           prog.transfers, " done | ", prog.errors, " retried | RSS ",
           fmtBytes(rssBytes()), " | heap ", fmtBytes(getOccupiedMem())

proc main() {.async.} =
  let cfg = loadConfig(backend)
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  let notice = cfg.chaosSkipNotice
  if notice.len > 0: echo cfg.label, " ", notice
  var leakBase = sampleBaseline(cfg.chaos)   # before ANY Navi is constructed
  var pool = initServerPool(cfg)

  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)     # honor NAVI_PROTO (h3 was previously ignored here)
  c.tls.caFile = cfg.cert
  let api = newNavi(c)

  # Warm up h3 discovery per origin (an Alt-Svc round-trip) before the measured phase.
  let expect = cfg.expectedVersion
  if expect.len > 0:
    for base in pool.all():
      for _ in 0 ..< 3:
        try:
          if (await api.request(GET, base & "/echo")).httpVersion == expect: break
        except CatchableError: break

  let start = epochTime()
  let deadline = start + cfg.seconds
  let chaos = chaosMaybeStart(cfg, deadline, cfg.reportSeconds)  # no-op when off
  let prog = StreamProgress(rate: newStreamRate(start))
  let rep = reporterLoop(cfg, prog, deadline)
  while epochTime() < deadline:
    try:
      await oneDownload(api, cfg, prog, pool.pick() & "/download?size=" & $cfg.streamBytes)
      inc prog.transfers
    except CatchableError as e:        # transient (recycled/idle-closed conn): retry
      inc prog.errors
      stderr.writeLine cfg.label & " transfer retried: " & e.msg
  await rep
  # Measure the run before the chaos/leak settle phase, which moves no bytes and
  # would otherwise drag the average rate down.
  let ran = prog.rate.elapsed(epochTime())
  await chaosAwait(chaos)

  if prog.transfers == 0:
    stderr.writeLine cfg.label & " FAIL: no transfer completed (" & $prog.errors & " errors)"
    quit(1)
  await chaosFinish(chaos, leakBase, cfg, @[api])
  echo "== streamDownload ", backend, " passed (",
       summary(prog.rate.total, ran, "rx", prog.transfers, prog.errors), ") =="

waitFor main()
