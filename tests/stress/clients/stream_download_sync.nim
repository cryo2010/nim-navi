## stressStreamDownload, sync backend (`import navi`). Streams `streamBytes` down
## from /download, hashing each chunk and discarding it (never buffered), then
## compares to the server's x-sha1. Mismatch FAILS HARD (exit 1). Repeats while
## time remains.
##
## There is no concurrent reporter on the sync backend, so the report line is emitted
## from inside the `each` callback on the report cadence. The `StreamRate` is created
## once in main and passed in, so the cumulative total and the interval MB/s span
## every transfer instead of restarting at zero on each one.

import std/[times, strutils]
import ../common/[config, reporter, servers, streamcontent, leakcheck]
import navi
include ../common/httpset
include ../common/chaos

proc oneDownload(api: Navi, cfg: Config, rate: StreamRate, url: string) =
  var st = newSha1State()
  var got = 0
  let res = api.stream.get(url)
  if res.status != 200:
    stderr.writeLine cfg.label & " FAIL: /download -> " & $res.status
    quit(1)
  cfg.checkVersion(res.httpVersion)   # hard-fail on a silent protocol downgrade
  let expected = res.headers.get("x-sha1").toLowerAscii
  res.each(chunk):
    if chunk.len > 0:
      st.update(chunk)
      got += chunk.len
      rate.add chunk.len
      let now = epochTime()
      if rate.due(now, cfg.reportSeconds):
        # "0 retried" is a constant here: the sync client has no retry loop, an
        # exception propagates. It stays in the line so sync and async parse alike.
        let (mb, mbps) = rate.mark(now)
        echo cfg.label, " ", mb, "MB rx | ", mbps, " MB/s | ",
             rate.transfers, " done | 0 retried | RSS ", fmtBytes(rssBytes()),
             " | heap ", fmtBytes(getOccupiedMem())

  let clientSha = st.hex
  if clientSha != expected:
    stderr.writeLine cfg.label & " FAIL: checksum mismatch\n" &
      "  got " & $got & " bytes, client sha1=" & clientSha & "\n" &
      "  server x-sha1=" & expected
    quit(1)

proc main() =
  let cfg = loadConfig("sync")
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

  # Warm up per-origin protocol discovery (h3 via Alt-Svc) before the measured phase.
  let expect = cfg.expectedVersion
  if expect.len > 0:
    for base in pool.all():
      for _ in 0 ..< 3:
        try:
          if api.request(GET, base & "/echo").httpVersion == expect: break
        except CatchableError: break

  let start = epochTime()
  let deadline = start + cfg.seconds
  var sc = syncChaosStart(cfg, leakBase)     # no-op when chaos is off
  let rate = newStreamRate(start)
  while true:
    oneDownload(api, cfg, rate, pool.pick() & "/download?size=" & $cfg.streamBytes)
    inc rate.transfers
    if sc.active: syncChaosStep(sc)          # interleave one chaos interaction per transfer
    if epochTime() >= deadline: break
  # Measure the run before the chaos/leak settle phase, which moves no bytes.
  let ran = rate.elapsed(epochTime())
  syncChaosFinish(sc, @[api])
  echo "== streamDownload sync passed (",
       summary(rate.total, ran, "rx", rate.transfers, 0), ") =="

main()
