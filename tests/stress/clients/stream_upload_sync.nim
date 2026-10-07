## stressStreamUpload, sync backend (`import navi`). Streams `streamBytes` up to
## /upload as a pull-based chunked body (constant memory), hashing as it flies, and
## compares to the server's SHA-1. Mismatch FAILS HARD (exit 1). Repeats while time
## remains.
##
## There is no concurrent reporter on the sync backend, so the report line is emitted
## from inside the body producer on the report cadence. The `StreamRate` is created
## once in main and passed in, so the cumulative total and the interval MB/s span
## every transfer instead of restarting at zero on each one.

import std/[times, json]
import ../common/[config, reporter, servers, streamcontent, leakcheck]
import navi
include ../common/httpset
include ../common/chaos

let blkBase = fillBlock()

proc oneUpload(api: Navi, cfg: Config, rate: StreamRate, url: string) =
  var st = newSha1State()
  var remaining = cfg.streamBytes
  var sent = 0
  var idx = 0
  var blk = blkBase   # local mutable copy so each block can be index-stamped
  var h = initHeaders()
  h["content-type"] = "application/octet-stream"

  let res = api.request(POST, url, headers = h,
    body = BodyProducer(proc(): string =
      if remaining <= 0: return ""
      let n = min(blockSize, remaining)
      remaining -= n
      stampBlock(blk, idx); inc idx   # distinct per block: server catches a reorder/dup
      let chunk = if n == blockSize: blk else: blk[0 ..< n]
      st.update(chunk)
      sent += n
      rate.add n
      let now = epochTime()
      if rate.due(now, cfg.reportSeconds):
        # "0 retried" is a constant here: the sync client has no retry loop, an
        # exception propagates. It stays in the line so sync and async parse alike.
        let (mb, mbps) = rate.mark(now)
        echo cfg.label, " ", mb, "MB tx | ", mbps, " MB/s | ",
             rate.transfers, " done | 0 retried | RSS ", fmtBytes(rssBytes()),
             " | heap ", fmtBytes(getOccupiedMem())
      chunk))

  if res.status != 200:
    stderr.writeLine cfg.label & " FAIL: /upload -> " & $res.status
    quit(1)
  cfg.checkVersion(res.httpVersion)   # hard-fail if the streamed upload downgraded
  let clientSha = st.hex
  let j = parseJson(res.body)
  if j{"sha1"}.getStr != clientSha or j{"size"}.getInt != sent:
    stderr.writeLine cfg.label & " FAIL: checksum mismatch\n" &
      "  sent " & $sent & " bytes, client sha1=" & clientSha & "\n" &
      "  server got " & $j{"size"}.getInt & " bytes, sha1=" & j{"sha1"}.getStr
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

  # Warm up per-origin protocol discovery (h3 via Alt-Svc) before the measured uploads.
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
    oneUpload(api, cfg, rate, pool.pick() & "/upload")
    inc rate.transfers
    # one interleaved chaos interaction per transfer: a 1 GiB stream is long, so
    # even at every transfer the chaos:verified ratio stays modest.
    if sc.active: syncChaosStep(sc)
    if epochTime() >= deadline: break
  # Measure the run before the chaos/leak settle phase, which moves no bytes.
  let ran = rate.elapsed(epochTime())
  syncChaosFinish(sc, @[api])
  echo "== streamUpload sync passed (",
       summary(rate.total, ran, "tx", rate.transfers, 0), ") =="

main()
