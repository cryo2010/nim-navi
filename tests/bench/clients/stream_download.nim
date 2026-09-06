## benchStreamDownload, async backends (built twice: asyncdispatch=navi-async,
## -d:useChronos=navi-chronos). Fans out `clients` x `concurrency` workers streaming
## `streamBytes` down from /download, hashing each chunk incrementally and discarding
## it (constant memory), verifying the SHA-1 against the server's x-sha1 (hard-fail on
## mismatch). Records per-transfer latency + cumulative bytes for MB/s over a
## time-boxed window after an unmeasured warmup. Emits one RESULT line.

import std/[times, monotimes, strutils]
import ../common/[config, reporter, servers, streamcontent, runner]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
  const clientName = "navi-chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"
  const clientName = "navi-async"
include ../common/httpset

proc oneDownload(api: Navi, cfg: Config, url: string): Future[int] {.async.} =
  var st = newSha1State()
  var got = 0
  let res = await api.stream(GET, url)
  if res.status != 200:
    stderr.writeLine cfg.label & " FAIL: /download -> " & $res.status
    quit(1)
  cfg.checkVersion(res.httpVersion)
  let expected = res.headers.get("x-sha1").toLowerAscii
  res.each(chunk):
    if chunk.len > 0:
      st.update(chunk)                 # hash then discard: never buffered
      got += chunk.len
  if st.hex != expected:
    stderr.writeLine cfg.label & " FAIL: checksum mismatch (" & $got & " bytes)"
    quit(1)
  return got

proc worker(api: Navi, cfg: Config, pool: ptr ServerPool, rec: BenchRecorder,
            measureStart, deadline: float) {.async.} =
  while epochTime() < deadline:
    let url = pool[].pick() & "/download?size=" & $cfg.streamBytes
    let t0 = getMonoTime()
    try:
      let got = await oneDownload(api, cfg, url)
      if epochTime() >= measureStart:
        rec.record((getMonoTime() - t0).inMicroseconds, got)
    except CatchableError as e:
      rec.fail()
      stderr.writeLine cfg.label & " FAIL: " & $e.name & ": " & e.msg
      quit(1)

proc mkClient(cfg: Config): Navi =
  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)
  c.tls.caFile = cfg.cert
  newNavi(c)

proc dlThread(a: ptr BenchThread) {.thread, nimcall.} =
  # One navi client per thread on its own event loop; navi keeps no shared mutable
  # globals, so the gcsafe complaints are false positives from indirect callbacks.
  {.gcsafe.}:
    let cfg = a.cfg
    var pool = initServerPool(cfg)
    let api = mkClient(cfg)

    let expect = cfg.expectedVersion
    if expect.len > 0:
      for base in pool.all():
        for _ in 0 ..< 3:
          try:
            if (waitFor api.request(GET, base & "/echo")).httpVersion == expect: break
          except CatchableError: break

    let rec = newBenchRecorder()
    var futs: seq[Future[void]]
    for _ in 0 ..< a.concurrency:
      futs.add worker(api, cfg, addr pool, rec, a.measureStart, a.deadline)
    for f in futs: waitFor f
    a.rec = rec

proc main() =
  let cfg = loadConfig(backend)
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  runThreaded(cfg, clientName, dlThread)

main()
