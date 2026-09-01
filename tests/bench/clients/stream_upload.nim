## benchStreamUpload, async backends (navi-async / navi-chronos). Fans out
## `clients` x `concurrency` workers streaming `streamBytes` up to /upload as a
## pull-based chunked body (constant memory: one reused 1 MiB block, hashed as it
## flies), verifying the client's SHA-1/size against the server's (hard-fail on
## mismatch). Records per-transfer latency + bytes for MB/s over a time-boxed window.

import std/[times, monotimes, json]
import ../common/[config, reporter, servers, streamcontent]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
  const clientName = "navi-chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"
  const clientName = "navi-async"
include ../common/httpset

let blk = fillBlock()

proc oneUpload(api: Navi, cfg: Config, url: string): Future[int] {.async.} =
  var st = newSha1State()
  var remaining = cfg.streamBytes
  var sent = 0
  var h = initHeaders()
  h["content-type"] = "application/octet-stream"
  let res = await api.request(POST, url, headers = h,
    bodyStream = proc(): string =
      if remaining <= 0: return ""
      let n = min(blockSize, remaining)
      remaining -= n
      let chunk = if n == blockSize: blk else: blk[0 ..< n]
      st.update(chunk)
      sent += n
      chunk)
  if res.status != 200:
    stderr.writeLine cfg.label & " FAIL: /upload -> " & $res.status
    quit(1)
  cfg.checkVersion(res.httpVersion)
  let j = parseJson(res.body)
  if j{"sha1"}.getStr != st.hex or j{"size"}.getInt != sent:
    stderr.writeLine cfg.label & " FAIL: checksum mismatch (sent " & $sent & " bytes)"
    quit(1)
  return sent

proc worker(api: Navi, cfg: Config, pool: ptr ServerPool, rec: BenchRecorder,
            measureStart, deadline: float) {.async.} =
  while epochTime() < deadline:
    let url = pool[].pick() & "/upload"
    let t0 = getMonoTime()
    try:
      let sent = await oneUpload(api, cfg, url)
      if epochTime() >= measureStart:
        rec.record((getMonoTime() - t0).inMicroseconds, sent)
    except CatchableError as e:
      rec.fail()
      stderr.writeLine cfg.label & " FAIL: " & $e.name & ": " & e.msg
      quit(1)

proc mkClient(cfg: Config): Navi =
  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)
  c.tls.caFile = cfg.cert
  newNavi(c)

proc main() {.async.} =
  let cfg = loadConfig(backend)
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  var pool = initServerPool(cfg)
  var apis: seq[Navi]
  for _ in 0 ..< cfg.clients: apis.add mkClient(cfg)

  let expect = cfg.expectedVersion
  if expect.len > 0:
    for api in apis:
      for base in pool.all():
        for _ in 0 ..< 3:
          try:
            if (await api.request(GET, base & "/echo")).httpVersion == expect: break
          except CatchableError: break

  let rec = newBenchRecorder()
  let start = epochTime()
  let measureStart = start + cfg.warmupSeconds
  let deadline = measureStart + cfg.seconds
  var futs: seq[Future[void]]
  for api in apis:
    for _ in 0 ..< cfg.concurrency:
      futs.add worker(api, cfg, addr pool, rec, measureStart, deadline)
  for f in futs: await f

  emitResult(clientName, rec, cfg.seconds)

waitFor main()
