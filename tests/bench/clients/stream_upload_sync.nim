## benchStreamUpload, sync backend (name: navi-sync). Sequential streamed uploads,
## verifying SHA-1/size against the server. Records per-transfer latency + bytes.

import std/[times, monotimes, json]
import ../common/[config, reporter, servers, streamcontent]
import navi
include ../common/httpset

let blk = fillBlock()

proc main() =
  let cfg = loadConfig("sync")
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  var pool = initServerPool(cfg)
  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)
  c.tls.caFile = cfg.cert
  let api = newNavi(c)

  let expect = cfg.expectedVersion
  if expect.len > 0:
    for base in pool.all():
      for _ in 0 ..< 3:
        try:
          if api.request(GET, base & "/echo").httpVersion == expect: break
        except CatchableError: break

  let rec = newBenchRecorder()
  let start = epochTime()
  let measureStart = start + cfg.warmupSeconds
  let deadline = measureStart + cfg.seconds
  while epochTime() < deadline:
    let url = pool.pick() & "/upload"
    let t0 = getMonoTime()
    var st = newSha1State()
    var remaining = cfg.streamBytes
    var sent = 0
    var h = initHeaders()
    h["content-type"] = "application/octet-stream"
    try:
      let res = api.request(POST, url, headers = h,
        bodyStream = proc(): string =
          if remaining <= 0: return ""
          let n = min(blockSize, remaining)
          remaining -= n
          let chunk = if n == blockSize: blk else: blk[0 ..< n]
          st.update(chunk); sent += n
          chunk)
      if res.status != 200:
        stderr.writeLine cfg.label & " FAIL: /upload -> " & $res.status; quit(1)
      cfg.checkVersion(res.httpVersion)
      let j = parseJson(res.body)
      if j{"sha1"}.getStr != st.hex or j{"size"}.getInt != sent:
        stderr.writeLine cfg.label & " FAIL: checksum mismatch (sent " & $sent & " bytes)"; quit(1)
      if epochTime() >= measureStart:
        rec.record((getMonoTime() - t0).inMicroseconds, sent)
    except CatchableError as e:
      rec.fail(); stderr.writeLine cfg.label & " FAIL: " & $e.name & ": " & e.msg; quit(1)

  emitResult("navi-sync", rec, cfg.seconds)

main()
