## benchStreamUpload, sync backend (name: navi-sync). Sequential streamed uploads,
## verifying SHA-1/size against the server. Records per-transfer latency + bytes.

import std/[times, monotimes, json]
import ../common/[config, reporter, servers, streamcontent, runner]
import navi
include ../common/httpset

let blk = fillBlock()

proc ulThread(a: ptr BenchThread) {.thread, nimcall.} =
  # One blocking navi client per thread; navi keeps no shared mutable globals, so any
  # gcsafe complaints are false positives from indirect callbacks (and read-only blk).
  {.gcsafe.}:
    let cfg = a.cfg
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
    while epochTime() < a.deadline:
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
        if epochTime() >= a.measureStart:
          rec.record((getMonoTime() - t0).inMicroseconds, sent)
      except CatchableError as e:
        rec.fail(); stderr.writeLine cfg.label & " FAIL: " & $e.name & ": " & e.msg; quit(1)
    a.rec = rec

proc main() =
  let cfg = loadConfig("sync")
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  runThreaded(cfg, "navi-sync", ulThread)

main()
