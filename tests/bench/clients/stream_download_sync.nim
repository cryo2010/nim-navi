## benchStreamDownload, sync backend (name: navi-sync). Sequential: one transfer at
## a time, hashing each chunk and verifying against x-sha1. Records per-transfer
## latency + bytes for MB/s over a time-boxed window.

import std/[times, monotimes, strutils]
import ../common/[config, reporter, servers, streamcontent, runner]
import navi
include ../common/httpset

proc dlThread(a: ptr BenchThread) {.thread, nimcall.} =
  # One blocking navi client per thread; navi keeps no shared mutable globals, so any
  # gcsafe complaints are false positives from indirect callbacks.
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
      let url = pool.pick() & "/download?size=" & $cfg.streamBytes
      let t0 = getMonoTime()
      var st = newSha1State()
      var got = 0
      try:
        let res = api.stream(GET, url)
        if res.status != 200:
          stderr.writeLine cfg.label & " FAIL: /download -> " & $res.status; quit(1)
        cfg.checkVersion(res.httpVersion)
        let expected = res.headers.get("x-sha1").toLowerAscii
        res.each(chunk):
          if chunk.len > 0:
            st.update(chunk); got += chunk.len
        if st.hex != expected:
          stderr.writeLine cfg.label & " FAIL: checksum mismatch (" & $got & " bytes)"; quit(1)
        if epochTime() >= a.measureStart:
          rec.record((getMonoTime() - t0).inMicroseconds, got)
      except CatchableError as e:
        rec.fail(); stderr.writeLine cfg.label & " FAIL: " & $e.name & ": " & e.msg; quit(1)
    a.rec = rec

proc main() =
  let cfg = loadConfig("sync")
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  runThreaded(cfg, "navi-sync", dlThread)

main()
