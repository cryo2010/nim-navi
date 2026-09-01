## benchStreamDownload, navi/js backend (Node; name: navi-js). Fans out
## `clients` x `concurrency` workers streaming `streamBytes` down from /download,
## hashing each chunk with Node's native SHA-1 and verifying against x-sha1 (hard-fail
## on mismatch). Records per-transfer latency + bytes for MB/s over a timed window.
## (js cannot stream request bodies, so there is no streamUpload js client.)

import navi/js
import ../common/harness_js

proc jsExit(code: int) {.importjs: "process.exit(#)".}

type Sha1 = ref object
proc createSha1(): Sha1 {.importjs: "require('crypto').createHash('sha1')".}
proc update(h: Sha1, chunk: seq[byte]) {.importjs: "#.update(Buffer.from(#))".}
proc digestHex(h: Sha1): cstring {.importjs: "#.digest('hex')".}

proc main() {.async.} =
  let cfg = loadJsCfg()
  var pool = initJsPool(cfg)
  let rec = newJsBench()
  let label = "[streamDownload " & cfg.proto & " js]"
  var apis: seq[Navi]
  for _ in 0 ..< cfg.clients:
    var c = initNaviConfig()
    apis.add newNavi(c)

  let startMs = nowMs()
  let measureStartMs = startMs + cfg.warmupSeconds * 1000.0
  let deadlineMs = measureStartMs + cfg.seconds * 1000.0

  proc oneDownload(api: Navi, url: string): Future[int] {.async.} =
    let h = createSha1()
    var got = 0
    let res = await api.stream(GET, url)
    if res.status != 200:
      echo label, " FAIL: /download -> ", res.status; jsExit(1)
    let expected = res.headers.get("x-sha1")
    res.each(chunk):
      if chunk.len > 0:
        h.update(chunk); got += chunk.len
    if $h.digestHex() != expected:
      echo label, " FAIL: checksum mismatch (", got, " bytes)"; jsExit(1)
    return got

  proc worker(api: Navi) {.async.} =
    while nowMs() < deadlineMs:
      let url = pool.pick() & "/download?size=" & $cfg.streamBytes
      let t0 = nowUs()
      try:
        let got = await oneDownload(api, url)
        if nowMs() >= measureStartMs: rec.record(nowUs() - t0, got.float)
      except CatchableError as e:
        rec.note()
        echo label, " FAIL: ", e.msg; jsExit(1)

  var futs: seq[Future[void]]
  for api in apis:
    for _ in 0 ..< cfg.concurrency:
      futs.add worker(api)
  for f in futs: await f

  rec.emitResult("navi-js", cfg.seconds)

discard main()
