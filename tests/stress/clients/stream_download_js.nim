## stressStreamDownload, navi/js backend (Node). Streams `streamBytes` down from
## /download, hashing each chunk with Node's native SHA-1 (crypto), then compares
## to the server's x-sha1. Mismatch FAILS HARD (process.exit(1)). Repeats while
## time remains. (js cannot stream uploads, so there is no streamUpload js client.)

import navi/js
import ../common/harness_js

proc jsExit(code: int) {.importjs: "process.exit(#)".}

# Node's native SHA-1 (C-speed) -- Nim's checksums/sha1 uses copyMem and does not
# compile to js.
type Sha1 = ref object
proc createSha1(): Sha1 {.importjs: "require('crypto').createHash('sha1')".}
proc update(h: Sha1, chunk: seq[byte]) {.importjs: "#.update(Buffer.from(#))".}
proc digestHex(h: Sha1): cstring {.importjs: "#.digest('hex')".}

proc oneDownload(api: Navi, cfg: JsCfg, url: string): Future[int] {.async.} =
  let h = createSha1()
  var got = 0
  let res = await api.stream(GET, url)
  if res.status != 200:
    echo "[streamDownload js] FAIL: /download -> ", res.status
    jsExit(1)
  let expected = res.headers.get("x-sha1")
  res.each(chunk):                       # chunk: seq[byte] under js
    if chunk.len > 0:
      h.update(chunk)
      got += chunk.len
  let clientSha = $h.digestHex()
  if clientSha != expected:
    echo "[streamDownload js] FAIL: checksum mismatch got ", got,
         " bytes client=", clientSha, " server=", expected
    jsExit(1)
  return got

proc main() {.async.} =
  let cfg = loadJsCfg()
  var pool = initJsPool(cfg)
  var c = initNaviConfig()
  let api = newNavi(c)

  let start = nowMs()
  let deadline = start + cfg.seconds * 1000.0
  var transfers = 0
  var bytes = 0                          # cumulative bytes rx, for the reporter
  proc reportMem() =
    echo "[streamDownload js] ", bytes div (1 shl 20), "MB rx | ", transfers,
         " done | RSS ", rssMb(), "MB | heap ", heapUsedMb(), "MB | t=",
         int((nowMs() - start) / 1000.0), "s"
  let timer = setIntervalJs(reportMem, cfg.reportSeconds * 1000)
  while true:
    bytes += await oneDownload(api, cfg, pool.pick() & "/download?size=" & $cfg.streamBytes)
    inc transfers
    if nowMs() >= deadline: break
  clearIntervalJs(timer)
  echo "== streamDownload js passed (", transfers, " x ", cfg.streamBytes, " bytes) =="

discard main()
