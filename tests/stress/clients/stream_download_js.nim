## stressStreamDownload, navi/js backend (Node). Streams `streamBytes` down from
## /download, hashing each chunk with Node's native SHA-1 (crypto), then compares
## to the server's x-sha1. Mismatch FAILS HARD (process.exit(1)). Repeats while
## time remains. (js cannot stream uploads, so there is no streamUpload js client.)
## Reports cumulative megabytes and the interval's MB/s, matching the native cells.

import navi/js
import ../common/[harness_js, streamcontent]   # streamcontent: the shared MB/s math

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
  let res = await api.stream.get(url)
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
  # Float, not int: the js backend overflow-checks int at 2^31, and cumulative
  # bytes crosses 2 GiB within seconds of a soak (~32 x 64 MiB). A JS number holds
  # the running total exactly well past any realistic soak (2^53 bytes = 8 PiB).
  # No StreamRate here for the same reason: its total is an int.
  var bytes = 0.0                        # cumulative bytes rx, for the reporter
  var lastBytes = 0.0                    # `bytes` as of the previous report line
  var lastMs = start                     # timestamp of the previous report line
  proc reportMem() =
    # MB/s over the window since the previous line, not the whole run, so a mid-soak
    # slowdown shows up instead of being averaged away. mbPerSec guards a zero or
    # sub-millisecond window.
    let now = nowMs()
    let mbps = mbPerSec(bytes - lastBytes, (now - lastMs) / 1000.0)
    lastBytes = bytes
    lastMs = now
    echo "[streamDownload js] ", megabytes(bytes), "MB rx | ", mbps, " MB/s | ",
         transfers, " done | 0 retried | RSS ", rssMb(), "MB | heap ",
         heapUsedMb(), "MB | t=", int((now - start) / 1000.0), "s"
  let timer = setIntervalJs(reportMem, cfg.reportSeconds * 1000)
  while true:
    bytes += float(await oneDownload(api, cfg, pool.pick() & "/download?size=" & $cfg.streamBytes))
    inc transfers
    if nowMs() >= deadline: break
  clearIntervalJs(timer)
  echo "== streamDownload js passed (",
       summary(bytes, (nowMs() - start) / 1000.0, "rx", transfers, 0), ") =="

discard main()
