## stressStreamDownload, navi/js backend (Node). Streams `streamBytes` down from
## /download, hashing each chunk with Node's native SHA-1 (crypto), then compares
## to the server's x-sha1. Mismatch FAILS HARD (process.exit(1)). Repeats while
## time remains. (js cannot stream uploads, so there is no streamUpload js client.)
## Reports cumulative megabytes and the interval's MB/s, matching the native cells.
##
## The verified transfer itself lives in parts/stream_download_js_part, shared with
## clients/mixed_js.nim.

import navi/js
import ../common/[harness_js, streamcontent]   # streamcontent: the shared MB/s math
include parts/stream_download_js_part   # one verified download (shared with mixed_js.nim)

proc main() {.async.} =
  let cfg = loadJsCfg()
  var pool = initJsPool(cfg)
  var c = initNaviConfig()
  let api = newNavi(c)
  let label = "[streamDownload js]"

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
    echo label, " ", megabytes(bytes), "MB rx | ", mbps, " MB/s | ",
         transfers, " done | 0 retried | RSS ", rssMb(), "MB | heap ",
         heapUsedMb(), "MB | t=", int((now - start) / 1000.0), "s"
  let timer = setIntervalJs(reportMem, cfg.reportSeconds * 1000)
  while true:
    bytes += float(await oneDownload(api, label,
      pool.pick() & "/download?size=" & $cfg.streamBytes))
    inc transfers
    if nowMs() >= deadline: break
  clearIntervalJs(timer)
  echo "== streamDownload js passed (",
       summary(bytes, (nowMs() - start) / 1000.0, "rx", transfers, 0), ") =="

discard main()
