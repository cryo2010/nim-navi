## Benchmark client: navi, sync backend. One timed loop of every HTTP method
## over a pooled TLS connection; navi decompresses the gzip response by default.
import std/[os, strutils, monotimes, times]
import navi

let url = getEnv("NAVI_BENCH_URL", "https://127.0.0.1:8443")
let iters = parseInt(getEnv("NAVI_BENCH_ITERS", "3000"))
let warmup = max(100, iters div 10)
const body = "x".repeat(256)

var cfg = initNaviConfig()
cfg.tls.verify = false            # self-signed target; TLS crypto still exercised
let api = newNavi(cfg)

proc oneIter() =
  discard api.get(url & "/get")
  discard api.post(url & "/post", body = body)
  discard api.put(url & "/put", body = body)
  discard api.patch(url & "/patch", body = body)
  discard api.delete(url & "/delete")
  discard api.head(url & "/get")
  discard api.options(url & "/get")

for _ in 0 ..< warmup: oneIter()
let t0 = getMonoTime()
for _ in 0 ..< iters: oneIter()
let secs = (getMonoTime() - t0).inNanoseconds.float / 1e9
let reqs = iters * 7
echo "RESULT\tnavi-sync\t", reqs, "\t", formatFloat(secs, ffDecimal, 3), "\t",
  formatFloat(reqs.float / secs, ffDecimal, 0)
