## Benchmark client: navi, asyncdispatch backend. Same workload as navi_sync,
## awaited sequentially (per-request async overhead, matched to the sync loop).
## NAVI_BENCH_COLD=1 forces a fresh TCP+TLS connection per request (Connection:
## close), exposing the connection-setup path the pooled run amortizes away.
import std/[os, strutils, monotimes, times]
import navi/asyncdispatch

let url = getEnv("NAVI_BENCH_URL", "https://127.0.0.1:8443")
let iters = parseInt(getEnv("NAVI_BENCH_ITERS", "3000"))
let cold = getEnv("NAVI_BENCH_COLD", "0") == "1"
let warmup = if cold: min(20, iters) else: max(100, iters div 10)
const body = "x".repeat(256)

proc oneIter(api: Navi) {.async.} =
  discard await api.get(url & "/get")
  discard await api.post(url & "/post", body = body)
  discard await api.put(url & "/put", body = body)
  discard await api.patch(url & "/patch", body = body)
  discard await api.delete(url & "/delete")
  discard await api.head(url & "/get")
  discard await api.options(url & "/get")

proc main() {.async.} =
  var cfg = initNaviConfig()
  cfg.tls.verify = false
  if cold: cfg.headers["connection"] = "close"
  let api = newNavi(cfg)
  for _ in 0 ..< warmup: await oneIter(api)
  let t0 = getMonoTime()
  for _ in 0 ..< iters: await oneIter(api)
  let secs = (getMonoTime() - t0).inNanoseconds.float / 1e9
  let reqs = iters * 7
  echo "RESULT\tnavi-async\t", reqs, "\t", formatFloat(secs, ffDecimal, 3), "\t",
    formatFloat(reqs.float / secs, ffDecimal, 0)

waitFor main()
