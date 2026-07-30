## Benchmark client: navi, asyncdispatch backend. NAVI_BENCH_COLD=1 forces a fresh
## TCP+TLS connection per request. NAVI_BENCH_CONC>1 keeps that many requests in
## flight concurrently on one client (the async pool opens parallel connections) --
## the event-loop concurrency a sequential loop never exercises.
import std/[os, strutils, monotimes, times]
import navi/asyncdispatch

let url = getEnv("NAVI_BENCH_URL", "https://127.0.0.1:8443")
let iters = parseInt(getEnv("NAVI_BENCH_ITERS", "3000"))
let cold = getEnv("NAVI_BENCH_COLD", "0") == "1"
let conc = parseInt(getEnv("NAVI_BENCH_CONC", "1"))
const body = "x".repeat(256)

proc oneIter(api: Navi) {.async.} =
  discard await api.get(url & "/get")
  discard await api.post(url & "/post", body = body)
  discard await api.put(url & "/put", body = body)
  discard await api.patch(url & "/patch", body = body)
  discard await api.delete(url & "/delete")
  discard await api.head(url & "/get")
  discard await api.options(url & "/get")

proc report(reqs: int, secs: float) =
  echo "RESULT\tnavi-async\t", reqs, "\t", formatFloat(secs, ffDecimal, 3), "\t",
    formatFloat(reqs.float / secs, ffDecimal, 0)

proc worker(api: Navi, per: int) {.async.} =
  for _ in 0 ..< per: await oneIter(api)

proc main() {.async.} =
  var cfg = initNaviConfig()
  cfg.tls.verify = false
  if cold: cfg.headers["connection"] = "close"
  let api = newNavi(cfg)
  if conc > 1:
    let per = max(1, iters div conc)
    for _ in 0 ..< min(20, per): await oneIter(api)   # warmup
    let t0 = getMonoTime()
    var workers: seq[Future[void]]
    for _ in 0 ..< conc: workers.add worker(api, per)
    await all(workers)
    report(per * conc * 7, (getMonoTime() - t0).inNanoseconds.float / 1e9)
  else:
    let warmup = if cold: min(20, iters) else: max(100, iters div 10)
    for _ in 0 ..< warmup: await oneIter(api)
    let t0 = getMonoTime()
    for _ in 0 ..< iters: await oneIter(api)
    report(iters * 7, (getMonoTime() - t0).inNanoseconds.float / 1e9)

waitFor main()
