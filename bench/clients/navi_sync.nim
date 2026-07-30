## Benchmark client: navi, sync backend. One timed loop of every HTTP method;
## navi decompresses the gzip response by default. NAVI_BENCH_COLD=1 sends
## `Connection: close`, so each request opens a fresh TCP+TLS connection.
## NAVI_BENCH_CONC>1 runs that many worker threads (each its own client) to keep
## that many requests in flight -- sync concurrency via OS threads.
import std/[os, strutils, monotimes, times]
import navi

let url = getEnv("NAVI_BENCH_URL", "https://127.0.0.1:8443")
let iters = parseInt(getEnv("NAVI_BENCH_ITERS", "3000"))
let cold = getEnv("NAVI_BENCH_COLD", "0") == "1"
let conc = parseInt(getEnv("NAVI_BENCH_CONC", "1"))
const body = "x".repeat(256)

proc newClient(): Navi =
  var cfg = initNaviConfig()
  cfg.tls.verify = false            # self-signed target; TLS crypto still exercised
  if cold: cfg.headers["connection"] = "close"
  newNavi(cfg)

proc oneIter(api: Navi, u: string) =
  discard api.get(u & "/get")
  discard api.post(u & "/post", body = body)
  discard api.put(u & "/put", body = body)
  discard api.patch(u & "/patch", body = body)
  discard api.delete(u & "/delete")
  discard api.head(u & "/get")
  discard api.options(u & "/get")

proc report(reqs: int, secs: float) =
  echo "RESULT\tnavi-sync\t", reqs, "\t", formatFloat(secs, ffDecimal, 3), "\t",
    formatFloat(reqs.float / secs, ffDecimal, 0)

proc worker(arg: tuple[per: int, url: string]) {.thread.} =
  # navi's request path is flagged GC-unsafe only because it can invoke middleware
  # closures (this bench configures none); each thread owns an independent client,
  # so the cast is safe. `url` is passed in rather than read from the global.
  {.cast(gcsafe).}:
    let api = newClient()
    for _ in 0 ..< arg.per: oneIter(api, arg.url)
    api.close()

proc runConcurrent() =
  let per = max(1, iters div conc)
  block warmup:
    let a = newClient()
    for _ in 0 ..< min(20, per): oneIter(a, url)
    a.close()
  var threads = newSeq[Thread[tuple[per: int, url: string]]](conc)
  let t0 = getMonoTime()
  for i in 0 ..< conc: createThread(threads[i], worker, (per, url))
  joinThreads(threads)
  report(per * conc * 7, (getMonoTime() - t0).inNanoseconds.float / 1e9)

proc runSequential() =
  let warmup = if cold: min(20, iters) else: max(100, iters div 10)
  let api = newClient()
  for _ in 0 ..< warmup: oneIter(api, url)
  let t0 = getMonoTime()
  for _ in 0 ..< iters: oneIter(api, url)
  report(iters * 7, (getMonoTime() - t0).inNanoseconds.float / 1e9)

if conc > 1: runConcurrent() else: runSequential()
