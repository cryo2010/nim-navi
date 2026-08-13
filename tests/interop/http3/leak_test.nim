## Leak coverage for the async HTTP/3 path. Measures fd and heap deltas ACROSS a
## loop (after a warmup that allocates the one-time ngtcp2/nghttp3/OpenSSL
## globals), so only per-iteration growth -- a real navi leak -- is flagged; no
## library suppressions needed. Two workloads: connection churn (open+close each
## iteration -> UDP socket / connection teardown) and mux reuse (many concurrent
## streams on one connection -> per-stream teardown). Also run under ASan/UBSan
## (detect_leaks=0) by run.sh to catch memory-safety errors in the driver/reader.
import std/[os, asyncdispatch]
import navi/asyncdispatch

proc fdCount(): int =
  for _ in walkDir("/proc/self/fd"): inc result

proc oneClientRequest(ca: string) {.async.} =
  var cfg = initNaviConfig()
  cfg.tls.caFile = ca
  cfg.http = {H1, H2, H3}
  let api = newNavi(cfg)
  discard await api.get("https://localhost:4433/")            # h2, learn Alt-Svc
  let r = await api.get("https://localhost:4433/big")         # h3
  doAssert r.httpVersion == "HTTP/3"
  await api.close()

proc churn(ca: string, iters: int) {.async.} =
  for i in 1 .. iters: await oneClientRequest(ca)

proc muxBurst(api: Navi, iters, conc: int) {.async.} =
  for i in 1 .. iters:
    var futs: seq[Future[Response]]
    for j in 0 ..< conc: futs.add api.get("https://localhost:4433/big")
    for r in await all(futs): doAssert r.httpVersion == "HTTP/3"

proc main() {.async.} =
  let ca = getEnv("NAVI_H3_CA")
  doAssert ca.len > 0, "NAVI_H3_CA must point at the origin cert"

  # 1. Connection churn: a fresh client + h3 connection every iteration.
  await churn(ca, 3)                                          # warmup
  GC_fullCollect()
  let (cfd0, cmem0) = (fdCount(), getOccupiedMem())
  await churn(ca, 20)
  GC_fullCollect()
  let (cfd1, cmem1) = (fdCount(), getOccupiedMem())
  echo "churn: fd ", cfd0, " -> ", cfd1, "  mem ", cmem0, " -> ", cmem1
  doAssert cfd1 <= cfd0, "fd leak in connection churn: " & $(cfd1 - cfd0)
  doAssert cmem1 - cmem0 < 512 * 1024, "heap grew " & $(cmem1 - cmem0) & " over churn"

  # 2. Mux reuse: many concurrent streams on one reused connection.
  var cfg = initNaviConfig()
  cfg.tls.caFile = ca
  cfg.http = {H1, H2, H3}
  let api = newNavi(cfg)
  discard await api.get("https://localhost:4433/")
  discard await api.get("https://localhost:4433/big")        # establish h3 conn
  await muxBurst(api, 2, 8)                                   # warmup
  GC_fullCollect()
  let (mfd0, mmem0) = (fdCount(), getOccupiedMem())
  await muxBurst(api, 10, 8)
  GC_fullCollect()
  let (mfd1, mmem1) = (fdCount(), getOccupiedMem())
  echo "mux:   fd ", mfd0, " -> ", mfd1, "  mem ", mmem0, " -> ", mmem1
  doAssert mfd1 <= mfd0, "fd leak in mux reuse: " & $(mfd1 - mfd0)
  doAssert mmem1 - mmem0 < 512 * 1024, "heap grew " & $(mmem1 - mmem0) & " over mux"
  await api.close()

  echo "NAVI HTTP/3 LEAK OK"

waitFor main()
