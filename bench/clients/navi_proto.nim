## Benchmark client: navi across protocols (h1/h2/h3), cold and pooled. Reads
## NAVI_BENCH_PROTO (h1|h2|h3), NAVI_BENCH_COLD (0|1), NAVI_BENCH_ITERS, and
## NAVI_BENCH_URL, and emits a RESULT line. Uses the asyncdispatch backend, which
## speaks all three protocols. Built with -d:naviHttp3 so h3 is available.
##
## - pooled: one client; connections are reused (h1 keep-alive, h2/h3 mux).
## - cold:   a fresh client per request, so every request pays full connection
##           setup. For h3, a request first does an h1/h2 round trip to discover
##           Alt-Svc, then the QUIC handshake -- so "h3 / cold" is discovery +
##           handshake per request, by the nature of h3 (labelled honestly).
import std/[os, strutils, monotimes, times, asyncdispatch]
import navi/asyncdispatch

let url = getEnv("NAVI_BENCH_URL", "https://localhost:4433/")
let proto = getEnv("NAVI_BENCH_PROTO", "h2")
let cold = getEnv("NAVI_BENCH_COLD", "0") == "1"
let iters = parseInt(getEnv("NAVI_BENCH_ITERS", "500"))

proc mkCfg(): NaviConfig =
  result = initNaviConfig()
  result.tls.verify = false
  result.http =
    case proto
    of "h1": {H1}
    of "h2": {H1, H2}
    of "h3": {H1, H2, H3}
    else: {H1, H2}

proc get1(api: Navi): Future[Response] {.async.} =
  result = await api.get(url)

proc main() {.async.} =
  let want =
    case proto
    of "h1": "HTTP/1.1"
    of "h2": "HTTP/2"
    of "h3": "HTTP/3"
    else: ""
  var got = ""
  var done = 0

  # warm up (and, for h3 pooled, establish the h3 connection via Alt-Svc)
  let warm = if cold: 3 else: max(50, iters div 10)
  block warmup:
    let api = newNavi(mkCfg())
    for _ in 0 ..< warm:
      if proto == "h3": discard await get1(api)   # discovery
      discard await get1(api)
    await api.close()

  let t0 = getMonoTime()
  if cold:
    for _ in 0 ..< iters:
      let api = newNavi(mkCfg())
      if proto == "h3": discard await get1(api)   # h1/h2 discovery, then h3
      let r = await get1(api)
      if r.status == 200: inc done
      got = r.httpVersion
      await api.close()
  else:
    let api = newNavi(mkCfg())
    if proto == "h3":
      discard await get1(api); discard await get1(api)   # establish h3
    for _ in 0 ..< iters:
      let r = await get1(api)
      if r.status == 200: inc done
      got = r.httpVersion
    await api.close()
  let secs = (getMonoTime() - t0).inNanoseconds.float / 1e9

  if want.len > 0 and got != want:
    stderr.writeLine "WARN navi-" & proto & ": wanted " & want & " but got " & got
  echo "RESULT\tnavi-", proto, "\t", done, "\t", formatFloat(secs, ffDecimal, 3),
       "\t", formatFloat(done.float / secs, ffDecimal, 0)

waitFor main()
