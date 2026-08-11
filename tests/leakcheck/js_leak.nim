## Node-based leak check for the navi/js backend. `nim js` compiles to JavaScript,
## which Valgrind/ASan cannot instrument, so instead of counting bytes at exit we
## sample the V8 heap and the open file-descriptor count around a long scenario
## loop and assert neither grows. Run under `node --expose-gc` (see run-js.sh) so
## global.gc() is available to settle the heap before each sample.
##
## Scenarios (js does h1 over fetch; the runtime owns h2/connection pooling, and
## fetch cannot stream a request body, so http2s / streamup* are not js cells):
##   http1 http1s streamdown streamdownc sse ws wss
import std/[os, strutils]
import navi/js

proc nodeGc() {.importjs: "(typeof global.gc === 'function' && global.gc())".}
proc heapUsed(): int {.importjs: "process.memoryUsage().heapUsed".}
proc fdCount(): int {.importjs: "require('fs').readdirSync('/proc/self/fd').length".}

const payloadBytes = 256 * 1024

proc cfgFor(tls: bool): NaviConfig =
  result = initNaviConfig()
  # Node honours a custom CA via NODE_EXTRA_CA_CERTS (set by run-js.sh), not the
  # navi config, so nothing TLS-specific is needed here.
  discard tls

proc wsUrl(httpBase, path: string): string =
  if httpBase.startsWith("https"): "wss" & httpBase["https".len .. ^1] & path
  else: "ws" & httpBase["http".len .. ^1] & path

let
  scenario = getEnv("NAVI_LEAK_SCENARIO")
  base = getEnv("NAVI_LEAK_BASE")
  baseTls = getEnv("NAVI_LEAK_BASE_TLS")
  iters = parseInt(getEnv("NAVI_LEAK_ITERS", "100"))

proc runOnce(): Future[void] {.async.} =
  case scenario
  of "http1", "http1s":
    let tls = scenario == "http1s"
    let api = newNavi(cfgFor(tls))
    let r = await api.get((if tls: baseTls else: base) & "/get")
    doAssert r.status == 200 and r.body == "ok"
    api.close()
  of "streamdown", "streamdownc":
    let path = if scenario == "streamdownc": "/download-gz" else: "/download"
    let api = newNavi(cfgFor(true))
    let s = await api.stream(GET, baseTls & path)
    doAssert s.status == 200
    var n = 0
    s.each(chunk): n += chunk.len
    doAssert n == payloadBytes, "got " & $n & " bytes"
    api.close()
  of "sse":
    let api = newNavi(cfgFor(true))
    let s = await api.sse(baseTls & "/sse", reconnect = false)
    var c = 0
    s.each(ev): inc c
    doAssert c == 6, "got " & $c & " events"
    s.close()
    api.close()
  of "ws", "wss":
    let tls = scenario == "wss"
    let api = newNavi(cfgFor(tls))
    let ws = await api.websocket(wsUrl(if tls: baseTls else: base, "/ws"))
    for _ in 0 ..< 3:
      await ws.send("ping")
      let m = await ws.receive()
      doAssert m.kind == wmText and m.data == "ping",
        "got kind=" & $m.kind & " data=<" & m.data & ">"
    await ws.close()
    api.close()
  else:
    doAssert false, "unknown/unsupported js scenario: " & scenario

proc main() {.async.} =
  doAssert scenario.len > 0, "NAVI_LEAK_SCENARIO must be set"
  doAssert iters >= 40, "NAVI_LEAK_ITERS must be >= 40 for a stable heap sample"
  let warmup = max(10, iters div 10)
  # Warm up: fills lazy module state / DNS / TLS session cache so it is not counted
  # as a leak.
  for _ in 0 ..< warmup: await runOnce()
  nodeGc(); nodeGc()
  let baseHeap = heapUsed()
  let baseFd = fdCount()
  for _ in 0 ..< iters: await runOnce()
  nodeGc(); nodeGc()
  let endHeap = heapUsed()
  let endFd = fdCount()

  let heapGrowth = endHeap - baseHeap
  let fdGrowth = endFd - baseFd
  echo "js ", scenario, ": iters=", iters,
       " heap ", baseHeap, " -> ", endHeap, " (", heapGrowth, " bytes)",
       " fd ", baseFd, " -> ", endFd
  # A leak-free run reclaims every client/connection: no descriptor may survive,
  # and the heap must not grow beyond GC/JIT noise (2 MiB over the loop).
  doAssert fdGrowth <= 0, "file descriptor leak: " & $fdGrowth & " fds not closed"
  doAssert heapGrowth < 2 * 1024 * 1024,
    "heap grew " & $heapGrowth & " bytes over " & $iters & " iterations (leak)"
  echo "ok: js ", scenario, " x ", iters, " iterations, no heap/fd growth"

discard main()
