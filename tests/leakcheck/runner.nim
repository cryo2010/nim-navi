## Per-scenario leak-check harness, compiled once per native target
## (sync / -d:useAsync / -d:useChronos) and run under Valgrind or ASan/UBSan by
## run.sh. It runs NAVI_LEAK_SCENARIO for NAVI_LEAK_ITERS iterations, building a
## fresh client each time and tearing it down fully, so a leak-free navi leaves no
## definite/indirect leak and no open file descriptor at exit.
import std/[os, strutils]

when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
elif defined(useAsync):
  import navi/asyncdispatch
  const backend = "asyncdispatch"
else:
  import navi
  template await(x: untyped): untyped = x   # identity: the sync body reads the same
  const backend = "sync"

const payloadBytes = 256 * 1024

proc cfgFor(cert: string, tls, h1only: bool): NaviConfig =
  result = initNaviConfig()
  if tls: result.tls.caFile = cert
  if h1only: result.http = {H1}

proc wsUrl(httpBase, path: string): string =
  if httpBase.startsWith("https"): "wss" & httpBase["https".len .. ^1] & path
  else: "ws" & httpBase["http".len .. ^1] & path

template runAll() =
  # Read config as locals so the chronos {.async.} main() stays GC-safe (no globals).
  let
    scenario = getEnv("NAVI_LEAK_SCENARIO")
    base = getEnv("NAVI_LEAK_BASE")           # http://localhost:8080
    baseTls = getEnv("NAVI_LEAK_BASE_TLS")    # https://localhost:8443
    cert = getEnv("NAVI_LEAK_CERT")
    iters = parseInt(getEnv("NAVI_LEAK_ITERS", "30"))
  doAssert scenario.len > 0, "NAVI_LEAK_SCENARIO must be set"
  doAssert iters >= 5, "NAVI_LEAK_ITERS must be >= 5"
  for _ in 0 ..< iters:
    case scenario
    of "http1", "http1s":
      let tls = scenario == "http1s"
      let api = newNavi(cfgFor(cert, tls, h1only = true))
      let r = await api.get((if tls: baseTls else: base) & "/get")
      doAssert r.status == 200 and r.body == "ok"
      await api.close()
    of "http2s":
      let api = newNavi(cfgFor(cert, true, h1only = false))
      let r = await api.get(baseTls & "/get")
      doAssert r.status == 200
      when backend != "chronos":
        doAssert r.httpVersion == "HTTP/2", "expected h2, got " & r.httpVersion
      await api.close()
    of "streamup", "streamupc":
      let api = newNavi(cfgFor(cert, true, h1only = false))
      var left = 4
      var h = initHeaders()
      if scenario == "streamupc": h["content-encoding"] = "gzip"  # nominal: navi does
                                                                  # not compress requests
      let r = await api.request(POST, baseTls & "/upload", headers = h,
        bodyStream = proc(): string =
          if left == 0: return ""
          dec left
          result = newString(16384))
      doAssert r.status == 200
      await api.close()
    of "streamdown", "streamdownc":
      let path = if scenario == "streamdownc": "/download-gz" else: "/download"
      let api = newNavi(cfgFor(cert, true, h1only = false))
      let s = await api.stream(GET, baseTls & path)
      doAssert s.status == 200
      var n = 0
      s.each(chunk): n += chunk.len
      doAssert n == payloadBytes, "got " & $n & " bytes"
      await api.close()
    of "sse":
      let api = newNavi(cfgFor(cert, true, h1only = false))
      let s = await api.sse(baseTls & "/sse", reconnect = false)
      var c = 0
      s.each(ev): inc c
      doAssert c == 6, "got " & $c & " events"
      await s.close()
      await api.close()
    of "ws", "wss":
      let tls = scenario == "wss"
      let api = newNavi(cfgFor(cert, tls, h1only = false))
      let ws = await api.websocket(wsUrl(if tls: baseTls else: base, "/ws"))
      for _ in 0 ..< 3:
        await ws.send("ping")
        let m = await ws.receive()
        doAssert m.kind == wmText and m.data == "ping"
      await ws.close()
      await api.close()
    else:
      doAssert false, "unknown scenario: " & scenario
  echo "ok: ", backend, " ", scenario, " x ", iters, " iterations completed"

when defined(useChronos) or defined(useAsync):
  proc main() {.async.} = runAll()
  waitFor main()
else:
  proc main() = runAll()
  main()
