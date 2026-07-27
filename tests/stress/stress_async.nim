## Async backend stress client. One source, built twice:
##   nim c -d:ssl ...                       -> navi/asyncdispatch
##   nim c -d:ssl -d:useChronos ...         -> navi/chronos
##
## Runs several navi clients concurrently, each looping until a deadline: every
## HTTP verb against the TLS server, plus a round trip on a persistent WebSocket.
## A middleware stamps `x-stress: 1`, which the server echoes back so we can assert
## the chain ran. Any bad status, wrong echo, or exception aborts (non-zero exit).

import std/[times, os, json, strutils]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"

const verbs = [GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS]

proc stampMw(): NaviMiddleware =
  result = proc(ctx: NaviContext) {.async.} =
    ctx.req.headers["x-stress"] = "1"        # proof-of-middleware; echoed by server
    await ctx.next()

proc mkClient(base, cert: string): Navi =
  var cfg = newNaviConfig()
  cfg.prefixUrl = base
  cfg.tls.caFile = cert                       # trust the server's self-signed cert
  cfg.middleware = @[stampMw()]
  newNavi(cfg)

proc httpRound(api: Navi) {.async.} =
  for v in verbs:
    let body = if v in {POST, PUT, PATCH}: "payload" else: ""
    let res = await api.request(v, "/echo", body = body)
    doAssert res.status == 200, $v & " -> " & $res.status
    if v != HEAD:
      doAssert res.data["method"].getStr == $v, "method echo: " & res.data["method"].getStr
      doAssert res.data["stress"].getStr == "1", "middleware header not seen by server"

proc wsRound(ws: WebSocket) {.async.} =
  await ws.send("ping")
  let t = await ws.receive()
  doAssert t.kind == wmText and t.data == "ping", "ws text echo"
  await ws.send("bytes", binary = true)
  let b = await ws.receive()
  doAssert b.kind == wmBinary and b.data == "bytes", "ws binary echo"

proc clientLoop(api: Navi, wsUrl: string, deadline: float): Future[int] {.async.} =
  let ws = await api.websocket(wsUrl)          # one persistent WS per client
  var ops = 0
  while epochTime() < deadline:
    await httpRound(api)
    await wsRound(ws)
    inc ops
  await ws.close()
  return ops

proc main() {.async.} =
  let secs = parseFloat(getEnv("NAVI_STRESS_SECONDS", "20"))
  let base = getEnv("NAVI_STRESS_URL", "https://127.0.0.1:9443")
  let cert = getEnv("NAVI_STRESS_CERT", "")
  let clients = parseInt(getEnv("NAVI_STRESS_CLIENTS", "3"))
  let wsUrl = "wss://" & base["https://".len .. ^1] & "/ws"
  let deadline = epochTime() + secs

  # Start every client, then await each: they run concurrently on the event loop.
  var loops: seq[Future[int]]
  for _ in 0 ..< clients:
    loops.add clientLoop(mkClient(base, cert), wsUrl, deadline)
  var total = 0
  for f in loops: total += await f
  echo "[", backend, "] ", clients, " clients, ", total, " batches over ", secs, "s: OK"

waitFor main()
