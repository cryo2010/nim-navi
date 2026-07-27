## Sync backend stress client. Drives several navi clients (sequentially, since
## the sync backend blocks) until a deadline: every HTTP verb against the TLS
## server, plus a round trip on a persistent WebSocket. A middleware stamps
## `x-stress: 1`, echoed back by the server to prove the chain ran. Any bad
## status, wrong echo, or exception aborts (non-zero exit).

import std/[times, os, json, strutils]
import navi

const verbs = [GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS]

proc stampMw(): NaviMiddleware =
  result = proc(ctx: NaviContext) =
    ctx.req.headers["x-stress"] = "1"
    ctx.next()

proc mkClient(base, cert: string): Navi =
  var cfg = newNaviConfig()
  cfg.prefixUrl = base
  cfg.tls.caFile = cert
  cfg.middleware = @[stampMw()]
  newNavi(cfg)

proc httpRound(api: Navi) =
  for v in verbs:
    let body = if v in {POST, PUT, PATCH}: "payload" else: ""
    let res = api.request(v, "/echo", body = body)
    doAssert res.status == 200, $v & " -> " & $res.status
    if v != HEAD:
      doAssert res.data["method"].getStr == $v, "method echo: " & res.data["method"].getStr
      doAssert res.data["stress"].getStr == "1", "middleware header not seen by server"

proc wsRound(ws: WebSocket) =
  ws.send("ping")
  let t = ws.receive()
  doAssert t.kind == wmText and t.data == "ping", "ws text echo"
  ws.send("bytes", binary = true)
  let b = ws.receive()
  doAssert b.kind == wmBinary and b.data == "bytes", "ws binary echo"

proc main() =
  let secs = parseFloat(getEnv("NAVI_STRESS_SECONDS", "20"))
  let base = getEnv("NAVI_STRESS_URL", "https://127.0.0.1:9443")
  let cert = getEnv("NAVI_STRESS_CERT", "")
  let clients = parseInt(getEnv("NAVI_STRESS_CLIENTS", "3"))
  let wsUrl = "wss://" & base["https://".len .. ^1] & "/ws"
  let deadline = epochTime() + secs

  var apis: seq[Navi]
  var sockets: seq[WebSocket]
  for _ in 0 ..< clients:
    let api = mkClient(base, cert)
    apis.add api
    sockets.add api.websocket(wsUrl)          # one persistent WS per client

  var total = 0
  while epochTime() < deadline:
    for i in 0 ..< clients:
      httpRound(apis[i])
      wsRound(sockets[i])
      inc total
  for ws in sockets: ws.close()
  echo "[sync] ", clients, " clients, ", total, " batches over ", secs, "s: OK"

main()
