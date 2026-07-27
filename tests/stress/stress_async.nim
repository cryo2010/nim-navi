## Async backend stress client. One source, built twice:
##   nim c -d:ssl ...                       -> navi/asyncdispatch
##   nim c -d:ssl -d:useChronos ...         -> navi/chronos
##
## Runs several navi clients concurrently, each looping until a deadline: every
## HTTP verb against the TLS server, plus a round trip on a persistent WebSocket.
## A middleware stamps `x-stress: 1`, which the server echoes back so we can assert
## the chain ran. Any bad status, wrong echo, or exception aborts (non-zero exit).

import std/[times, os, strutils]
import ./zlibcodec
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
    let sentBody = if v in {POST, PUT, PATCH}: "payload-" & $v else: ""
    let sentCt = if sentBody.len > 0: "text/plain" else: ""
    # Compress the request body (and ask for a compressed response) on the bodied
    # verbs, alternating gzip/deflate. navi passes the compressed request through
    # and decodes the compressed response -- both ways on the wire.
    let enc = if sentBody.len == 0: "" elif ord(v) mod 2 == 0: "gzip" else: "deflate"
    var h = initHeaders()
    if sentCt.len > 0: h["content-type"] = sentCt
    var wireBody = sentBody
    if enc.len > 0:
      wireBody = zcompress(sentBody, enc)
      h["content-encoding"] = enc
      h["x-want-encoding"] = enc
    let res = await api.request(v, "/echo", headers = h, body = wireBody)
    doAssert res.status == 200, $v & " -> " & $res.status
    doAssert res.headers.get("x-echo-method") == $v, "method echo: " & res.headers.get("x-echo-method")
    doAssert res.headers.get("x-echo-stress") == "1", "middleware header not seen by server"
    if v != HEAD:                               # HEAD carries no body
      doAssert res.body == sentBody, "body echo mismatch: " & res.body   # navi decoded it
      doAssert res.headers.get("content-length") == $sentBody.len,       # rewritten to decoded len
        "content-length: " & res.headers.get("content-length")
      doAssert res.headers.get("content-type") == sentCt,
        "content-type: " & res.headers.get("content-type")
      if enc.len > 0:
        doAssert res.headers.get("content-encoding") == "",              # navi consumed it
          "content-encoding not consumed: " & res.headers.get("content-encoding")

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
  let rps = int(float(total * verbs.len) / secs)   # HTTP requests/s (WS excluded)
  echo "[", backend, "] ", clients, " clients, ", total, " batches, ", rps,
       " req/s over ", secs, "s: OK"

waitFor main()
