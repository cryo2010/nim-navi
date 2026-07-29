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
  var cfg = initNaviConfig()
  cfg.prefixUrl = base
  cfg.tls.caFile = cert                       # trust the server's self-signed cert
  cfg.middleware = @[stampMw()]
  initNavi(cfg)

proc httpRound(api: Navi) {.async.} =
  # Fire every verb concurrently on one client, then await each and check it, so
  # verbs.len requests are in flight at once -- stressing the connection pool and
  # (on an h2 origin) the stream multiplexer, which a serial await-each would not.
  var futs: seq[Future[Response]]
  var specs: seq[tuple[v: HttpVerb, body, ct, enc: string]]
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
    futs.add api.request(v, "/echo", headers = h, body = wireBody)   # started now
    specs.add (v: v, body: sentBody, ct: sentCt, enc: enc)
  for i in 0 ..< futs.len:
    let res = await futs[i]
    let s = specs[i]
    doAssert res.status == 200, $s.v & " -> " & $res.status
    doAssert res.headers.get("x-echo-method") == $s.v, "method echo: " & res.headers.get("x-echo-method")
    doAssert res.headers.get("x-echo-stress") == "1", "middleware header not seen by server"
    if s.v != HEAD:                             # HEAD carries no body
      doAssert res.body == s.body, "body echo mismatch: " & res.body   # navi decoded it
      doAssert res.headers.get("content-length") == $s.body.len,       # rewritten to decoded len
        "content-length: " & res.headers.get("content-length")
      doAssert res.headers.get("content-type") == s.ct,
        "content-type: " & res.headers.get("content-type")
      if s.enc.len > 0:
        doAssert res.headers.get("content-encoding") == "",            # navi consumed it
          "content-encoding not consumed: " & res.headers.get("content-encoding")

proc wsRound(ws: WebSocket) {.async.} =
  await ws.send("ping")
  let t = await ws.receive()
  doAssert t.kind == wmText and t.data == "ping", "ws text echo"
  await ws.send("bytes", binary = true)
  let b = await ws.receive()
  doAssert b.kind == wmBinary and b.data == "bytes", "ws binary echo"

type Client = ref object                       # ref wrapper: Navi has no valid default
  api: Navi
  ws: WebSocket

proc clientLoop(c: Client, deadline: float): Future[int] {.async.} =
  var ops = 0
  while epochTime() < deadline:
    await httpRound(c.api)
    await wsRound(c.ws)
    inc ops
  await c.ws.close()
  return ops

proc main() {.async.} =
  let secs = parseFloat(getEnv("NAVI_STRESS_SECONDS", "20"))
  let base = getEnv("NAVI_STRESS_URL", "https://127.0.0.1:9443")
  let cert = getEnv("NAVI_STRESS_CERT", "")
  let clients = parseInt(getEnv("NAVI_STRESS_CLIENTS", "3"))
  let wsUrl = "wss://" & base["https://".len .. ^1] & "/ws"

  # Open every client's WebSocket first (sequentially), so the concurrent HTTP
  # soak that follows never competes with a WS TLS handshake for CPU/accepts.
  var pool: seq[Client]
  for _ in 0 ..< clients:
    let api = mkClient(base, cert)
    pool.add Client(api: api, ws: await api.websocket(wsUrl))

  let deadline = epochTime() + secs
  var loops: seq[Future[int]]                  # start all, then await: concurrent
  for c in pool: loops.add clientLoop(c, deadline)
  var total = 0
  for f in loops: total += await f
  let rps = int(float(total * verbs.len) / secs)   # HTTP requests/s (WS excluded)
  echo "[", backend, "] ", clients, " clients, ", total, " batches, ", rps,
       " req/s over ", secs, "s: OK"

waitFor main()
