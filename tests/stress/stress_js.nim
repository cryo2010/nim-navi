## navi/js backend stress client. Runs under Node (needs Node 22+ for the global
## WebSocket); the runner trusts the server's self-signed cert via
## NODE_EXTRA_CA_CERTS. Same workload as the native clients: every HTTP verb plus
## a persistent WebSocket round trip, several clients concurrently, a middleware
## stamping x-stress, until a deadline. A failed assert rejects the promise and
## exits Node non-zero. Config comes from process.env (std/os is thin under js).
##
## Note: each client is held behind a `ref` (`Client`) rather than passing the
## `Navi` value object around; a ref survives `await` cleanly under `nim js`.
import std/strutils
import navi/js

proc envJs(name, dflt: cstring): cstring {.importjs: "(process.env[#] ?? #)".}
proc nowMs(): float {.importjs: "Date.now()".}

const verbs = [GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS]

proc stampMw(): NaviMiddleware =
  result = proc(ctx: NaviContext) {.async.} =
    ctx.req.headers["x-stress"] = "1"
    await ctx.next()

proc mkClient(base: string): Navi =
  var cfg = initNaviConfig()
  cfg.prefixUrl = base
  cfg.middleware = @[stampMw()]
  newNavi(cfg)

proc main() {.async.} =
  let secs = parseFloat($envJs("NAVI_STRESS_SECONDS", "20"))
  let base = $envJs("NAVI_STRESS_URL", "https://127.0.0.1:9443")
  let clients = parseInt($envJs("NAVI_STRESS_CLIENTS", "3"))
  let wsUrl = "wss://" & base["https://".len .. ^1] & "/ws"
  type Client = ref object
    api: Navi
    ws: WebSocket

  proc oneClient(c: Client, deadline: float): Future[int] {.async.} =
    var ops = 0
    while nowMs() < deadline:
      # Fire every verb concurrently, then await each -- verbs.len fetches in
      # flight at once (undici pools the connections) rather than one at a time.
      var futs: seq[Future[Response]]
      var specs: seq[tuple[v: HttpVerb, body, ct: string]]
      for v in verbs:
        let sentBody = if v in {POST, PUT, PATCH}: "payload-" & $v else: ""
        let sentCt = if sentBody.len > 0: "text/plain" else: ""
        var hh = initHeaders()
        if sentCt.len > 0: hh["content-type"] = sentCt
        futs.add c.api.request(v, "/echo", headers = hh, body = sentBody)   # started now
        specs.add (v: v, body: sentBody, ct: sentCt)
      for i in 0 ..< futs.len:
        let res = await futs[i]
        let s = specs[i]
        doAssert res.status == 200, $s.v & " -> " & $res.status
        doAssert res.headers.get("x-echo-method") == $s.v, "method echo: " & res.headers.get("x-echo-method")
        doAssert res.headers.get("x-echo-stress") == "1", "middleware header not seen by server"
        if s.v != HEAD:                           # HEAD carries no body
          doAssert res.body == s.body, "body echo mismatch: " & res.body
          doAssert res.headers.get("content-length") == $s.body.len,
            "content-length: " & res.headers.get("content-length")
          doAssert res.headers.get("content-type") == s.ct,
            "content-type: " & res.headers.get("content-type")
      await c.ws.send("ping")
      let t = await c.ws.receive()
      doAssert t.kind == wmText and t.data == "ping", "ws text echo"
      await c.ws.send("bytes", binary = true)
      let b = await c.ws.receive()
      doAssert b.kind == wmBinary and b.data == "bytes", "ws binary echo"
      inc ops
    await c.ws.close()
    return ops

  # Open every client's WebSocket first, then run the concurrent HTTP soak, so a
  # WS handshake never competes with the fetch burst for connection setup.
  var pool: seq[Client]
  for _ in 0 ..< clients:
    let api = mkClient(base)
    pool.add Client(api: api, ws: await api.websocket(wsUrl))
  let deadline = nowMs() + secs * 1000.0
  var loops: seq[Future[int]]
  for c in pool: loops.add oneClient(c, deadline)
  var total = 0
  for f in loops: total += await f
  let rps = int(float(total * verbs.len) / secs)   # HTTP requests/s (WS excluded)
  echo "[js] ", clients, " clients, ", total, " batches, ", rps,
       " req/s over ", secs, "s: OK"

discard main()
