## navi/js backend stress client. Runs under Node (needs Node 22+ for the global
## WebSocket); the runner trusts the server's self-signed cert via
## NODE_EXTRA_CA_CERTS. Same workload as the native clients: every HTTP verb plus
## a persistent WebSocket round trip, several clients concurrently, a middleware
## stamping x-stress, until a deadline. A failed assert rejects the promise and
## exits Node non-zero. Config comes from process.env (std/os is thin under js).
##
## Note: the per-client loop is one closure that *closes over* its Navi rather
## than taking it as a parameter. Under `nim js`, a value-object (`Navi`) passed
## as a parameter into an `{.async.}` proc is lost across an `await`; a captured
## upvalue survives. (Not an issue on the native backends.)
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
  var cfg = newNaviConfig()
  cfg.prefixUrl = base
  cfg.middleware = @[stampMw()]
  newNavi(cfg)

proc main() {.async.} =
  let secs = parseFloat($envJs("NAVI_STRESS_SECONDS", "20"))
  let base = $envJs("NAVI_STRESS_URL", "https://127.0.0.1:9443")
  let clients = parseInt($envJs("NAVI_STRESS_CLIENTS", "3"))
  let wsUrl = "wss://" & base["https://".len .. ^1] & "/ws"
  let deadline = nowMs() + secs * 1000.0

  proc oneClient(): Future[int] {.async.} =
    let api = mkClient(base)              # captured upvalue (survives awaits under js)
    let ws = await api.websocket(wsUrl)   # one persistent WS per client
    var ops = 0
    while nowMs() < deadline:
      for v in verbs:
        let sentBody = if v in {POST, PUT, PATCH}: "payload-" & $v else: ""
        let sentCt = if sentBody.len > 0: "text/plain" else: ""
        var hh = initHeaders()
        if sentCt.len > 0: hh["content-type"] = sentCt
        let res = await api.request(v, "/echo", headers = hh, body = sentBody)
        doAssert res.status == 200, $v & " -> " & $res.status
        doAssert res.headers.get("x-echo-method") == $v, "method echo: " & res.headers.get("x-echo-method")
        doAssert res.headers.get("x-echo-stress") == "1", "middleware header not seen by server"
        if v != HEAD:                             # HEAD carries no body
          doAssert res.body == sentBody, "body echo mismatch: " & res.body
          doAssert res.headers.get("content-length") == $sentBody.len,
            "content-length: " & res.headers.get("content-length")
          doAssert res.headers.get("content-type") == sentCt,
            "content-type: " & res.headers.get("content-type")
      await ws.send("ping")
      let t = await ws.receive()
      doAssert t.kind == wmText and t.data == "ping", "ws text echo"
      await ws.send("bytes", binary = true)
      let b = await ws.receive()
      doAssert b.kind == wmBinary and b.data == "bytes", "ws binary echo"
      inc ops
    await ws.close()
    return ops

  # Start every client, then await each: they run concurrently on the event loop.
  var loops: seq[Future[int]]
  for _ in 0 ..< clients: loops.add oneClient()
  var total = 0
  for f in loops: total += await f
  let rps = int(float(total * verbs.len) / secs)   # HTTP requests/s (WS excluded)
  echo "[js] ", clients, " clients, ", total, " batches, ", rps,
       " req/s over ", secs, "s: OK"

discard main()
