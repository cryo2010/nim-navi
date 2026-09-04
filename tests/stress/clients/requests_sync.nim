## stressRequests, sync backend (`import navi`). The sync client is single
## in-flight by nature: it loops the clients round-robin, firing GET/POST/PUT at
## /echo across the server pool until the deadline. Same consume-and-discard +
## per-report cadence as the async client, minus the fan-out (documented: sync has
## no concurrency).

import std/[times, strutils]
import ../zlibcodec
import ../common/[config, reporter, servers]
import navi
include ../common/httpset

const verbs = [GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS]
const bodies = stressBodies()

proc stampMw(): NaviMiddleware =
  result = proc(ctx: NaviContext) =
    ctx.req.headers["x-stress"] = "1"
    ctx.next()

proc failHard(cfg: Config, msg: string) =
  stderr.writeLine cfg.label & " FAIL: " & msg
  quit(1)

proc featureChecks(cfg: Config, base: string) =
  ## Once-per-cell checks of paths the /echo soak never hits: redirect following,
  ## error-status handling, the cookie jar, and Basic auth -- under the pinned protocol.
  var ec = initNaviConfig()
  ec.http = httpVersions(cfg.proto)
  ec.tls.caFile = cfg.cert
  ec.throwHttpErrors = false
  let api = newNavi(ec)
  block:
    let r = api.request(GET, base & "/redirect/3")
    if r.status != 200 or r.body != "redirect-done":
      cfg.failHard("redirect: status " & $r.status & " body '" & r.body & "'")
  for code in [404, 503]:
    if api.request(GET, base & "/status/" & $code).status != code:
      cfg.failHard("status " & $code & " not surfaced")
  discard api.request(GET, base & "/setcookie")
  if api.request(GET, base & "/needs-cookie").status != 200:
    cfg.failHard("cookie jar not carried back to the origin")
  var ac = initNaviConfig()
  ac.http = httpVersions(cfg.proto)
  ac.tls.caFile = cfg.cert
  ac.auth = basicAuth("stress", "secret")
  if newNavi(ac).request(GET, base & "/needs-auth").status != 200:
    cfg.failHard("basic auth rejected")

proc mkClient(cfg: Config): Navi =
  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)
  c.tls.caFile = cfg.cert
  c.middleware = @[stampMw()]
  newNavi(c)

proc main() =
  let cfg = loadConfig("sync")
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  var pool = initServerPool(cfg)
  let counter = newStatusCounter()
  var apis: seq[Navi]
  for _ in 0 ..< cfg.clients: apis.add mkClient(cfg)

  # Warm up per-origin protocol discovery (h3 needs an Alt-Svc round-trip) so the
  # measured phase is pinned from the first request; then a downgrade fails hard.
  let expect = cfg.expectedVersion
  if expect.len > 0:
    for api in apis:
      for base in pool.all():
        for _ in 0 ..< 3:
          try:
            if api.request(GET, base & "/echo").httpVersion == expect: break
          except CatchableError: break

  featureChecks(cfg, pool.all()[0])   # redirect/status/auth/cookie coverage

  let start = epochTime()
  let deadline = start + cfg.seconds
  var lastReport = start
  var n = 0
  while epochTime() < deadline:
    for api in apis:
      let v = verbs[n mod verbs.len]
      let plain = bodies[n mod bodies.len]
      let bodied = v in {POST, PUT, PATCH}
      inc n
      let url = pool.pick() & "/echo"
      var h = initHeaders()
      var wire = ""
      if bodied:
        wire = plain
        h["content-type"] = "application/octet-stream"
        if cfg.reqCompression != "none" and plain.len > 0:
          wire = zcompress(plain, cfg.reqCompression)
          h["content-encoding"] = cfg.reqCompression
        if cfg.respCompression != "none":
          h["x-want-encoding"] = cfg.respCompression
      if n mod 11 == 0: h["x-big"] = repeat("H", 8192)   # exercise the HPACK path
      try:
        let res = api.request(v, url, headers = h, body = wire)
        cfg.checkVersion(res.httpVersion)   # hard-fail on a silent protocol downgrade
        if res.status != 200: cfg.failHard($v & " " & url & " -> status " & $res.status)
        if res.headers.get("x-echo-method") != $v:
          cfg.failHard($v & ": echoed method '" & res.headers.get("x-echo-method") & "'")
        if res.headers.get("x-echo-stress") != "1":
          cfg.failHard($v & ": middleware header not echoed")
        let expectBody = if bodied and v != HEAD: plain else: ""
        if res.body != expectBody:
          cfg.failHard($v & ": body mismatch (expected " & $expectBody.len &
            " bytes, got " & $res.body.len & ")")
        counter.tally(res.status)
      except CatchableError as e:
        counter.fail()
        cfg.failHard($v & " " & url & " -> " & $e.name & ": " & e.msg)
    if epochTime() - lastReport >= cfg.reportSeconds.float:
      lastReport = epochTime()
      report(cfg.label, counter, epochTime() - start)

  if counter.ops == 0: cfg.failHard("no request completed")
  report(cfg.label & " final", counter, epochTime() - start)
  echo "== requests sync ", cfg.proto, " passed (", counter.ops, " ops) =="

main()
