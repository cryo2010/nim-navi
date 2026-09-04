## stressRequests, async backends (one source, built twice):
##   nim c -d:ssl ...                -> navi/asyncdispatch
##   nim c -d:ssl -d:useChronos ...  -> navi/chronos
##
## A buffered request/response soak: `clients` navi clients, each fanning out
## `concurrency` workers that loop every verb against `/echo` across the server pool
## until the deadline. A middleware stamps x-stress (exercises the chain). Each
## response is fully verified -- status is 200, x-echo-method matches the verb,
## x-echo-stress is echoed, and the (decompressed) body byte-matches what was sent
## across a rotation of body shapes (empty, frame/window boundaries, large,
## highly-compressible). A verification miss or transport error FAILS HARD. The
## protocol is pinned via config.http, so a silent downgrade also fails.

import std/[times, strutils]
import ../zlibcodec
import ../common/[config, reporter, servers]

when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"
include ../common/httpset

const verbs = [GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS]
const bodies = stressBodies()          # compile-time: gcsafe global for the chronos build

proc stampMw(): NaviMiddleware =
  result = proc(ctx: NaviContext) {.async.} =
    ctx.req.headers["x-stress"] = "1"
    await ctx.next()

proc mkClient(cfg: Config): Navi =
  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)     # pin the cell's protocol (strict: no downgrade)
  c.tls.caFile = cfg.cert
  c.middleware = @[stampMw()]
  newNavi(c)

proc failHard(cfg: Config, msg: string) =
  {.cast(gcsafe).}:
    stderr.writeLine cfg.label & " FAIL: " & msg
  quit(1)

proc worker(api: Navi, cfg: Config, pool: ptr ServerPool,
            counter: StatusCounter, deadline: float, i: int) {.async.} =
  var n = i
  while epochTime() < deadline:
    let v = verbs[n mod verbs.len]
    let plain = bodies[n mod bodies.len]
    let bodied = v in {POST, PUT, PATCH}
    inc n
    let url = pool[].pick() & "/echo"
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
    if n mod 11 == 0:                    # occasionally a big header value -> HPACK path
      h["x-big"] = repeat("H", 8192)
    try:
      let res = await api.request(v, url, headers = h, body = wire)
      cfg.checkVersion(res.httpVersion)  # hard-fail on a silent protocol downgrade
      if res.status != 200:
        cfg.failHard($v & " " & url & " -> status " & $res.status)
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
      # A surfaced transport error means navi could not handle the request (a
      # replayable failure is retried internally and never reaches here), so treat
      # it as a bug to investigate, not soak noise to tally.
      cfg.failHard($v & " " & url & " -> " & $e.name & ": " & e.msg)

proc featureChecks(cfg: Config, base: string) {.async.} =
  ## Once-per-cell checks of paths the /echo soak never hits: redirect following,
  ## error-status handling, the cookie jar, and Basic auth -- all under the pinned
  ## protocol. Closes the redirect/status/auth/cookie coverage gaps.
  var ec = initNaviConfig()
  ec.http = httpVersions(cfg.proto)
  ec.tls.caFile = cfg.cert
  ec.throwHttpErrors = false            # inspect 4xx/5xx instead of raising
  let api = newNavi(ec)
  block:
    let r = await api.request(GET, base & "/redirect/3")   # 3 hops -> 200
    if r.status != 200 or r.body != "redirect-done":
      cfg.failHard("redirect: status " & $r.status & " body '" & r.body & "'")
  for code in [404, 503]:
    if (await api.request(GET, base & "/status/" & $code)).status != code:
      cfg.failHard("status " & $code & " not surfaced")
  discard await api.request(GET, base & "/setcookie")       # jar records the cookie
  if (await api.request(GET, base & "/needs-cookie")).status != 200:
    cfg.failHard("cookie jar not carried back to the origin")
  var ac = initNaviConfig()
  ac.http = httpVersions(cfg.proto)
  ac.tls.caFile = cfg.cert
  ac.auth = basicAuth("stress", "secret")
  if (await newNavi(ac).request(GET, base & "/needs-auth")).status != 200:
    cfg.failHard("basic auth rejected")

proc reporterLoop(cfg: Config, counter: StatusCounter,
                  start, deadline: float) {.async.} =
  var last = start
  while epochTime() < deadline:
    await sleep(1000)                   # 1s granularity: stop within ~1s of the deadline
    if epochTime() - last >= cfg.reportSeconds.float:
      last = epochTime()
      report(cfg.label, counter, epochTime() - start)

proc main() {.async.} =
  let cfg = loadConfig(backend)
  let reason = cfg.skipReason
  if reason.len > 0: echo cfg.label, " ", reason; return
  var pool = initServerPool(cfg)
  let counter = newStatusCounter()
  var apis: seq[Navi]
  for _ in 0 ..< cfg.clients: apis.add mkClient(cfg)

  # Warm up each client's per-origin protocol so the measured phase runs pinned from
  # the first request: h3 is discovered via an initial Alt-Svc round-trip, so hit
  # each origin until the expected version is negotiated. After this, any downgrade
  # during the soak fails hard (checkVersion), catching a silent fallback.
  let expect = cfg.expectedVersion
  if expect.len > 0:
    for api in apis:
      for base in pool.all():
        for _ in 0 ..< 3:
          try:
            if (await api.request(GET, base & "/echo")).httpVersion == expect: break
          except CatchableError: break

  await featureChecks(cfg, pool.all()[0])   # redirect/status/auth/cookie coverage

  let start = epochTime()
  let deadline = start + cfg.seconds
  var futs: seq[Future[void]]
  for api in apis:
    for i in 0 ..< cfg.concurrency:
      futs.add worker(api, cfg, addr pool, counter, deadline, i)
  futs.add reporterLoop(cfg, counter, start, deadline)
  for f in futs: await f

  if counter.ops == 0: cfg.failHard("no request completed")   # a cell must do work
  report(cfg.label & " final", counter, epochTime() - start)
  echo "== requests ", backend, " ", cfg.proto, " passed (", counter.ops, " ops) =="

waitFor main()
