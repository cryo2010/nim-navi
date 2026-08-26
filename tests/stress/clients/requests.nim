## stressRequests, async backends (one source, built twice):
##   nim c -d:ssl ...                -> navi/asyncdispatch
##   nim c -d:ssl -d:useChronos ...  -> navi/chronos
##
## A buffered request/response soak: `clients` navi clients, each fanning out
## `concurrency` workers that loop GET/POST/PUT against `/echo` across the server
## pool until the deadline. A middleware stamps x-stress (exercises the chain);
## bodied verbs send a compressed body and request a compressed response. Each
## worker tallies res.status and drops the response (never retains a body), so
## memory stays flat over a long soak. A reporter prints status counts + RSS every
## report interval. Transport failures increment an error count and the soak goes on.

import std/times
import ../zlibcodec
import ../common/[config, reporter, servers]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"
include ../common/httpset

const verbs = [GET, POST, PUT]

proc stampMw(): NaviMiddleware =
  result = proc(ctx: NaviContext) {.async.} =
    ctx.req.headers["x-stress"] = "1"
    await ctx.next()

proc mkClient(cfg: Config): Navi =
  var c = initNaviConfig()
  c.http = httpVersions(cfg.proto)
  c.tls.caFile = cfg.cert
  c.middleware = @[stampMw()]
  newNavi(c)

proc worker(api: Navi, cfg: Config, pool: ptr ServerPool,
            counter: StatusCounter, deadline: float, i: int) {.async.} =
  var n = i
  while epochTime() < deadline:
    let v = verbs[n mod verbs.len]; inc n
    let url = pool[].pick() & "/echo"
    var h = initHeaders()
    var body = ""
    if v in {POST, PUT}:
      body = "payload-" & $v
      h["content-type"] = "text/plain"
      if cfg.reqCompression != "none":
        body = zcompress(body, cfg.reqCompression)
        h["content-encoding"] = cfg.reqCompression
      if cfg.respCompression != "none":
        h["x-want-encoding"] = cfg.respCompression
    try:
      let res = await api.request(v, url, headers = h, body = body)
      counter.tally(res.status)        # then res drops: no body retained
    except CatchableError:
      counter.fail()

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

  let start = epochTime()
  let deadline = start + cfg.seconds
  var futs: seq[Future[void]]
  for api in apis:
    for i in 0 ..< cfg.concurrency:
      futs.add worker(api, cfg, addr pool, counter, deadline, i)
  futs.add reporterLoop(cfg, counter, start, deadline)
  for f in futs: await f

  report(cfg.label & " final", counter, epochTime() - start)
  echo "== requests ", backend, " ", cfg.proto, " passed (", counter.ops, " ops) =="

waitFor main()
