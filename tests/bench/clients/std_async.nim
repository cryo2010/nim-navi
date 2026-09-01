## benchRequests reference: Nim std/httpclient, async (name: std-async). HTTP/1.1
## only. Single in-flight per client (std AsyncHttpClient), so it fans out `clients`
## x `concurrency` AsyncHttpClients to match the navi async loop's width.

import std/[times, monotimes, asyncdispatch, httpclient, net]
import ../common/[config, reporter, servers]

const verbs = [HttpGet, HttpPost, HttpPut]

proc mkClient(): AsyncHttpClient =
  newAsyncHttpClient(sslContext = newContext(verifyMode = CVerifyNone))

proc worker(cfg: Config, pool: ptr ServerPool, rec: BenchRecorder,
            measureStart, deadline: float, i: int) {.async.} =
  var client = mkClient()
  var n = i
  while epochTime() < deadline:
    let v = verbs[n mod verbs.len]; inc n
    let url = pool[].pick() & "/echo"
    var body = ""
    if v in {HttpPost, HttpPut}: body = "payload-x"
    let t0 = getMonoTime()
    try:
      let resp = await client.request(url, httpMethod = v, body = body)
      discard await resp.body            # drain
      if epochTime() >= measureStart:
        rec.record((getMonoTime() - t0).inMicroseconds)
      if cfg.cold:
        client.close(); client = mkClient()
    except CatchableError as e:
      rec.fail()
      stderr.writeLine "[std-async] FAIL: " & e.msg
      quit(1)

proc main() {.async.} =
  let cfg = loadConfig("std")
  if cfg.proto != "h1":
    echo "SKIP\tstd-async\tstd/httpclient is HTTP/1.1 only"; return
  var pool = initServerPool(cfg)
  let rec = newBenchRecorder()
  let start = epochTime()
  let measureStart = start + cfg.warmupSeconds
  let deadline = measureStart + cfg.seconds
  var futs: seq[Future[void]]
  for i in 0 ..< cfg.clients * cfg.concurrency:
    futs.add worker(cfg, addr pool, rec, measureStart, deadline, i)
  for f in futs: await f
  emitResult("std-async", rec, cfg.seconds)

waitFor main()
