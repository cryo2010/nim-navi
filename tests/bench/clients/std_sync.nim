## benchRequests reference: Nim std/httpclient, sync (name: std-sync). HTTP/1.1 only
## (std/httpclient has no h2/h3), so it self-skips any non-h1 cell. Reuses the bench
## common modules for config/pool/latency, mirroring the navi sync client's loop.

import std/[times, monotimes, httpclient, net]
import ../common/[config, reporter, servers]

const verbs = [HttpGet, HttpPost, HttpPut]

proc mkClient(): HttpClient =
  newHttpClient(sslContext = newContext(verifyMode = CVerifyNone))

proc main() =
  let cfg = loadConfig("std")
  if cfg.workload != "requests":
    echo "SKIP\tstd-sync\tstd/httpclient reference does requests only"; return
  if cfg.proto != "h1":
    echo "SKIP\tstd-sync\tstd/httpclient is HTTP/1.1 only"; return
  var pool = initServerPool(cfg)
  let rec = newBenchRecorder()
  let start = epochTime()
  let measureStart = start + cfg.warmupSeconds
  let deadline = measureStart + cfg.seconds
  var client = mkClient()
  var n = 0
  while epochTime() < deadline:
    let v = verbs[n mod verbs.len]; inc n
    let url = pool.pick() & "/echo"
    var body = ""
    if v in {HttpPost, HttpPut}: body = "payload-x"
    let t0 = getMonoTime()
    try:
      let resp = client.request(url, httpMethod = v, body = body)
      discard resp.body                 # drain
      if epochTime() >= measureStart:
        rec.record((getMonoTime() - t0).inMicroseconds)
      if cfg.cold:                       # fresh connection per request
        client.close(); client = mkClient()
    except CatchableError as e:
      rec.fail()
      stderr.writeLine "[std-sync] FAIL: " & e.msg
      quit(1)
  emitResult("std-sync", rec, cfg.seconds)

main()
