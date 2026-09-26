## benchRequests reference: Nim std/httpclient, sync (name: std-sync). HTTP/1.1 only
## (std/httpclient has no h2/h3), so it self-skips any non-h1 cell. Reuses the bench
## common modules for config/pool/latency, mirroring the navi sync client's loop.

import std/[times, monotimes, httpclient, net]
import ../common/[config, reporter, servers]

const verbs = [HttpGet, HttpPost, HttpPut]

proc mkClient(cfg: Config): HttpClient =
  ## Verify the origin's certificate, exactly as navi does (its TlsConfig.verify
  ## defaults on): CVerifyNone would skip X.509 chain building and the hostname match,
  ## so this row would pay less TLS work per connection than navi's for free. cfg.cert
  ## is NAVI_CERT, the harness's self-signed CA; an empty caFile falls back to the
  ## platform trust store, never to an unverified handshake -- a handshake failure must
  ## surface as a FAIL below, not as a cheap number.
  newHttpClient(sslContext = newContext(verifyMode = CVerifyPeer, caFile = cfg.cert))

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
  var client = mkClient(cfg)
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
        client.close(); client = mkClient(cfg)
    except CatchableError as e:
      rec.fail()
      stderr.writeLine "[std-sync] FAIL: " & e.msg
      quit(1)
  # Divide by the REAL measured window, not the nominal cfg.seconds: the loop only
  # exits after the request that crossed the deadline has finished, so cfg.seconds
  # would flatter this row's req/s. Matches go's time.Since(measureStart) and the
  # native runner; cfg.seconds stays as the fallback if the delta is non-positive.
  let elapsed = epochTime() - measureStart
  emitResult("std-sync", rec, if elapsed > 0: elapsed else: cfg.seconds)

main()
