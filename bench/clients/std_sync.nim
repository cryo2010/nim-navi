## Benchmark client: Nim stdlib std/httpclient (sync). std/httpclient neither
## requests nor decodes gzip on its own, so we ask for it and gunzip the body
## with zippy -- so the compression work matches the other clients.
import std/[os, strutils, monotimes, times, httpclient, net]
import zippy

let url = getEnv("NAVI_BENCH_URL", "https://127.0.0.1:8443")
let iters = parseInt(getEnv("NAVI_BENCH_ITERS", "3000"))
let cold = getEnv("NAVI_BENCH_COLD", "0") == "1"
let warmup = if cold: min(20, iters) else: max(100, iters div 10)
const body = "x".repeat(256)

let client = newHttpClient(sslContext = newContext(verifyMode = CVerifyNone))
# NAVI_BENCH_COLD=1: `Connection: close` makes the server drop the socket so each
# request reconnects (fresh TCP+TLS), exposing the connection-setup path.
var hdrs = @[("Accept-Encoding", "gzip")]
if cold: hdrs.add ("Connection", "close")
client.headers = newHttpHeaders(hdrs)

proc decode(resp: Response): string =
  if resp.headers.getOrDefault("content-encoding") == "gzip":
    uncompress(resp.body, dfGzip)
  else: resp.body

proc oneIter() =
  discard decode(client.request(url & "/get", HttpGet))
  discard decode(client.request(url & "/post", HttpPost, body = body))
  discard decode(client.request(url & "/put", HttpPut, body = body))
  discard decode(client.request(url & "/patch", HttpPatch, body = body))
  discard decode(client.request(url & "/delete", HttpDelete))
  discard client.request(url & "/get", HttpHead).body    # HEAD: no body
  discard decode(client.request(url & "/get", HttpOptions))

for _ in 0 ..< warmup: oneIter()
let t0 = getMonoTime()
for _ in 0 ..< iters: oneIter()
let secs = (getMonoTime() - t0).inNanoseconds.float / 1e9
let reqs = iters * 7
echo "RESULT\tstd-sync\t", reqs, "\t", formatFloat(secs, ffDecimal, 3), "\t",
  formatFloat(reqs.float / secs, ffDecimal, 0)
