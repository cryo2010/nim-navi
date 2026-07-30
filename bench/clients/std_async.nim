## Benchmark client: Nim stdlib std/httpclient (AsyncHttpClient, asyncdispatch).
## Same workload as std_sync; gzip requested and gunzipped with zippy.
## NAVI_BENCH_COLD=1 sends `Connection: close` so each request reconnects.
import std/[os, strutils, monotimes, times, httpclient, net, asyncdispatch]
import zippy

let url = getEnv("NAVI_BENCH_URL", "https://127.0.0.1:8443")
let iters = parseInt(getEnv("NAVI_BENCH_ITERS", "3000"))
let cold = getEnv("NAVI_BENCH_COLD", "0") == "1"
let warmup = if cold: min(20, iters) else: max(100, iters div 10)
const body = "x".repeat(256)

proc decode(resp: AsyncResponse): Future[string] {.async.} =
  let raw = await resp.body
  if resp.headers.getOrDefault("content-encoding") == "gzip":
    uncompress(raw, dfGzip)
  else: raw

proc oneIter(client: AsyncHttpClient) {.async.} =
  discard await decode(await client.request(url & "/get", HttpGet))
  discard await decode(await client.request(url & "/post", HttpPost, body = body))
  discard await decode(await client.request(url & "/put", HttpPut, body = body))
  discard await decode(await client.request(url & "/patch", HttpPatch, body = body))
  discard await decode(await client.request(url & "/delete", HttpDelete))
  discard await (await client.request(url & "/get", HttpHead)).body
  discard await decode(await client.request(url & "/get", HttpOptions))

proc main() {.async.} =
  let client = newAsyncHttpClient(sslContext = newContext(verifyMode = CVerifyNone))
  var hdrs = @[("Accept-Encoding", "gzip")]
  if cold: hdrs.add ("Connection", "close")
  client.headers = newHttpHeaders(hdrs)
  for _ in 0 ..< warmup: await oneIter(client)
  let t0 = getMonoTime()
  for _ in 0 ..< iters: await oneIter(client)
  let secs = (getMonoTime() - t0).inNanoseconds.float / 1e9
  let reqs = iters * 7
  echo "RESULT\tstd-async\t", reqs, "\t", formatFloat(secs, ffDecimal, 3), "\t",
    formatFloat(reqs.float / secs, ffDecimal, 0)

waitFor main()
