## Concurrent streaming interop (navi/asyncdispatch over HTTP/2).
##
## Fires N simultaneous streamed downloads and N simultaneous streamed uploads and
## then a mixed batch of both, all over a single h2 connection (the mux). Every
## transfer is verified by SHA-1, and the run asserts they multiplexed onto ONE
## connection (`openedConnections == 1`), proving concurrent streaming shares the
## mux rather than opening a connection per file. Driven by docker-compose against
## the FastAPI h2 server (see docker-compose.yml). N defaults to 50 and is
## overridable via NAVI_CONCURRENT_N.

import std/[os, strutils, json]
import checksums/sha1
import navi/asyncdispatch
from navi/backend/asyncdispatch import openedConnections

let n = parseInt(getEnv("NAVI_CONCURRENT_N", "50"))   # at least 50 files at a time
const fileSize = 256 * 1024                            # per-file payload

proc sampleContent(bytes, seed: int): string =
  ## Deterministic per-file bytes (a small LCG, seeded per file so uploads differ).
  result = newStringOfCap(bytes)
  var x = uint32(seed) * 2654435761'u32 + 0x9e3779b9'u32
  for _ in 0 ..< bytes:
    x = x * 1664525'u32 + 1013904223'u32
    result.add char(x shr 24)

proc downloadOne(api: Navi, base: string, i: int) {.async.} =
  ## Stream a download and verify its body against the server's x-sha1.
  let res = await api.stream(GET, base & "/download?size=" & $fileSize)
  doAssert res.status == 200, "download " & $i & " status " & $res.status
  doAssert res.httpVersion == "HTTP/2", "download " & $i & " not h2: " & res.httpVersion
  var body = newStringOfCap(fileSize)
  res.each(chunk):
    body.add chunk
  doAssert body.len == fileSize, "download " & $i & " size " & $body.len
  let want = res.headers.get("x-sha1").toLowerAscii
  doAssert ($secureHash(body)).toLowerAscii == want, "download " & $i & " sha1 mismatch"

proc uploadOne(api: Navi, base: string, i: int) {.async.} =
  ## Stream an upload (pull-based bodyStream) and verify the server's computed SHA-1.
  let payload = sampleContent(fileSize, i)
  let localSha = ($secureHash(payload)).toLowerAscii
  var off = 0
  const chunkSize = 32 * 1024
  var headers = initHeaders()
  headers["content-type"] = "application/octet-stream"
  let res = await api.request(POST, base & "/upload", headers = headers,
    bodyStream = proc(): string =
      if off >= payload.len: return ""
      let take = min(chunkSize, payload.len - off)
      result = payload[off ..< off + take]
      off += take)
  doAssert res.status == 200, "upload " & $i & " status " & $res.status
  let j = parseJson(res.body)
  doAssert j["size"].getInt == fileSize, "upload " & $i & " size " & $j["size"].getInt
  doAssert j["sha1"].getStr.toLowerAscii == localSha, "upload " & $i & " sha1 mismatch"

proc main() {.async.} =
  var cfg = initNaviConfig()
  cfg.tls.verify = false                    # the server uses a self-signed cert
  let api = newNavi(cfg)
  let base = getEnv("BASE")

  block:                                    # N concurrent streamed downloads
    var futs: seq[Future[void]]
    for i in 0 ..< n: futs.add downloadOne(api, base, i)
    await all(futs)
    echo "ok: ", n, " concurrent streamed downloads verified"

  block:                                    # N concurrent streamed uploads
    var futs: seq[Future[void]]
    for i in 0 ..< n: futs.add uploadOne(api, base, i)
    await all(futs)
    echo "ok: ", n, " concurrent streamed uploads verified"

  block:                                    # N concurrent, uploads and downloads mixed
    var futs: seq[Future[void]]
    for i in 0 ..< n:
      if i mod 2 == 0: futs.add downloadOne(api, base, i)
      else: futs.add uploadOne(api, base, i)
    await all(futs)
    echo "ok: ", n, " concurrent mixed up/down transfers verified"

  await api.close()
  doAssert openedConnections == 1,
    "expected every stream to multiplex over ONE connection; opened " &
    $openedConnections
  echo "ok: all ", n * 3, " transfers multiplexed over a single h2 connection"

# `api.close()` joins the h2 mux's background reader (see H2Mux.close), so the
# process exits cleanly here with no dispatcher-teardown crash.
waitFor main()
