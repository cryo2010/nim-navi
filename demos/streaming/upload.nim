## navi/asyncdispatch streaming upload, verified end to end.
##
## Streams a local file as a chunked request body (constant memory: a pull-based
## `bodyStream` producer navi calls per chunk) over HTTP/2, then checks the SHA-1
## the server computed for what it received against the file's own SHA-1.

import std/[os, strutils, json]
import checksums/sha1
import navi/asyncdispatch

proc sampleContent(bytes: int): string =
  ## Deterministic, varied bytes (a small LCG) -- no fixture file needed.
  result = newStringOfCap(bytes)
  var x = 0x12345678'u32
  for _ in 0 ..< bytes:
    x = x * 1664525'u32 + 1013904223'u32
    result.add char(x shr 24)

proc main() {.async.} =
  var cfg = initNaviConfig()
  cfg.tls.verify = false                       # demo server uses a self-signed cert
  let api = newNavi(cfg)

  let path = "/tmp/navi-upload.bin"
  writeFile(path, sampleContent(3 * 1024 * 1024))   # 3 MiB
  let localSha = ($secureHash(readFile(path))).toLowerAscii

  var f = open(path, fmRead)
  defer: f.close()
  const chunkSize = 64 * 1024
  var buf = newString(chunkSize)
  var sent = 0
  var headers = initHeaders()
  headers["content-type"] = "application/octet-stream"

  let res = await api.request(POST, getEnv("BASE") & "/upload", headers = headers,
    bodyStream = proc(): string =
      let n = f.readBuffer(addr buf[0], buf.len)
      if n <= 0: return ""
      sent += n
      buf[0 ..< n])

  doAssert res.status == 200, "unexpected status " & $res.status
  let serverSha = parseJson(res.body){"sha1"}.getStr.toLowerAscii
  echo "upload: ", sent, " bytes over ", res.httpVersion
  echo "  local  sha1 = ", localSha
  echo "  server sha1 = ", serverSha
  doAssert serverSha == localSha, "server received bytes that do NOT match the original"
  echo "  verified: streamed upload matches the original -- ok"

waitFor main()
