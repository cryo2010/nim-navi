## navi/asyncdispatch streaming download, verified end to end.
##
## Streams a response body straight to a file (constant memory: each chunk is
## written as it lands) over HTTP/2, then checks the file's SHA-1 against the
## `x-sha1` header the server computed for the original payload.

import std/[os, strutils]
import checksums/sha1
import navi/asyncdispatch

proc main() {.async.} =
  var cfg = initNaviConfig()
  cfg.tls.verify = false                       # demo server uses a self-signed cert
  let api = newNavi(cfg)
  let outPath = "/tmp/navi-download.bin"

  var f = open(outPath, fmWrite)
  var written = 0
  let res = await api.stream(GET, getEnv("BASE") & "/download",
    sink = proc(chunk: seq[byte]) {.async.} =
      if chunk.len > 0:
        discard f.writeBuffer(unsafeAddr chunk[0], chunk.len)
        written += chunk.len)
  f.close()

  let expected = res.headers.get("x-sha1").toLowerAscii
  let got = ($secureHash(readFile(outPath))).toLowerAscii
  echo "download: ", written, " bytes over ", res.httpVersion
  echo "  server sha1 = ", expected
  echo "  local  sha1 = ", got
  doAssert res.status == 200, "unexpected status " & $res.status
  doAssert got == expected, "downloaded file does NOT match the server's original"
  echo "  verified: streamed download matches the original -- ok"

waitFor main()
