## Stream an HTTP response body straight to a file -- constant memory, whatever the
## size -- and verify it arrived intact. `stream` returns a handle whose status and
## headers are ready immediately; `each` then pulls the body a chunk at a time as it
## comes off the socket (decompressing on the way if the server encoded the body).
## A local server serves a known payload, so we SHA-1 the downloaded file against
## the original.
##
##   nim c -r examples/streaming/download_file.nim [outPath]

import std/os
import checksums/sha1
import navi
import ./echo_server

proc main() =
  let outPath = if paramCount() >= 1: paramStr(1)
                else: getTempDir() / "navi-download.bin"
  let payload = sampleContent(3 * 1024 * 1024)       # 3 MiB
  let original = secureHash(payload)

  const port = 8081
  var th: Thread[ServerArgs]
  startEchoServer(th, port, payload)                 # GET returns the payload
  sleep(200)                                         # let the server bind

  let api = newNavi()
  var f = open(outPath, fmWrite)
  var written = 0
  # Status and headers land first; the body is pulled on demand. Write each chunk
  # as it arrives, so memory stays flat no matter how large the response is.
  let res = api.stream(GET, "http://127.0.0.1:" & $port & "/")
  res.each(chunk):
    if chunk.len > 0:
      discard f.writeBuffer(unsafeAddr chunk[0], chunk.len)
      written += chunk.len
  f.close()                                          # flush before hashing

  echo "downloaded: ", written, " bytes -> ", outPath
  echo "status:     ", res.status
  doAssert res.status == 200, "unexpected status " & $res.status
  doAssert secureHash(readFile(outPath)) == original,
    "downloaded file does NOT match the original"
  echo "verified: SHA-1 of the downloaded file matches the original -- ok"

main()
