## Stream a file as the request body -- constant memory, whatever its size -- and
## verify the bytes arrived intact. `bodyStream` is a pull-based producer: navi
## calls it for each chunk and stops when it returns "". A local echo server sends
## the upload straight back, so we SHA-1 the round-trip against the original.
##
##   nim c -r examples/streaming/upload_file.nim [path]
##
## With no `path`, a multi-megabyte temp file is generated so the body outgrows the
## flow-control window and is streamed out incrementally.

import std/os
import checksums/sha1
import navi
import ./echo_server

proc main() =
  let path =
    if paramCount() >= 1: paramStr(1)
    else:
      let p = getTempDir() / "navi-upload-sample.bin"
      writeFile(p, sampleContent(3 * 1024 * 1024))   # 3 MiB
      p
  let original = secureHash(readFile(path))          # SHA-1 of the whole file

  const port = 8080
  var th: Thread[ServerArgs]
  startEchoServer(th, port)
  sleep(200)                                         # let the server bind

  let api = newNavi()
  var f = open(path, fmRead)
  defer: f.close()

  const chunkSize = 64 * 1024
  var buf = newString(chunkSize)
  var sent = 0
  var headers = initHeaders()
  headers["content-type"] = "application/octet-stream"

  # Pull-based producer: return the next chunk, or "" at end-of-file.
  let res = api.request(POST, "http://127.0.0.1:" & $port & "/", headers = headers,
    bodyStream = proc(): string =
      let n = f.readBuffer(addr buf[0], buf.len)
      if n <= 0: return ""
      sent += n
      buf[0 ..< n])

  echo "uploaded: ", sent, " bytes from ", path
  echo "status:   ", res.status, "  echoed back: ", res.body.len, " bytes"
  doAssert res.status == 200, "unexpected status " & $res.status
  doAssert secureHash(res.body) == original, "echoed upload does NOT match the original"
  echo "verified: SHA-1 of the echoed upload matches the original -- ok"

main()
