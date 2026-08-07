## File-streaming interop client (sync backend). Driven by streaming.sh, which
## stands up a server for the requested protocol (nghttpd for http/2, a local
## HTTP/1.1 server for http/1.1) and exports:
##   NAVI_STREAM_URL    base URL
##   NAVI_STREAM_DIR    "upload" | "download"
##   NAVI_STREAM_PROTO  "HTTP/1.1" | "HTTP/2" (the transport we must actually use)
##   NAVI_STREAM_CERT   CA/cert to trust ("" for cleartext http/1.1)
##   NAVI_STREAM_FILE   the original payload on disk
##
## Streams the transfer and asserts (a) it ran over the intended protocol and
## (b) the bytes match the original -- upload via the server's verbatim echo,
## download via the file written to disk.
import std/os
import checksums/sha1
import navi

let
  base = getEnv("NAVI_STREAM_URL")
  dir = getEnv("NAVI_STREAM_DIR")
  proto = getEnv("NAVI_STREAM_PROTO")
  cert = getEnv("NAVI_STREAM_CERT")
  original = getEnv("NAVI_STREAM_FILE")

proc client(): Navi =
  var cfg = initNaviConfig()
  cfg.retry.limit = 0
  if cert.len > 0: cfg.tls.caFile = cert
  newNavi(cfg)

proc runUpload() =
  let api = client()
  var f = open(original, fmRead)
  defer: f.close()
  var buf = newString(64 * 1024)
  var sent = 0
  var headers = initHeaders()
  headers["content-type"] = "application/octet-stream"
  let res = api.request(POST, base & "/echo", headers = headers,
    bodyStream = proc(): string =
      let n = f.readBuffer(addr buf[0], buf.len)
      if n <= 0: return ""
      sent += n
      buf[0 ..< n])
  doAssert res.status == 200, "unexpected status " & $res.status
  doAssert res.httpVersion == proto,
    "expected " & proto & " but negotiated " & res.httpVersion
  doAssert secureHash(res.body) == secureHash(readFile(original)),
    "echoed upload does not match the original"
  echo "OK  ", proto, " upload verified (", sent, " bytes streamed, echo hash-matched)"

proc runDownload() =
  let api = client()
  let outPath = getTempDir() / "navi-stream-dl.bin"
  var f = open(outPath, fmWrite)
  var written = 0
  let res = api.stream(GET, base & "/download",
    sink = proc(chunk: openArray[byte]) =
      if chunk.len > 0:
        discard f.writeBuffer(unsafeAddr chunk[0], chunk.len)
        written += chunk.len)
  f.close()
  doAssert res.status == 200, "unexpected status " & $res.status
  doAssert res.httpVersion == proto,
    "expected " & proto & " but negotiated " & res.httpVersion
  doAssert secureHash(readFile(outPath)) == secureHash(readFile(original)),
    "downloaded file does not match the original"
  echo "OK  ", proto, " download verified (", written, " bytes to disk, hash-matched)"

if dir == "upload": runUpload() else: runDownload()
