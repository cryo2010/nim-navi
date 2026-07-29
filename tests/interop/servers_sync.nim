## Multi-server HTTP/2 interop (sync backend). Driven by servers.sh, which starts
## nginx / Caddy / h2o over TLS and exports NAVI_SERVERS ("name=url ..." pairs)
## and NAVI_INTEROP_CERT. Exercises navi's h2 client against several unrelated
## server implementations, not just the nghttp2 reference: negotiates h2 over
## ALPN, reads a small file and a 256 KiB body (receive-side flow control).

import std/[os, strutils]
import navi

let cert = getEnv("NAVI_INTEROP_CERT")

proc client(): Navi =
  var cfg = initNaviConfig()
  cfg.tls.caFile = cert
  # A read timeout on purpose: these servers keep the connection alive, and the
  # responses (hello.txt is 13 bytes) end well inside one recv buffer. That is
  # the exact shape that made the sync backend's timeout path stall until the
  # deadline; keeping a timeout here guards against that regression.
  cfg.timeout = 15_000
  newNavi(cfg)

for entry in getEnv("NAVI_SERVERS").splitWhitespace():
  let p = entry.split('=', 1)
  let name = p[0]
  let url = p[1]
  let api = client()
  let r = api.get(url & "/hello.txt")
  doAssert r.status == 200, name & ": status " & $r.status
  doAssert r.httpVersion == "HTTP/2", name & ": version " & r.httpVersion
  doAssert r.body == "navi interop\n", name & ": body " & r.body
  let big = api.get(url & "/large.bin")
  doAssert big.status == 200 and big.body.len == 262144,
    name & ": large.bin " & $big.status & " len " & $big.body.len
  echo "[sync] ", name, " (", url, "): ", r.httpVersion, " OK"
echo "== sync: all servers OK =="
