## Multi-server interop (async). Built twice:
##   nim c ...                 -> navi/asyncdispatch (expects HTTP/2)
##   nim c -d:useChronos ...   -> navi/chronos       (HTTP/1.1: BearSSL has no ALPN,
##                                                     so this exercises the h1 + TLS
##                                                     path against real servers)
## Driven by servers.sh (NAVI_SERVERS / NAVI_INTEROP_CERT).

import std/[os, strutils]
when defined(useChronos):
  import navi/chronos
  const backend = "chronos"
  const wantVersion = "HTTP/1.1"
else:
  import navi/asyncdispatch
  const backend = "asyncdispatch"
  const wantVersion = "HTTP/2"

proc client(): Navi =
  # Read the cert path here (not from a global) so the closure stays GC-safe
  # under chronos's async macro.
  var cfg = newNaviConfig()
  cfg.tls.caFile = getEnv("NAVI_INTEROP_CERT")
  newNavi(cfg)

proc main() {.async.} =
  for entry in getEnv("NAVI_SERVERS").splitWhitespace():
    let p = entry.split('=', 1)
    let name = p[0]
    let url = p[1]
    let api = client()
    let r = await api.get(url & "/hello.txt")
    doAssert r.status == 200, name & ": status " & $r.status
    doAssert r.httpVersion == wantVersion,
      name & ": version " & r.httpVersion & " (want " & wantVersion & ")"
    doAssert r.body == "navi interop\n", name & ": body " & r.body
    let big = await api.get(url & "/large.bin")
    doAssert big.status == 200 and big.body.len == 262144,
      name & ": large.bin " & $big.status & " len " & $big.body.len
    echo "[", backend, "] ", name, " (", url, "): ", r.httpVersion, " OK"
  echo "== ", backend, ": all servers OK =="

waitFor main()
