## TLS version pinning enforcement on the chronos backend. The chronos analog of
## tls_version.nim, driven by the same tls_version.sh servers (TLS 1.2 on one
## port, TLS 1.3 on another). Now that chronos runs OpenSSL it can negotiate TLS
## 1.3 -- impossible under its old BearSSL stack -- so it honors both the min and
## max version pins exactly like the sync/asyncdispatch backends.
import std/[os, strutils]
import pkg/chronos
import navi/chronos

let p12 = parseInt(getEnv("NAVI_TV_PORT12"))   # server that speaks only TLS 1.2
let p13 = parseInt(getEnv("NAVI_TV_PORT13"))   # server that speaks only TLS 1.3

proc connects(port: int, minV = tlsDefault, maxV = tlsDefault): Future[bool] {.async.} =
  ## Whether a GET to the server on `port` succeeds with the given version pins.
  var cfg = initNaviConfig()
  cfg.tls.verify = false                        # self-signed test cert
  cfg.tls.minVersion = minV
  cfg.tls.maxVersion = maxV
  cfg.retry.limit = 0
  cfg.headers["connection"] = "close"
  let api = newNavi(cfg)
  try:
    result = (await api.get("https://127.0.0.1:" & $port & "/")).status == 200
  except CatchableError:
    result = false
  await api.close()

proc main() {.async.} =
  # minVersion, against the TLS 1.2-only server
  doAssert (await connects(p12)), "default should connect to the TLS 1.2 server"
  doAssert (await connects(p12, minV = tls12)),
    "min=tls12 should connect to the TLS 1.2 server"
  doAssert not (await connects(p12, minV = tls13)),
    "min=tls13 must be rejected by the TLS 1.2 server"
  echo "OK  minVersion enforced (chronos): TLS 1.2 server rejects min=tls13, accepts min=tls12"

  # maxVersion, against the TLS 1.3-only server (BearSSL could never reach here)
  doAssert (await connects(p13)), "default should connect to the TLS 1.3 server"
  doAssert (await connects(p13, maxV = tls13)),
    "max=tls13 should connect to the TLS 1.3 server"
  doAssert not (await connects(p13, maxV = tls12)),
    "max=tls12 must be rejected by the TLS 1.3 server"
  echo "OK  maxVersion enforced (chronos): TLS 1.3 server rejects max=tls12, accepts max=tls13"

  echo "== tls version pinning passed (chronos) =="

waitFor main()
