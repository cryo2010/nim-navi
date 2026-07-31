## TLS version pinning enforcement (sync/OpenSSL backend). Driven by
## tls_version.sh, which stands up an `openssl s_server` pinned to TLS 1.2 on one
## port and TLS 1.3 on another. Verifies navi honors `minVersion`/`maxVersion`: a
## pin that excludes the server's only version fails the handshake, while a pin
## that includes it succeeds. Covers both the min and the max bound.
import std/[os, strutils]
import navi

let p12 = parseInt(getEnv("NAVI_TV_PORT12"))   # server that speaks only TLS 1.2
let p13 = parseInt(getEnv("NAVI_TV_PORT13"))   # server that speaks only TLS 1.3

proc connects(port: int, minV = tlsDefault, maxV = tlsDefault): bool =
  ## Whether a GET to the server on `port` succeeds with the given version pins.
  var cfg = initNaviConfig()
  cfg.tls.verify = false                        # self-signed test cert
  cfg.tls.minVersion = minV
  cfg.tls.maxVersion = maxV
  cfg.retry.limit = 0
  cfg.headers["connection"] = "close"
  let api = newNavi(cfg)
  try:
    result = api.get("https://127.0.0.1:" & $port & "/").status == 200
  except CatchableError:
    result = false
  api.close()

# minVersion, against the TLS 1.2-only server
doAssert connects(p12), "default should connect to the TLS 1.2 server"
doAssert connects(p12, minV = tls12), "min=tls12 should connect to the TLS 1.2 server"
doAssert not connects(p12, minV = tls13),
  "min=tls13 must be rejected by the TLS 1.2 server"
echo "OK  minVersion enforced: TLS 1.2 server rejects min=tls13, accepts min=tls12"

# maxVersion, against the TLS 1.3-only server
doAssert connects(p13), "default should connect to the TLS 1.3 server"
doAssert connects(p13, maxV = tls13), "max=tls13 should connect to the TLS 1.3 server"
doAssert not connects(p13, maxV = tls12),
  "max=tls12 must be rejected by the TLS 1.3 server"
echo "OK  maxVersion enforced: TLS 1.3 server rejects max=tls12, accepts max=tls13"

echo "== tls version pinning passed =="
