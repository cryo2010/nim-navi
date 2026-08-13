## End-to-end transparent HTTP/3 dispatch: the sync navi client talks to Caddy
## over h2 first (learning the Alt-Svc h3 advertisement), then transparently
## upgrades a subsequent GET to HTTP/3. Built with -d:ssl -d:naviHttp3.
import std/os
import std/strutils
import navi

let ca = getEnv("NAVI_H3_CA")
doAssert ca.len > 0, "NAVI_H3_CA must point at the origin cert"

var cfg = initNaviConfig()
cfg.tls.caFile = ca
cfg.http = {H1, H2, H3}          # allow h3 (opt-in)
let api = newNavi(cfg)

# 1. First request negotiates h2 (no h3 discovered yet) and sees Alt-Svc.
let r1 = api.get("https://localhost:4433/")
doAssert r1.status == 200, "first GET status " & $r1.status
doAssert r1.httpVersion == "HTTP/2", "first GET should be h2, got " & r1.httpVersion
echo "first GET over ", r1.httpVersion, " (Alt-Svc: ", r1.headers.get("alt-svc"), ")"

# 2. Next GET to the same origin transparently upgrades to HTTP/3.
let r2 = api.get("https://localhost:4433/")
doAssert r2.status == 200, "second GET status " & $r2.status
doAssert r2.httpVersion == "HTTP/3", "second GET should upgrade to h3, got " & r2.httpVersion
doAssert "hello from http/3" in r2.body
echo "second GET transparently over ", r2.httpVersion

api.close()
echo "NAVI HTTP/3 DISPATCH OK"
