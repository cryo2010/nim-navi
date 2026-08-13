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

# 3. A POST with a body goes over h3 and the origin echoes the body back.
let r3 = api.post("https://localhost:4433/echo", body = "hello-body-42")
doAssert r3.status == 200, "POST status " & $r3.status
doAssert r3.httpVersion == "HTTP/3", "POST should be h3, got " & r3.httpVersion
doAssert r3.body == "echo:hello-body-42", "POST echo mismatch: " & r3.body
echo "POST over ", r3.httpVersion, " echoed the uploaded body"

# 4. PUT likewise carries its body over h3.
let r4 = api.put("https://localhost:4433/echo", body = "put-99")
doAssert r4.status == 200 and r4.httpVersion == "HTTP/3"
doAssert r4.body == "echo:put-99", "PUT echo mismatch: " & r4.body
echo "PUT over ", r4.httpVersion, " echoed the uploaded body"

# 5. Compression over h3: the server gzips /big; navi forwards accept-encoding
#    and the policy layer decodes the response, so the body arrives as plaintext.
let expectedBig = getEnv("BIG")
doAssert expectedBig.len > 0, "BIG must be set by run.sh"
let big = api.get("https://localhost:4433/big")
doAssert big.status == 200 and big.httpVersion == "HTTP/3"
doAssert big.body == expectedBig, "decoded /big mismatch (got " & $big.body.len & " bytes)"
doAssert big.headers.get("content-encoding") == "",
  "content-encoding should be stripped after decode"
echo "GET /big over h3 decoded ", big.body.len, " bytes"

api.close()

# Prove the body really was gzipped on the wire: a client with decompression off
# and an explicit accept-encoding sees the raw, smaller, gzip-encoded body.
var rawCfg = initNaviConfig()
rawCfg.tls.caFile = ca
rawCfg.http = {H1, H2, H3}
rawCfg.decompress = false
let raw = newNavi(rawCfg)
discard raw.get("https://localhost:4433/")            # h2 first, to learn Alt-Svc
let rawBig = raw.get("https://localhost:4433/big",
                     headers = initHeaders([("accept-encoding", "gzip")]))
doAssert rawBig.httpVersion == "HTTP/3"
doAssert rawBig.headers.get("content-encoding") == "gzip",
  "expected gzip on the wire, got: '" & rawBig.headers.get("content-encoding") & "'"
doAssert rawBig.body.len < expectedBig.len, "compressed body should be smaller"
raw.close()
echo "wire body was gzip (", rawBig.body.len, " < ", expectedBig.len, " bytes)"

echo "NAVI HTTP/3 DISPATCH OK"
