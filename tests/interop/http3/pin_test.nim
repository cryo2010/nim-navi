## Interop test: TlsConfig.pinnedKeys and TlsConfig.verifyCallback are enforced on
## the HTTP/3 leg (#419). Before the fix the QUIC SSL_CTX received only caFile +
## verify, so a pin never ran on h3 and `verify=false` plus a pin (the documented
## "replace verification entirely" mode) accepted any certificate over QUIC.
##
## The origin is the run.sh Caddy instance; NAVI_H3_PIN is the base64 SHA-256 of
## its leaf SubjectPublicKeyInfo, computed by run.sh with the openssl CLI -- so this
## also checks navi's pin derivation against the canonical HPKP recipe.
import std/[strutils, os]
import navi/backend/quic

let ca = getEnv("NAVI_H3_CA")
let goodPin = getEnv("NAVI_H3_PIN")
doAssert ca.len > 0, "NAVI_H3_CA must point at the origin cert"
doAssert goodPin.len > 0, "NAVI_H3_PIN must hold the origin's SPKI pin"

# A pin that is well-formed but belongs to no key: the good one with its first
# character rotated, so it can never coincide with the real leaf.
let badPin = (if goodPin[0] == 'A': 'B' else: 'A') & goodPin[1 .. ^1]
doAssert badPin != goodPin

proc rejects(body: proc()): string =
  ## Run `body`, requiring it to raise; returns the message for the caller to check.
  try:
    body()
  except ValueError as e:
    return e.msg
  except QuicError as e:
    doAssert false, "expected a TLS rejection, got a transport error: " & e.msg
  doAssert false, "the h3 connection was accepted but should have been rejected"

# 1. The right pin, alongside normal chain verification, connects.
let r1 = h3Get("localhost", 4433, sni = "localhost", caFile = ca,
               pinnedKeys = @[goodPin])
doAssert r1.status == 200, "pinned GET failed: " & $r1.status
echo "ok: correct SPKI pin accepted (verify=true)"

# 2. A wrong pin is rejected even though the chain and hostname are fine -- the
#    mis-issued-certificate case pinning exists for.
let m2 = rejects(proc() =
  discard h3Get("localhost", 4433, sni = "localhost", caFile = ca,
                pinnedKeys = @[badPin]))
doAssert "does not match any pin" in m2, "unexpected message: " & m2
echo "ok: wrong SPKI pin rejected (verify=true)"

# 3. verify=false + the right pin: the documented "replace verification entirely"
#    mode. No chain is built, the pin is the whole check, and it passes.
let r3 = h3Get("localhost", 4433, sni = "localhost", verify = false,
               pinnedKeys = @[goodPin])
doAssert r3.status == 200, "verify=false pinned GET failed: " & $r3.status
echo "ok: correct SPKI pin accepted (verify=false)"

# 4. verify=false + a wrong pin: the on-path-attacker scenario from #419. This is
#    the case that used to connect to anything answering on UDP 443.
let m4 = rejects(proc() =
  discard h3Get("localhost", 4433, sni = "localhost", verify = false,
                pinnedKeys = @[badPin]))
doAssert "does not match any pin" in m4, "unexpected message: " & m4
echo "ok: wrong SPKI pin rejected (verify=false)"

# 5. verifyCallback sees the real leaf DER and can reject it.
var sawDer = 0
let m5 = rejects(proc() =
  let cfg = TlsConfig(insecureSkipVerify: true, verifyCallback: proc(der: string): bool =
    sawDer = der.len
    false)
  let c = h3Open("localhost", 4433, sni = "localhost", tls = cfg)
  c.close())
doAssert "verify callback rejected" in m5, "unexpected message: " & m5
doAssert sawDer > 0, "the verify callback received no certificate"
echo "ok: verifyCallback rejection honored on h3 (leaf DER ", sawDer, " bytes)"

# 6. A callback that accepts lets the connection through.
let cfg6 = TlsConfig(insecureSkipVerify: true, verifyCallback: proc(der: string): bool = der.len > 0)
let c6 = h3Open("localhost", 4433, sni = "localhost", tls = cfg6)
try:
  doAssert c6.get("/").status == 200
finally:
  c6.close()
echo "ok: verifyCallback acceptance honored on h3"

echo "NAVI HTTP/3 PIN OK"
