## Interop test: an IP-literal origin on the h3 leg is bound to the certificate's
## iPAddress SAN and is never offered as SNI (#451). The QUIC SSL used to hand every
## origin to SSL_set1_host and then send it verbatim as server_name -- but RFC 6066 3
## forbids an IP literal there, and SSL_set1_host is a DNS-name entry point whose IP
## handling is an internal fallback that a bracketed literal slips past. Both halves
## are now explicit, matching openssl_ctx.nim on the TCP backends.
##
## The run.sh origin cert carries DNS:localhost and IP:127.0.0.1, and the /sni route
## echoes the server_name the ClientHello carried. Sync opener. -d:ssl -d:naviHttp3.
import std/os
import navi/backend/quic

let ca = getEnv("NAVI_H3_CA")
doAssert ca.len > 0, "NAVI_H3_CA must point at the origin cert"

# 1. A verified GET straight to the IP literal. IP:127.0.0.1 is the only SAN that
#    can match it, so this fails outright if the address is not bound as one.
let r1 = h3Get("127.0.0.1", 4433, sni = "127.0.0.1", caFile = ca)
doAssert r1.status == 200, "verified GET to the IP origin failed: " & $r1.status
echo "ok: IP-literal origin verified against the iPAddress SAN (sync)"

# 2. ...and no server_name went out with it (RFC 6066 3).
let r2 = h3Get("127.0.0.1", 4433, sni = "127.0.0.1", path = "/sni", caFile = ca)
doAssert r2.body == "sni=", "the IP literal was sent as SNI: " & r2.body
echo "ok: no SNI sent for an IP-literal origin (sync)"

# 3. The identity check is real, not skipped: the same origin, required to prove a
#    different address, must be rejected.
var rejected = false
try:
  discard h3Get("127.0.0.1", 4433, sni = "127.0.0.2", caFile = ca)
except QuicError:
  rejected = true
doAssert rejected, "a certificate without IP:127.0.0.2 was accepted"
echo "ok: mismatched IP literal rejected (sync)"

# 4. A bracketed literal, the form a URL authority uses for IPv6, is recognised as
#    an address rather than left to be matched as a DNS name, which nothing answers.
#    Only the handshake is asserted: the bracketed form is not a routable :authority
#    for this origin, so the request itself is beside the point here.
let c4 = h3Open("127.0.0.1", 4433, sni = "[127.0.0.1]", tls = TlsConfig(caFile: ca))
c4.close()
echo "ok: bracketed IP literal verified against the iPAddress SAN (sync)"

# 5. A DNS host still takes the dNSName path, SNI included.
let r5 = h3Get("localhost", 4433, sni = "localhost", path = "/sni", caFile = ca)
doAssert r5.status == 200, "verified GET to the DNS origin failed: " & $r5.status
doAssert r5.body == "sni=localhost", "unexpected SNI for a DNS origin: " & r5.body
echo "ok: DNS origin still verified against the dNSName SAN, with SNI (sync)"

echo "NAVI HTTP/3 IP-LITERAL OK"
