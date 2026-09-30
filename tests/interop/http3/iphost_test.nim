## Interop test: an IP-literal origin on the h3 leg is bound to the certificate's
## iPAddress SAN and is never offered as SNI (#451). The QUIC SSL used to hand every
## origin to SSL_set1_host and then send it verbatim as server_name -- but RFC 6066 3
## forbids an IP literal there, and SSL_set1_host is a DNS-name entry point whose IP
## handling is an internal fallback that a bracketed literal slips past. Both halves
## are now explicit, matching openssl_ctx.nim on the TCP backends.
##
## The last two cases are the #433 audit's remaining spellings: the unbracketed "::1"
## an https://[::1]/ URL leaves in `url.host`, and the verify-off path, where nothing
## is verified but RFC 6066 3 still forbids the literal as a server_name.
##
## The run.sh origin cert carries DNS:localhost and IP:127.0.0.1, and the /sni route
## echoes the server_name the ClientHello carried. Sync opener. -d:ssl -d:naviHttp3.
import std/[os, strutils]
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
#    different address, must be rejected -- and the rejection must say so. Since #446
#    the driver records the reason instead of printing it to stderr, so this is a
#    `QuicTlsError` (a `QuicError` subtype: the h2/h1 fallback is unchanged) whose
#    message carries the X509 verify error text.
var rejected = false
try:
  discard h3Get("127.0.0.1", 4433, sni = "127.0.0.2", caFile = ca)
except QuicTlsError as e:
  rejected = true
  doAssert "IP address mismatch" in e.msg,
    "the X509 verify reason was not surfaced: " & e.msg
except QuicError as e:
  doAssert false, "a verification rejection was not a QuicTlsError: " & e.msg
doAssert rejected, "a certificate without IP:127.0.0.2 was accepted"
echo "ok: mismatched IP literal rejected as QuicTlsError naming the X509 error (sync)"

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

# 6. The IPv6 forms #433 calls out. `url.host` for https://[::1]/ is the UNBRACKETED
#    "::1", and both spellings must reach the address matcher. The origin is dialed
#    over 127.0.0.1 (nothing serves h3 on ::1 here) and asked to prove a DIFFERENT
#    address, so the X509 text names the matcher that ran: "IP address mismatch" is
#    reachable only through X509_check_ip. Pre-#451 SSL_set1_host got the bare form
#    right by accident (it tries X509_VERIFY_PARAM_set1_ip_asc before falling back to
#    the name matcher), but the bracketed one did fall through and failed with
#    "hostname mismatch" -- the path that would also have accepted a CN=<address>
#    certificate with no SAN at all (#433).
for form in ["::1", "[::1]"]:
  var v6Rejected = false
  try:
    discard h3Get("127.0.0.1", 4433, sni = form, caFile = ca)
  except QuicTlsError as e:
    v6Rejected = true
    doAssert "IP address mismatch" in e.msg,
      "the IPv6 literal " & form & " was not matched as an address: " & e.msg
  except QuicError as e:
    # Like case 3: a verification rejection must arrive as the TLS subtype. Without
    # this arm a plain QuicError would satisfy `v6Rejected` is-false-only by never
    # being caught at all -- it would escape and fail the probe, but with the wrong
    # diagnosis. Name the real defect instead.
    doAssert false, "a verification rejection was not a QuicTlsError: " & e.msg
  doAssert v6Rejected, "a certificate without IP:::1 was accepted for " & form
echo "ok: an IPv6 literal is matched as an address in both spellings (sync)"

# 7. The verify-off path, the other half of #433: no identity is bound (nothing is
#    being verified), but an IP literal still must not go out as a server_name --
#    RFC 6066 3 does not depend on whether the client checks the certificate.
#    (127.0.0.1 rather than ::1 here only because the :authority follows the origin,
#    and the /sni route lives on the site Caddy serves for that authority.)
let r7 = h3Get("127.0.0.1", 4433, sni = "127.0.0.1", path = "/sni", verify = false)
doAssert r7.status == 200, "verify-off GET to the IP origin failed: " & $r7.status
doAssert r7.body == "sni=", "an IP literal was sent as SNI with verify off: " & r7.body
echo "ok: no SNI sent for an IP-literal origin with verification off (sync)"

echo "NAVI HTTP/3 IP-LITERAL OK"
