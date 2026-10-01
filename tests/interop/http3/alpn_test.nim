## Interop test: the HTTP/3 session is only created once the peer has selected the
## "h3" ALPN protocol (#445).
##
## navi offers only "h3", and OpenSSL rejects a server that answers with a different
## protocol (tls_parse_stoc_alpn), but nothing on this path enforced that a protocol
## was selected AT ALL: ngtcp2's crypto_ossl binding does not look, and OpenSSL's
## no_application_protocol check lives in ossl_quic_tls_tick, which only its own
## native QUIC stack runs. So a non-compliant QUIC listener that completed the
## handshake with no ALPN selection was treated as an h3 peer: navi opened the
## control/QPACK streams, submitted the request, and the failure surfaced late as a
## stream reset (`QuicSubmittedError`), which the fallback rules refuse to replay for
## a non-idempotent method. `navi_h3_bind` now requires the selection before the
## nghttp3 session exists, so the failure is a clean pre-submit `QuicError`.
##
## The peer is noalpn_server.py (aioquic, no `alpn_protocols`), on udp/4434 with the
## same certificate as the Caddy origin; run.sh starts it. Sync opener.
## -d:ssl -d:naviHttp3.
import std/[os, strutils]
import navi/backend/quic

let ca = getEnv("NAVI_H3_CA")
doAssert ca.len > 0, "NAVI_H3_CA must point at the origin cert"

# 1. The non-compliant listener. Its certificate verifies (it is the origin's own, and
#    the certificate check runs first), so the ALPN gate is the only thing that can
#    refuse this connection -- and it must, before any stream is opened.
var refused = false
try:
  let c = h3Open("127.0.0.1", 4434, sni = "localhost", tls = TlsConfig(caFile: ca),
                 connectMs = 5000)
  c.close()
except QuicSubmittedError as e:
  doAssert false, "the ALPN failure was reported as post-submit: " & e.msg
except QuicTlsError as e:
  doAssert false, "the ALPN failure was reported as a TLS rejection: " & e.msg
except QuicError as e:
  refused = true
  # The reason the driver recorded, not a fixed text (#446).
  doAssert "ALPN" in e.msg, "the ALPN reason was not surfaced: " & e.msg
doAssert refused, "a peer that selected no ALPN protocol was accepted as an h3 peer"
echo "ok: a QUIC peer that selected no ALPN protocol is refused pre-submit (sync)"

# 2. ...and the peer was told why: the CONNECTION_CLOSE navi wrote on the way out
#    carries crypto_error(no_application_protocol), i.e. transport error 0x178
#    (TLS alert 120, RFC 9001 4.8), instead of a clean NO_ERROR. The listener logs
#    every termination it sees, so this is the peer's own view of the close.
let log = getEnv("NAVI_H3_NOALPN_LOG")
doAssert log.len > 0, "NAVI_H3_NOALPN_LOG must point at the listener's log"
var sawClose = false
for _ in 1 .. 60:                  # the close is in flight; give it up to 3 s
  if "error_code=0x178" in readFile(log):
    sawClose = true
    break
  sleep(50)
doAssert sawClose,
  "the peer was not told no_application_protocol (0x178): " & readFile(log)
echo "ok: the connection was closed with crypto_error(no_application_protocol)"

# 3. ...and the gate does not refuse a real h3 peer: this request goes through the
#    same navi_h3_bind check before any stream is opened. (Every other probe in this
#    image would fail too if it did, which is the other half of the coverage.)
let r = h3Get("localhost", 4433, sni = "localhost", caFile = ca)
doAssert r.status == 200, "a real h3 request was refused by the ALPN gate: " & $r.status
echo "ok: a genuine h3 peer still passes the gate"

echo "NAVI HTTP/3 ALPN GATE OK"
