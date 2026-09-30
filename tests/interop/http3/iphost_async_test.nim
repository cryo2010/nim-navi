## Interop test: an IP-literal origin on the h3 leg is bound to the certificate's
## iPAddress SAN and is never offered as SNI (#451), asyncdispatch opener. Same
## origin, certificate and /sni echo route as iphost_test.nim; `openConnAsync` shares
## navi_h3_new with the sync opener, so this pins the fix for the async path too.
## Built with -d:ssl -d:naviHttp3.
import std/[asyncdispatch, os, strutils]
import navi/backend/quic_async

proc main() {.async.} =
  let ca = getEnv("NAVI_H3_CA")
  doAssert ca.len > 0, "NAVI_H3_CA must point at the origin cert"

  let c = await openConnAsync("127.0.0.1", 4433, "127.0.0.1", TlsConfig(caFile: ca))
  let r = await c.requestOnConn("GET", "/sni", @[], "")
  doAssert r.status == 200, "verified request to the IP origin failed: " & $r.status
  doAssert r.body == "sni=", "the IP literal was sent as SNI: " & r.body
  await c.closeConn()
  echo "ok: IP-literal origin verified, no SNI sent (asyncdispatch)"

  # The rejection must also say why: since #446 the driver records the reason instead
  # of printing it to stderr, so this is a `QuicTlsError` (a `QuicError` subtype, so
  # the h2/h1 fallback is unchanged) whose message carries the X509 verify error text.
  var rejected = false
  var reason = ""
  try:
    let bad = await openConnAsync("127.0.0.1", 4433, "127.0.0.2", TlsConfig(caFile: ca))
    await bad.closeConn()
  except QuicTlsError as e:
    rejected = true
    reason = e.msg
  except QuicError as e:
    doAssert false, "a verification rejection was not a QuicTlsError: " & e.msg
  doAssert rejected, "a certificate without IP:127.0.0.2 was accepted"
  doAssert "IP address mismatch" in reason,
    "the X509 verify reason was not surfaced: " & reason
  echo "ok: mismatched IP literal rejected as QuicTlsError naming the X509 error (asyncdispatch)"

  let dns = await openConnAsync("localhost", 4433, "localhost", TlsConfig(caFile: ca))
  let rd = await dns.requestOnConn("GET", "/sni", @[], "")
  doAssert rd.body == "sni=localhost", "unexpected SNI for a DNS origin: " & rd.body
  await dns.closeConn()
  echo "ok: DNS origin still sends SNI (asyncdispatch)"

waitFor main()
echo "NAVI HTTP/3 IP-LITERAL (asyncdispatch) OK"
