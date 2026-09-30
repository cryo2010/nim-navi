## Interop test: an IP-literal origin on the h3 leg is bound to the certificate's
## iPAddress SAN and is never offered as SNI (#451), chronos opener. Same origin,
## certificate and /sni echo route as iphost_test.nim; `openConnChronos` shares
## navi_h3_new with the sync opener, so this pins the fix for the chronos path too.
## Built with -d:ssl -d:naviHttp3.
import std/os
import pkg/chronos
import navi/backend/quic_chronos

proc main() {.async.} =
  let ca = getEnv("NAVI_H3_CA")
  doAssert ca.len > 0, "NAVI_H3_CA must point at the origin cert"

  let c = await openConnChronos("127.0.0.1", 4433, "127.0.0.1", TlsConfig(caFile: ca))
  let r = await c.requestOnConn("GET", "/sni", @[], "")
  doAssert r.status == 200, "verified request to the IP origin failed: " & $r.status
  doAssert r.body == "sni=", "the IP literal was sent as SNI: " & r.body
  await c.closeConn()
  echo "ok: IP-literal origin verified, no SNI sent (chronos)"

  var rejected = false
  try:
    let bad = await openConnChronos("127.0.0.1", 4433, "127.0.0.2", TlsConfig(caFile: ca))
    await bad.closeConn()
  except QuicError:
    rejected = true
  doAssert rejected, "a certificate without IP:127.0.0.2 was accepted"
  echo "ok: mismatched IP literal rejected (chronos)"

  let dns = await openConnChronos("localhost", 4433, "localhost", TlsConfig(caFile: ca))
  let rd = await dns.requestOnConn("GET", "/sni", @[], "")
  doAssert rd.body == "sni=localhost", "unexpected SNI for a DNS origin: " & rd.body
  await dns.closeConn()
  echo "ok: DNS origin still sends SNI (chronos)"

waitFor main()
echo "NAVI HTTP/3 IP-LITERAL (chronos) OK"
