## Interop test: the HTTP/3 session is only created once the peer has selected the
## "h3" ALPN protocol (#445), asyncdispatch opener.
##
## alpn_test.nim covers the same gate on the sync opener and explains why the gate
## exists. This is the twin for `openConnAsync`, which drives the handshake with its
## own loop, calls `navi_h3_bind` itself and tears the connection down from its own
## `except` arm: nothing about the refusal is shared with the sync opener except the
## C check, so the classification that makes the failure useful -- a pre-submit
## `QuicError`, never the `QuicSubmittedError` the fallback rules refuse to replay
## for a non-idempotent method -- has to be pinned per opener.
##
## The peer is noalpn_server.py (aioquic, no `alpn_protocols`) on udp/4434 with the
## same certificate as the Caddy origin; run.sh starts it. -d:ssl -d:naviHttp3.
import std/[asyncdispatch, os, strutils]
import navi/backend/quic_async

proc noAppProtoCloses(log: string): int =
  ## How many connections the listener has logged as terminating with
  ## crypto_error(no_application_protocol), i.e. transport error 0x178. Counted
  ## rather than matched: the sync probe has already left one of these in the log.
  for line in readFile(log).splitLines:
    if "error_code=0x178" in line: inc result

proc main() {.async.} =
  let ca = getEnv("NAVI_H3_CA")
  doAssert ca.len > 0, "NAVI_H3_CA must point at the origin cert"
  let log = getEnv("NAVI_H3_NOALPN_LOG")
  doAssert log.len > 0, "NAVI_H3_NOALPN_LOG must point at the listener's log"
  let before = noAppProtoCloses(log)

  # 1. The non-compliant listener. Its certificate verifies (it is the origin's own,
  #    and the certificate check runs first), so the ALPN gate is the only thing that
  #    can refuse this connection -- and it must, before any stream is opened.
  var refused = false
  var reason = ""
  try:
    let c = await openConnAsync("127.0.0.1", 4434, "localhost", TlsConfig(caFile: ca),
                                connectMs = 5000)
    await c.closeConn()
  except QuicSubmittedError as e:
    doAssert false, "the ALPN failure was reported as post-submit: " & e.msg
  except QuicTlsError as e:
    doAssert false, "the ALPN failure was reported as a TLS rejection: " & e.msg
  except QuicError as e:
    refused = true
    reason = e.msg
  doAssert refused, "a peer that selected no ALPN protocol was accepted as an h3 peer"
  # The reason the driver recorded, not a fixed text (#446).
  doAssert "ALPN" in reason, "the ALPN reason was not surfaced: " & reason
  echo "ok: a QUIC peer that selected no ALPN protocol is refused pre-submit (asyncdispatch)"

  # 2. ...and this opener's teardown told the peer why: the CONNECTION_CLOSE carries
  #    crypto_error(no_application_protocol) (TLS alert 120, RFC 9001 4.8) rather than
  #    a clean NO_ERROR. The listener logs every termination it sees, so this is the
  #    peer's own view of how the async opener closed.
  var sawClose = false
  for _ in 1 .. 60:                  # the close is in flight; give it up to 3 s
    if noAppProtoCloses(log) > before:
      sawClose = true
      break
    await sleepAsync(50)
  doAssert sawClose,
    "the peer was not told no_application_protocol (0x178): " & readFile(log)
  echo "ok: the connection was closed with crypto_error(no_application_protocol)"

  # 3. ...and the gate does not refuse a real h3 peer: this request runs the same
  #    navi_h3_bind check before any stream is opened.
  let good = await openConnAsync("localhost", 4433, "localhost", TlsConfig(caFile: ca))
  let r = await good.requestOnConn("GET", "/", @[], "")
  doAssert r.status == 200, "a real h3 request was refused by the ALPN gate: " & $r.status
  await good.closeConn()
  echo "ok: a genuine h3 peer still passes the gate (asyncdispatch)"

waitFor main()
echo "NAVI HTTP/3 ALPN GATE (asyncdispatch) OK"
