## TLS over a Unix domain socket on the chronos backend (the `pkUnix` establish
## path). Driven by tests/interop/unixsocket.sh, which runs an `openssl s_server
## -unix` whose certificate is valid for uds.test only, and exports
## NAVI_UDS_TLS_PATH / NAVI_UDS_TLS_CA.
##
## The negative cases are the regression guard for #420: a handshake/verification
## failure on this path used to leave `conn.tls` pointing at a live, fully
## handshaken SSL, and with `timeouts.connect` set chronos's `withTimeout`
## (which completes `true` when the inner future FAILED) handed that Conn back to
## the caller, which then sent the request over an unverified session.
import std/[os, strutils]
import pkg/chronos
import navi/chronos

proc client(connectMs: int): Navi =
  # Read the environment here rather than into globals: chronos's async macro
  # requires the callers to be GC-safe, and a global string is not.
  var cfg = initNaviConfig()
  cfg.unixSocket = getEnv("NAVI_UDS_TLS_PATH")
  cfg.tls.caFile = getEnv("NAVI_UDS_TLS_CA")
  cfg.timeouts.connect = connectMs
  cfg.throwHttpErrors = false
  cfg.retry.limit = 0
  newNavi(cfg)

proc failure(url: string, connectMs: int): Future[string] {.async.} =
  ## "" when the request unexpectedly SUCCEEDED (which is the #420 bug: the
  ## request went out over a session whose identity check failed); otherwise the
  ## message navi surfaced.
  let api = client(connectMs)
  try:
    discard await api.get(url)
    return ""
  except CatchableError as e:
    return e.msg
  finally:
    await api.close()

proc main() {.async.} =
  # Positive: TLS layered over the Unix socket, certificate matching the URL host.
  block:
    let api = client(0)
    let r = await api.get("https://uds.test/")
    doAssert r.status == 200, "chronos uds TLS: status " & $r.status
    await api.close()

  # Negative: the certificate is valid for uds.test, not for other.test. The
  # hostname check must reject it -- with and without a connect timeout, and the
  # reason must be the TLS one, not a generic closed-connection error.
  for connectMs in [0, 5000]:
    let msg = await failure("https://other.test/", connectMs)
    doAssert msg.len > 0,
      "chronos uds TLS: unverified session accepted (connectMs=" & $connectMs & ")"
    doAssert "closed connection" notin msg,
      "chronos uds TLS: lost the TLS error (connectMs=" & $connectMs & "): " & msg
    doAssert ("TLS" in msg or "certificate" in msg or "does not match" in msg),
      "chronos uds TLS: unexpected error (connectMs=" & $connectMs & "): " & msg

  echo "== chronos: TLS over Unix socket, verify + failure teardown OK =="

waitFor main()
