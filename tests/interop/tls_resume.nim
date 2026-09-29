## TLS session resumption, on all three native backends (#431). Built three
## times from tests/interop/ca_verify.sh: plain (sync), -d:naviAsync
## (asyncdispatch) and -d:naviChronos (chronos).
##
## `openssl s_server -www` reports the session state of the connection it is
## answering on in the page it serves ("New, TLSv1.3, ..." or "Reused, TLSv1.3,
## ..."), and it closes the connection after every response, so two sequential
## requests from ONE client are two handshakes: the second can only say "Reused"
## if the session navi cached for that origin was actually presented AND accepted.
##
## The chronos backend used to fail this: `close` called SSL_free without ever
## calling SSL_shutdown, and OpenSSL then runs ssl_clear_bad_session, which marks
## the very SSL_SESSION navi cached as not_resumable, so every later connection to
## the origin did a full handshake.
import std/[os, strutils]

when defined(naviChronos):
  import pkg/chronos
  import navi/chronos
  const backend = "chronos"
elif defined(naviAsync):
  import std/asyncdispatch
  import navi/asyncdispatch
  const backend = "asyncdispatch"
else:
  import navi
  const backend = "sync"

proc config(): NaviConfig =
  result = initNaviConfig()
  result.tls.caFile = getEnv("NAVI_CAFILE_CA")
  result.throwHttpErrors = false
  result.retry.limit = 0
  # resumeSessions is on by default; spelled out because it is what is under test.
  result.tls.resumeSessions = true

when defined(naviChronos) or defined(naviAsync):
  proc twoPages(url: string): Future[(string, string)] {.async.} =
    let api = newNavi(config())
    let first = (await api.get(url)).body
    let second = (await api.get(url)).body
    await api.close()
    return (first, second)

  proc pages(url: string): (string, string) = waitFor twoPages(url)
else:
  proc pages(url: string): (string, string) =
    let api = newNavi(config())
    let first = api.get(url).body
    let second = api.get(url).body
    api.close()
    (first, second)

let url = getEnv("NAVI_CAFILE_URL") & "/"
let (first, second) = pages(url)

doAssert "New, TLS" in first,
  backend & ": the first connection should be a full handshake, page said: " &
  first.strip()[0 ..< min(200, first.strip().len)]
doAssert "Reused, TLS" in second,
  backend & ": the second connection did NOT resume the TLS session"
echo "== ", backend, ": TLS session resumed on the second connection OK =="
