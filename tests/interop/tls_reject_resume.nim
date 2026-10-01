## A session from a peer navi rejected after the handshake must not be re-offered
## to the origin on the next connect (#440), on all three native backends. Built
## three times from tests/interop/tls_pin.sh: plain (sync), -d:naviAsync
## (asyncdispatch) and -d:naviChronos (chronos).
##
## `openssl s_server -www` names the session state of the connection it is
## answering on in the page it serves ("New, TLSv1.3, ..." / "Reused, TLSv1.2,
## ...") and closes that connection afterwards, so two sequential requests from
## one client are two handshakes and the page says whether the second resumed.
##
## Each scenario makes exactly two requests through ONE client (so one session
## cache), with the FIRST one rejected after its handshake completed:
##
##   * `mControl` rejects nothing, so the second request must report "Reused" --
##     without that leg a "New" below would prove nothing about the eviction.
##   * `mPin` gives the client a pin the server cannot match, then puts the real
##     pin on the live config before the second request.
##   * `mCallback` installs a verify callback that refuses only its first call.
##
## Every scenario runs twice, pinned to TLS 1.2 and to TLS 1.3, because the
## new-session callback fires at opposite ends of the rejection in the two
## versions: inside the handshake for 1.2 (so only an eviction can undo the
## caching), and during the first reads for 1.3 (so the slot must also refuse a
## ticket that arrives after the rejection).
##
## What each backend proves, measured by reverting the fix and re-running: the
## stale offer is visible on the wire only on chronos. The sync and asyncdispatch
## reject paths free the SSL without an SSL_shutdown, so OpenSSL's
## ssl_clear_bad_session marks the session it had just cached not_resumable and
## the server refuses it anyway (the same mechanism tls_resume.nim's header
## describes, there as a bug); chronos shuts down cleanly since #431, so its copy
## stays resumable and the origin really does resume onto the rejected peer's
## session. Those two legs are kept regardless: they pin that behaviour rather
## than leaving it to a teardown detail, and they would catch the same defect the
## day either backend starts closing cleanly. The other half of #440 -- keeping a
## rejected peer's session, with its certificate, in the client's memory -- is not
## observable from the server, and is covered in tests/test_tls_session.nim.
##
## The URL, CA path and pin are passed as proc parameters rather than read from
## module globals: chronos's async transform is gcsafe-strict and forbids the
## nested coroutine from touching GC'd globals (as nghttpd_chronos.nim notes).
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

const badPin = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

type
  Mode = enum
    mControl    ## nothing is rejected
    mPin        ## an SPKI pin the server cannot match, fixed before request two
    mCallback   ## a verify callback that refuses only its first call

  Env = object
    ## What the shell script exported, carried by value into the coroutines.
    url, caPath, goodPin: string

proc config(env: Env, v: TlsVersion, mode: Mode, calls: ref int): NaviConfig =
  result = initNaviConfig()
  result.tls.caFile = env.caPath
  result.tls.resumeSessions = true   # the default; spelled out, it is under test
  result.tls.minVersion = v
  result.tls.maxVersion = v
  result.throwHttpErrors = false
  result.retry.limit = 0             # one attempt per request, so one handshake
  case mode
  of mControl: discard
  of mPin: result.tls.pinnedKeys = @[badPin]
  of mCallback:
    # Refuses its first call only. The closure's environment is shared with the
    # client's copy of the config, so the counter survives that copy.
    result.tls.verifyCallback = proc(leafDer: string): bool {.gcsafe.} =
      inc calls[]
      calls[] > 1

template scenarioBody(env: Env, v: TlsVersion, mode: Mode) =
  ## Two requests through one client. `rejected` records whether the first was
  ## refused; `second` is the page the second request got.
  let calls = new(int)
  let api = newNavi(config(env, v, mode, calls))
  try:
    discard (await0 api.get(env.url)).body
  except CatchableError:
    rejected = true
  if mode == mPin:
    # The live config the next connect reads (navi re-reads client.config per
    # request), so the second handshake is judged against the server's real pin.
    api.config.tls.pinnedKeys = @[env.goodPin]
  second = (await0 api.get(env.url)).body
  await0 api.close()

when defined(naviChronos) or defined(naviAsync):
  template await0(e: untyped): untyped = await e
  proc runAsync(env: Env, v: TlsVersion,
                mode: Mode): Future[(bool, string)] {.async.} =
    var rejected = false
    var second: string
    scenarioBody(env, v, mode)
    return (rejected, second)
  proc run(env: Env, v: TlsVersion, mode: Mode): (bool, string) =
    waitFor runAsync(env, v, mode)
else:
  template await0(e: untyped): untyped = e
  proc run(env: Env, v: TlsVersion, mode: Mode): (bool, string) =
    var rejected = false
    var second: string
    scenarioBody(env, v, mode)
    (rejected, second)

proc label(v: TlsVersion): string = (if v == tls12: "TLS 1.2" else: "TLS 1.3")

proc head(page: string): string =
  let s = page.strip()
  s[0 ..< min(160, s.len)]

let env = Env(url: getEnv("NAVI_REJ_URL") & "/",
              caPath: getEnv("NAVI_REJ_CA"),
              goodPin: getEnv("NAVI_REJ_PIN"))

for v in [tls12, tls13]:
  # The control: this origin and this backend really do resume, so a "New" in the
  # rejection legs below is the eviction and not a setup that never resumed.
  let (ctlRejected, ctlSecond) = run(env, v, mControl)
  doAssert not ctlRejected,
    backend & " / " & label(v) & ": the control request should not be rejected"
  doAssert "Reused, TLS" in ctlSecond,
    backend & " / " & label(v) & ": the control did NOT resume, so this origin " &
    "cannot tell an eviction from a setup that never resumes; page said: " &
    head(ctlSecond)

  for mode in [mPin, mCallback]:
    let (rejected, second) = run(env, v, mode)
    doAssert rejected,
      backend & " / " & label(v) & " / " & $mode &
      ": the first request should have been rejected after its handshake"
    doAssert "Reused, TLS" notin second,
      backend & " / " & label(v) & " / " & $mode &
      ": the rejected peer's session was re-offered and RESUMED; page said: " &
      head(second)
    doAssert "New, TLS" in second,
      backend & " / " & label(v) & " / " & $mode &
      ": expected a full handshake on the second request; page said: " & head(second)
    echo "== ", backend, " / ", label(v), " / ", $mode,
         ": rejected session evicted, second connection was a full handshake OK =="
