## A connect abandoned on `connectMs` must stop, not keep racing the pool in the
## background (issue #443). Driven by tests/interop/connect_abandon.sh, which stands
## up deaf_tcp_server.py on both loopback families on one port (so `localhost` is a
## two-address Happy-Eyeballs pool whose winner never answers the ClientHello) and
## exports NAVI_ABANDON_PORT. The script counts the accepts: exactly one connection
## may ever reach the listeners.
##
## Built twice, for the two backends that race the pool inside a single future:
##   nim c ...                -> navi/backend/asyncdispatch
##   nim c -d:useChronos ...  -> navi/backend/chronos
## Before the fix the asyncdispatch leg opened a SECOND connection (a fresh TCP race
## plus a fresh SSL_CTX/handshake) after the caller had already raised TimeoutError.
## The chronos leg is the control: `withTimeout` cancels `establish` structurally and
## its `except CancelledError` branch deliberately does not re-race, so it was, and
## must stay, at one.
import std/[os, strutils]
when defined(useChronos):
  import chronos
  import navi/backend/chronos as be
  const backend = "chronos"
  template napMs(ms: int): untyped = sleepAsync(ms.milliseconds)
else:
  import std/asyncdispatch
  import navi/backend/asyncdispatch as be
  const backend = "asyncdispatch"
  template napMs(ms: int): untyped = sleepAsync(ms)
import navi/backend/api                 # TlsConfig / ProxyTarget
import navi/backend/happyeyeballs       # resolveAddrs: the pool the backend will race
import navi/core/response as naviresp   # navi's TimeoutError

proc main() =
  # Locals rather than module globals: a chronos `{.async.}` proc must be gcsafe, and
  # touching global GC'ed memory from one is a compile error.
  let port = parseInt(getEnv("NAVI_ABANDON_PORT"))
  let pool = resolveAddrs("localhost", port)
  echo backend, ": pool for localhost:", port, " = ", pool
  doAssert pool.len >= 2,
    "this test needs a dual-homed localhost (an IPv4 and an IPv6 loopback address)"

  var timedOut = false
  var established = false
  var msg = ""

  proc run() {.async.} =
    try:
      let conn = await be.connect("localhost", port, true, TlsConfig(), ProxyTarget(),
                                  @["h2", "http/1.1"], connectMs = 600)
      established = true
      await be.close(conn)
    except naviresp.TimeoutError as e:
      timedOut = true
      msg = e.msg
    except CatchableError as e:
      echo "unexpected ", e.name, ": ", e.msg
    # Keep the loop turning after the caller has given up: an abandoned asyncdispatch
    # `establish` only makes progress (and, before the fix, only re-raced the other
    # address) while the dispatcher is polled. This is the window the script measures.
    await napMs(2500)

  waitFor run()

  doAssert not established, "a deaf peer cannot complete a TLS handshake"
  doAssert timedOut, "expected connectMs to trip navi's TimeoutError"
  echo "OK  ", backend, ": ", msg.split('\n')[0]

main()
