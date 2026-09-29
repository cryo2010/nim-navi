## A TLS close that carries no close_notify must not pass for the end of a body
## that is delimited by the close itself (issue #426). Driven by
## tests/interop/tls_truncate.sh, which stands up truncate_tls_server.py (a TLS
## server that ends a response cleanly, with a RST, or with a bare FIN) plus an
## `openssl s_server -www` origin, and exports NAVI_TRUNC_URL / NAVI_TRUNC_CA /
## NAVI_TRUNC_SSRV. Built three ways, like host_verify.nim:
##   nim c ...                -> navi (sync)
##   nim c -d:useAsync ...    -> navi/asyncdispatch
##   nim c -d:useChronos ...  -> navi/chronos
## Before the fix every backend reported the unauthenticated close as a clean EOF,
## so /rst and /fin came back as a complete 200 with a silently truncated body.
import std/[os, strutils]
when defined(useAsync):
  import navi/asyncdispatch
  const backend = "asyncdispatch"
elif defined(useChronos):
  import navi/chronos
  const backend = "chronos"
else:
  import navi
  template await(x: untyped): untyped = x
  const backend = "sync"

const fullBody = "the body the server meant to send in full"

proc truncCfg(): NaviConfig =
  ## Trusts the test server's certificate; no retries, so a rejected response is
  ## reported once and is not replayed onto a second connection.
  result = initNaviConfig()
  result.tls.caFile = getEnv("NAVI_TRUNC_CA")
  result.throwHttpErrors = false
  result.retry.limit = 0

template runAll() =
  var passed = 0
  var failures: seq[string]
  let url = getEnv("NAVI_TRUNC_URL")      # https://127.0.0.1:port
  let ssrv = getEnv("NAVI_TRUNC_SSRV")    # openssl s_server -www, same CA

  template check(name: string, cond: untyped) =
    block:
      try:
        if cond: inc passed
        else: (failures.add name; echo "FAIL ", name)
      except CatchableError as e:
        failures.add name & " [" & e.msg & "]"
        echo "FAIL ", name, "  (", e.msg, ")"

  template failure(body: untyped): string =
    ## The message the request fails with, or "" when it succeeds.
    var msg = ""
    try: discard body
    except CatchableError as e: msg = e.msg
    msg

  template streamed(target: string): string =
    ## The body pulled chunk by chunk, which is the other read-until-close path
    ## (the streamed/SSE reader, not the buffered drain).
    var acc = ""
    let handle = await newNavi(truncCfg()).stream.get(target)
    handle.each(chunk):
      acc.add chunk
    acc

  # The control: the same un-delimited body, ended with a close_notify. It is the
  # peer saying "that was all of it", so the body must be delivered as before.
  check "a close_notify still completes a body delimited by the close":
    let r = await newNavi(truncCfg()).get(url & "/clean")
    r.status == 200 and r.body == fullBody

  check "a close_notify still completes a streamed body delimited by the close":
    streamed(url & "/clean") == fullBody

  # The attack: the connection is reset mid-body. Nothing frames this body, so
  # without the alert there is no way to tell a finished response from a cut one.
  check "a reset without close_notify is rejected, not returned as a short body":
    "close_notify" in failure(await newNavi(truncCfg()).get(url & "/rst"))

  check "a reset without close_notify is rejected on the streamed path too":
    "close_notify" in failure(streamed(url & "/rst"))

  # A bare FIN is the same thing on the backends that do not already fail it in
  # OpenSSL (OpenSSL 3 raises "unexpected eof" for the fd-based backends), so the
  # message is not pinned here: what matters is that no truncated body is returned.
  check "a bare FIN without close_notify is rejected":
    failure(await newNavi(truncCfg()).get(url & "/fin")).len > 0

  # A framed body keeps its own check: the parser sees the length is unmet and
  # reports a truncation, and the close_notify rule never enters into it.
  check "a Content-Length body cut short still reports the parser's truncation":
    let msg = failure(await newNavi(truncCfg()).get(url & "/cl-short"))
    "truncated" in msg and "close_notify" notin msg

  # A delimited keep-alive exchange is untouched: two requests, one connection,
  # and no close in sight for the new rule to judge.
  check "delimited keep-alive responses are unaffected":
    let client = newNavi(truncCfg())
    let a = await client.get(url & "/keepalive")
    let b = await client.get(url & "/keepalive")
    a.status == 200 and b.status == 200 and a.body == "ok" and b.body == "ok"

  # The real-world until-close body: `openssl s_server -www` serves its status
  # page with no Content-Length and closes after each reply. It does send
  # close_notify, so the page must keep arriving, twice in a row.
  check "an openssl s_server page (until-close, closed cleanly) still arrives":
    let client = newNavi(truncCfg())
    let a = await client.get(ssrv & "/")
    let b = await client.get(ssrv & "/")
    a.status == 200 and a.body.len > 0 and b.status == 200 and b.body.len > 0

  echo "unclean TLS close [", backend, "]: ", passed, " passed, ",
       failures.len, " failed"
  if failures.len > 0:
    for f in failures: echo "  - ", f
    quit(1)

when defined(useAsync) or defined(useChronos):
  proc main() {.async.} = runAll()
  waitFor main()
else:
  runAll()
