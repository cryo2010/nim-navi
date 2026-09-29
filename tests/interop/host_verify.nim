## Certificate *identity* verification (the name, not the chain) on the native
## OpenSSL backends. Driven by tests/interop/ca_verify.sh, which signs two server
## certificates with the test CA -- one that legitimately covers 127.0.0.1, and
## one whose subject CN is `localhost` while its only dNSName SAN is something
## else -- and exports NAVI_HOSTV_CA / NAVI_HOSTV_GOOD / NAVI_HOSTV_MISMATCH.
## Built three ways, like mtls.nim:
##   nim c ...                -> navi (sync)
##   nim c -d:useAsync ...    -> navi/asyncdispatch
##   nim c -d:useChronos ...  -> navi/chronos
## The mismatch cases are the regression guard for the CN-over-SAN fallback: with
## X509_CHECK_FLAG_ALWAYS_CHECK_SUBJECT a certificate whose SANs all mismatch was
## accepted whenever its CN happened to match the target.
import std/os
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

proc hostvCfg(): NaviConfig =
  ## Trusts the test CA (so only the identity check can reject), verify on.
  result = initNaviConfig()
  result.tls.caFile = getEnv("NAVI_HOSTV_CA")
  result.throwHttpErrors = false
  result.retry.limit = 0

template runAll() =
  var passed = 0
  var failures: seq[string]
  let good = getEnv("NAVI_HOSTV_GOOD")           # https://127.0.0.1:port
  let mismatch = getEnv("NAVI_HOSTV_MISMATCH")   # https://localhost:port

  template check(name: string, cond: untyped) =
    block:
      try:
        if cond: inc passed
        else: (failures.add name; echo "FAIL ", name)
      except CatchableError as e:
        failures.add name & " [" & e.msg & "]"
        echo "FAIL ", name, "  (", e.msg, ")"

  template rejects(body: untyped): bool =
    ## True when the request fails: the handshake must abort on the identity
    ## check rather than return a response.
    var raised = false
    try: discard body
    except CatchableError: raised = true
    raised

  # An IP-literal target whose certificate carries the matching iPAddress SAN
  # still verifies: the pre-handshake binding uses X509_VERIFY_PARAM_set1_ip_asc
  # for IP literals, not SSL_set1_host.
  check "IP literal with a matching IP SAN verifies":
    (await newNavi(hostvCfg()).get(good & "/")).status == 200

  # Same chain-valid CA, but the certificate's only dNSName SAN is not the host
  # we asked for; its subject CN is. RFC 9525: the CN must not rescue it.
  check "CN matching while every SAN mismatches is rejected":
    rejects(await newNavi(hostvCfg()).get(mismatch & "/"))

  # The mismatch certificate carries no iPAddress SAN at all, so reaching it by
  # IP literal must fail too (the checkCertIp / set1_ip_asc leg).
  check "IP literal against a certificate without an IP SAN is rejected":
    let byIp = getEnv("NAVI_HOSTV_MISMATCH_IP")   # https://127.0.0.1:mismatchPort
    rejects(await newNavi(hostvCfg()).get(byIp & "/"))

  echo "certificate identity [", backend, "]: ", passed, " passed, ",
       failures.len, " failed"
  if failures.len > 0:
    for f in failures: echo "  - ", f
    quit(1)

when defined(useAsync) or defined(useChronos):
  proc main() {.async.} = runAll()
  waitFor main()
else:
  runAll()
