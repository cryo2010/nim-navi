## `clearTlsSecrets` must leave a client that still does mTLS (#438), on every
## native backend. Driven by tests/interop/mtls.sh, which stands up an
## `openssl s_server -Verify 1` that REQUIRES a client certificate, and built
## three ways (like mtls.nim):
##   nim c ...                -> navi (sync)
##   nim c -d:useAsync ...    -> navi/asyncdispatch
##   nim c -d:useChronos ...  -> navi/chronos
##
## The point of the test is the ORDER. Every request here is made AFTER the wipe,
## so the handshake can only succeed on a context that `clearTlsSecrets` built
## eagerly: navi normally builds one lazily per ALPN shape on first connect, and a
## context built from the wiped config would have no credential to install. A
## server that mandates a client certificate turns that into a hard failure rather
## than a silently anonymous handshake.
##
## Both ALPN shapes are covered, because each gets its own context: the default
## config offers @["h2", "http/1.1"], and `http = {H1}` offers none at all.
## `s_server` does not speak h2, so both end up on http/1.1 -- what differs is the
## context the connection was made from.
import std/os
when defined(useAsync):
  import std/asyncdispatch
  import navi/asyncdispatch
  const backend = "asyncdispatch"
  template waitFor0(e: untyped): untyped = waitFor e
elif defined(useChronos):
  import pkg/chronos
  import navi/chronos
  const backend = "chronos"
  template waitFor0(e: untyped): untyped = waitFor e
else:
  import navi
  const backend = "sync"
  template waitFor0(e: untyped): untyped = e

let
  url = getEnv("NAVI_MTLS_URL") & "/"
  caFile = getEnv("NAVI_MTLS_CA")
  certPem = readFile(getEnv("NAVI_MTLS_CERT"))
  keyPem = readFile(getEnv("NAVI_MTLS_KEY"))
  encKeyPem = readFile(getEnv("NAVI_MTLS_ENCKEY"))
  pass = getEnv("NAVI_MTLS_PASS")

var passed = 0

template case0(name: string, body: untyped) =
  # `block` so each case gets its own scope: a template body is not one, and at
  # top level every `let api` would otherwise collide.
  block:
    body
  inc passed
  echo "  [OK] ", backend, ": ", name

proc inMemoryCfg(onlyH1: bool, encrypted: bool): NaviConfig =
  result = initNaviConfig()
  result.tls.caFile = caFile
  result.tls.certPem = certPem
  result.tls.keyPem = if encrypted: encKeyPem else: keyPem
  if encrypted: result.tls.password = pass
  result.throwHttpErrors = false
  result.retry.limit = 0
  if onlyH1: result.http = {H1}

case0 "an in-memory credential survives the wipe (h2 ALPN shape)":
  let api = newNavi(inMemoryCfg(onlyH1 = false, encrypted = false))
  doAssert api.config.tls.keyPem.len > 0, "the client should hold the key until asked"
  api.clearTlsSecrets()
  doAssert api.config.tls.keyPem.len == 0, "keyPem should be wiped"
  doAssert api.config.tls.certPem.len == 0, "certPem should be wiped"
  # Only an eagerly built context can satisfy a server that mandates a client cert.
  let r = waitFor0 api.get(url)
  doAssert r.status == 200, backend & ": mTLS after the wipe failed: " & $r.status
  waitFor0 api.close()

case0 "an in-memory credential survives the wipe (no-ALPN shape, http = {H1})":
  let api = newNavi(inMemoryCfg(onlyH1 = true, encrypted = false))
  api.clearTlsSecrets()
  let r = waitFor0 api.get(url)
  doAssert r.status == 200, backend & ": mTLS after the wipe failed on {H1}: " & $r.status
  waitFor0 api.close()

case0 "an ENCRYPTED in-memory key survives the wipe, passphrase and all":
  # The passphrase is the field the issue is really about: it is only needed while
  # a context is being built, and after the wipe there is no way to build another.
  let api = newNavi(inMemoryCfg(onlyH1 = false, encrypted = true))
  doAssert api.config.tls.password.len > 0
  api.clearTlsSecrets()
  doAssert api.config.tls.password.len == 0, "the passphrase should be wiped"
  doAssert api.config.tls.keyPem.len == 0
  let r = waitFor0 api.get(url)
  doAssert r.status == 200,
    backend & ": mTLS with an encrypted key after the wipe failed: " & $r.status
  waitFor0 api.close()

case0 "two requests after the wipe both work (the context really is shared)":
  let api = newNavi(inMemoryCfg(onlyH1 = false, encrypted = true))
  api.clearTlsSecrets()
  for i in 1 .. 2:
    let r = waitFor0 api.get(url)
    doAssert r.status == 200, backend & ": request " & $i & " after the wipe failed"
  waitFor0 api.close()

case0 "a file-based credential is untouched by the wipe":
  # Paths are not secrets, so they stay; only the passphrase goes. The contexts are
  # built by then, so an encrypted key on disk still works without it.
  var cfg = initNaviConfig()
  cfg.tls.caFile = caFile
  cfg.tls.certFile = getEnv("NAVI_MTLS_CERT")
  cfg.tls.keyFile = getEnv("NAVI_MTLS_ENCKEY")
  cfg.tls.password = pass
  cfg.throwHttpErrors = false
  cfg.retry.limit = 0
  let api = newNavi(cfg)
  api.clearTlsSecrets()
  doAssert api.config.tls.password.len == 0, "the passphrase should be wiped"
  doAssert api.config.tls.certFile.len > 0, "certFile is a path, not a secret"
  doAssert api.config.tls.keyFile.len > 0, "keyFile is a path, not a secret"
  let r = waitFor0 api.get(url)
  doAssert r.status == 200,
    backend & ": mTLS from files after the wipe failed: " & $r.status
  waitFor0 api.close()

case0 "the wipe is a no-op for a client with no credential":
  var cfg = initNaviConfig()
  cfg.tls.caFile = caFile
  cfg.throwHttpErrors = false
  let api = newNavi(cfg)
  api.clearTlsSecrets()          # nothing to clear, and nothing to build
  # The server mandates a client certificate, so this must be REFUSED -- proof the
  # no-op really did nothing rather than quietly leaving a credential behind.
  var refused = false
  try:
    let r = waitFor0 api.get(url)
    refused = r.status != 200
  except CatchableError:
    refused = true
  doAssert refused, backend & ": an anonymous client should not be served"
  waitFor0 api.close()

case0 "a malformed credential raises and keeps the material":
  var cfg = initNaviConfig()
  cfg.tls.caFile = caFile
  cfg.tls.certPem = "-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n"
  cfg.tls.keyPem = keyPem
  let api = newNavi(cfg)
  var raised = false
  try: api.clearTlsSecrets()
  except ValueError: raised = true
  doAssert raised, backend & ": a malformed credential should raise here"
  doAssert api.config.tls.keyPem.len > 0, "the material must survive a failed build"
  doAssert api.config.tls.certPem.len > 0
  waitFor0 api.close()

echo "clearTlsSecrets interop [", backend, "]: ", passed, " passed, 0 failed"
