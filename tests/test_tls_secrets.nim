## In-memory TLS key material must not outlive its use (#438).
##
## `TlsConfig.password`, `keyPem` and `certPem` are ordinary Nim strings that
## `newNavi` copies into `client.config` and keeps for the client's lifetime,
## because the SSL_CTXs are built lazily, one per ALPN shape on first connect.
## After `loadClientCert` handed them to OpenSSL nothing zeroed them, so a heap
## dump or a core file from a long-running process read the passphrase and the PEM
## private key in cleartext -- at several addresses, since the transient buffers
## the loaders allocate (key-file contents, the PKCS#12 bytes) were freed
## uncleansed too.
##
## These cover the parts that are observable without a server: that `cleanse`
## really overwrites the bytes rather than only shortening the string, that the
## config-level and client-level wipes clear exactly the secret fields, that the
## client-level wipe builds every context first (so the client still works), and
## that the caller's own copy is untouched -- which is the trade-off, not an
## oversight: `newNavi` takes the config by value and can never reach it.
##
## The credential below is a throwaway self-signed EC pair generated for this
## file. Nothing verifies it; it only has to be a certificate and a key OpenSSL
## accepts and considers matched, so that a context can actually be built.
import unittest
from std/strutils import repeat
import navi
import navi/backend/openssl_ctx

const
  testCertPem = """-----BEGIN CERTIFICATE-----
MIIBlDCCATugAwIBAgIUUfaOJWj0dOQVMX+31cr6n88ogWcwCgYIKoZIzj0EAwIw
HzEdMBsGA1UEAwwUbmF2aS10ZXN0LWNsaWVudC00MzgwIBcNMjYwOTMwMTcyMjQ0
WhgPMjEyNjA5MDYxNzIyNDRaMB8xHTAbBgNVBAMMFG5hdmktdGVzdC1jbGllbnQt
NDM4MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEZhfX7SjnDSi/+zvdWNjBqUyr
VGPAam6UkjyPqKwAGfdRb3/JCnY5nkrE7zj+cxDWNIwKzs+JOkeeWXRT2N6PVqNT
MFEwHQYDVR0OBBYEFBETK7qPw90W7mDV+k9vW+O0hVusMB8GA1UdIwQYMBaAFBET
K7qPw90W7mDV+k9vW+O0hVusMA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZIzj0EAwID
RwAwRAIgGd5RV1sV1g2hXZAOJoCJPAd3T7ZnSxTB80seIoHI5tACIFawDXwmch2k
fOjDBu4pr35Jm9ZrAg1Vt6H6qVPH9CXG
-----END CERTIFICATE-----
"""
  testKeyPem = """-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg5KhXuXHEL9FdV2Me
qtKMdnrP4y/6whwSbkrSqXI8PfuhRANCAARmF9ftKOcNKL/7O91Y2MGpTKtUY8Bq
bpSSPI+orAAZ91Fvf8kKdjmeSsTvOP5zENY0jArOz4k6R55ZdFPY3o9W
-----END PRIVATE KEY-----
"""

proc heapString(s: string): string =
  ## `s` in a payload of our own. A string built straight from a literal shares
  ## the literal's immutable payload, which is not what a passphrase read from a
  ## file or an environment variable looks like (and `addr s[0]` on one would
  ## point into read-only memory).
  result = ""
  result.add s

proc bytesStillThere(p: ptr UncheckedArray[byte], n: int): int =
  ## How many of the first `n` bytes at `p` are non-zero. `cleanse` ends with
  ## `setLen 0`, which does NOT free the payload, so the buffer it just wrote over
  ## is still ours to read -- which is exactly the read a heap dump performs.
  for i in 0 ..< n:
    if p[i] != 0: inc result

suite "cleanse (#438)":
  test "cleanse should overwrite the bytes, not just shorten the string":
    var s = heapString("correct horse battery staple")
    let n = s.len
    let p = cast[ptr UncheckedArray[byte]](addr s[0])
    check bytesStillThere(p, n) == n        # all of it is there to begin with
    cleanse(s)
    check s.len == 0
    check bytesStillThere(p, n) == 0        # and none of it is, afterwards

  test "setLen alone should leave the secret readable (what cleanse fixes)":
    # The counter-example, so the test above is not just asserting that setLen
    # works: shortening a string leaves every byte in place.
    var s = heapString("correct horse battery staple")
    let n = s.len
    let p = cast[ptr UncheckedArray[byte]](addr s[0])
    s.setLen(0)
    check s.len == 0
    # `>= n - 1`, not `== n`: under refc `setLen` writes a terminating NUL at
    # index 0, so one byte of the secret goes by accident. Every other byte, and
    # under arc/orc every byte, is still sitting there to be read.
    check bytesStillThere(p, n) >= n - 1

  test "cleanse should accept an empty string and a literal-backed one":
    var empty = ""
    cleanse(empty)
    check empty.len == 0
    var lit = "a literal, whose payload is shared and immutable"
    cleanse(lit)                            # must not fault on the shared payload
    check lit.len == 0

  test "a literal-backed secret should stay in the image (documented limit)":
    # The limitation `cleanse`'s doc, the README and HARDENING.md all name: under
    # arc/orc a string built from a literal, a `const` or `staticRead` shares the
    # binary's read-only payload, so `prepareMutation` hands the wipe a PRIVATE
    # COPY and the original is still there to be read for the life of the process.
    # Pinned here so the limitation cannot quietly change into something the docs
    # no longer describe: the answer is to read secrets at run time, not to expect
    # this to start working.
    var lit = "a compiled-in passphrase"
    let n = lit.len
    let p = cast[ptr UncheckedArray[byte]](addr lit[0])
    cleanse(lit)
    check lit.len == 0                      # emptied on every memory model
    when defined(gcArc) or defined(gcOrc):
      check bytesStillThere(p, n) == n      # ... but the shared payload is intact
    else:
      # --mm:refc copies a literal into the heap at assignment, so there the wipe
      # does reach every byte the variable owns. The static payload behind it is
      # just as unreachable either way.
      check bytesStillThere(p, n) == 0

  test "cleanse should zero a long secret, not just the head of it":
    # The wipe is a byte loop through a volatile pointer (#438): the volatility is
    # what stops an optimising build from dropping stores into a buffer it can see
    # being freed or shortened straight afterwards. A length past any vector width
    # and not a multiple of one, so a partial-tail bug would show.
    var s = heapString(repeat("k", 4095))
    let n = s.len
    let p = cast[ptr UncheckedArray[byte]](addr s[0])
    check bytesStillThere(p, n) == n
    cleanse(s)
    check bytesStillThere(p, n) == 0

suite "clearTlsSecrets on a TlsConfig (#438)":
  test "it should clear password, keyPem and certPem and nothing else":
    var tls = defaultTls()
    tls.password = heapString("s3cret")
    tls.keyPem = heapString(testKeyPem)
    tls.certPem = heapString(testCertPem)
    tls.certFile = "client.pem"
    tls.keyFile = "client.key"
    tls.caFile = "ca.pem"
    tls.pinnedKeys = @["AAAA"]
    tls.clearTlsSecrets()
    check tls.password.len == 0
    check tls.keyPem.len == 0
    check tls.certPem.len == 0
    # Not secrets, and the config must stay usable for a file-based credential.
    check tls.certFile == "client.pem"
    check tls.keyFile == "client.key"
    check tls.caFile == "ca.pem"
    check tls.pinnedKeys == @["AAAA"]
    check tls.resumeSessions                # defaultTls's own setting survives

  test "it should be idempotent on a config with nothing to clear":
    var tls = defaultTls()
    tls.clearTlsSecrets()
    tls.clearTlsSecrets()
    check tls.password.len == 0
    check tls.keyPem.len == 0

  test "it should actually zero the password, not just drop it":
    var tls = defaultTls()
    tls.password = heapString("passphrase-in-the-heap")
    let n = tls.password.len
    let p = cast[ptr UncheckedArray[byte]](addr tls.password[0])
    tls.clearTlsSecrets()
    check bytesStillThere(p, n) == 0

suite "clearTlsSecrets on a client (#438)":
  proc mtlsConfig(): NaviConfig =
    result = initNaviConfig()
    result.tls.certPem = heapString(testCertPem)
    result.tls.keyPem = heapString(testKeyPem)
    result.tls.password = heapString("unused-for-an-unencrypted-key")

  test "it should wipe the client's copy of the credential":
    let api = newNavi(mtlsConfig())
    defer: api.close()
    check api.config.tls.keyPem.len > 0     # the client holds it until asked
    api.clearTlsSecrets()
    check api.config.tls.keyPem.len == 0
    check api.config.tls.certPem.len == 0
    check api.config.tls.password.len == 0

  test "it should zero the bytes of the client's copy":
    let api = newNavi(mtlsConfig())
    defer: api.close()
    let n = api.config.tls.keyPem.len
    let p = cast[ptr UncheckedArray[byte]](addr api.config.tls.keyPem[0])
    api.clearTlsSecrets()
    check bytesStillThere(p, n) == 0

  test "it should build every ALPN shape's context before wiping":
    # This is what keeps the client working: contexts are built lazily, one per
    # ALPN shape on first connect, and a context built after the wipe would have
    # no credential to install. Both shapes navi ever offers must exist by the
    # time the material goes.
    let api = newNavi(mtlsConfig())
    defer: api.close()
    let store = cast[TlsContextStore](api.config.tls.contextStore)
    check store.contextCount == 0            # nothing dialled yet
    api.clearTlsSecrets()
    check store.contextCount == naviAlpnShapes.len
    check naviAlpnShapes.len == 2            # @[] and @["h2", "http/1.1"]

  test "it should leave the caller's own config untouched":
    # The trade-off #438 names: newNavi copies the config BY VALUE, so navi has no
    # way to reach the copy the caller kept. Callers clear that one themselves.
    var cfg = mtlsConfig()
    let api = newNavi(cfg)
    defer: api.close()
    api.clearTlsSecrets()
    check api.config.tls.keyPem.len == 0
    check cfg.tls.keyPem.len > 0             # still the caller's problem
    cfg.tls.clearTlsSecrets()                # and this is how they solve it
    check cfg.tls.keyPem.len == 0

  test "it should be a no-op, not a context build, with no credential":
    # The overwhelmingly common client has no client certificate at all: it must
    # not pay two context builds for a wipe with nothing to wipe.
    let api = newNavi(initNaviConfig())
    defer: api.close()
    api.clearTlsSecrets()
    let store = cast[TlsContextStore](api.config.tls.contextStore)
    check store.contextCount == 0

  test "it should keep the credential when the context build fails":
    # A malformed credential raises here rather than on the first connect. The
    # material must survive that, or a caller who fixes the config and retries
    # would have nothing left to build from.
    var cfg = initNaviConfig()
    cfg.tls.certPem = heapString("-----BEGIN CERTIFICATE-----\nnot base64\n" &
                                 "-----END CERTIFICATE-----\n")
    cfg.tls.keyPem = heapString(testKeyPem)
    let api = newNavi(cfg)
    defer: api.close()
    expect ValueError:
      api.clearTlsSecrets()
    check api.config.tls.keyPem.len > 0
    check api.config.tls.certPem.len > 0

  test "a wiped client should still be able to build a context":
    # The proof that the eager build is enough: after the wipe, asking for either
    # ALPN shape again returns the cached context rather than trying to install a
    # credential that is no longer there.
    let api = newNavi(mtlsConfig())
    defer: api.close()
    api.clearTlsSecrets()
    for alpn in naviAlpnShapes:
      let (ctx, owned) = obtainContext(api.config.tls.contextStore,
                                       api.config.tls, alpn)
      check not ctx.isNil
      check not owned                        # the store owns it, not us
    check cast[TlsContextStore](api.config.tls.contextStore).contextCount == 2
