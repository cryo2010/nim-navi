## Unit tests for the OpenSSL session-cache plumbing in
## `navi/backend/openssl_ctx`: which CRYPTO_EX_INDEX class the ex_data slot is
## allocated from (#439).
##
## These exercise pure Nim decision logic over the OpenSSL library's *version*,
## not OpenSSL itself, so they run on any host: the libraries whose numbering
## differs (LibreSSL, OpenSSL 1.0.x) cannot be installed alongside the CI target,
## and the handshake-level behaviour is covered by the interop scripts instead.
import unittest
import std/openssl
import navi/backend/openssl_ctx

suite "ex_data class selection (#439)":
  test "LibreSSL should use the legacy SSL class (1), where BIO took 0":
    # LibreSSL pins OPENSSL_VERSION_NUMBER at 0x20000000 whatever its real
    # version, so every LibreSSL takes this branch. Measured on the macOS system
    # LibreSSL: CRYPTO_get_ex_new_index(0, ...) draws from the BIO counter and
    # returns an index that collides with SSL-class index 0.
    check sslExIndexClass(0x20000000'u) == 1

  test "OpenSSL 1.0.x should use the legacy SSL class (1)":
    check sslExIndexClass(0x10002000'u) == 1   # 1.0.2
    check sslExIndexClass(0x1000100f'u) == 1   # 1.0.1a
    check sslExIndexClass(0x0090700f'u) == 1   # 0.9.7

  test "OpenSSL 1.1.0 and later should use the modern SSL class (0)":
    check sslExIndexClass(0x10100000'u) == 0   # exactly 1.1.0, the renumbering
    check sslExIndexClass(0x1010107f'u) == 0   # 1.1.1g
    check sslExIndexClass(0x30000000'u) == 0   # 3.0
    check sslExIndexClass(0x30500000'u) == 0   # 3.5
    check sslExIndexClass(0x40000000'u) == 0   # a future major

  test "the boundary should fall exactly at 1.1.0":
    # The version just below 1.1.0 is legacy, 1.1.0 itself is modern.
    check sslExIndexClass(0x10100000'u - 1) == 1
    check sslExIndexClass(0x10100000'u) == 0

  test "the class should only ever be 0 or 1":
    # Guards against a future edit returning a raw class constant from the wrong
    # era's header (BIO is 12 on modern OpenSSL, for instance).
    for v in [0x0'u, 0x10000000'u, 0x10100000'u, 0x20000000'u, 0x30000000'u,
              0xffffffff'u]:
      check sslExIndexClass(v) in [0.cint, 1.cint]

  test "the loaded library's own class should match its version":
    # Whatever this host resolved, the selection must agree with the constant its
    # header would have used. Not a tautology: it pins the mapping for the library
    # the rest of the suite actually links against.
    let v = uint(getOpenSSLVersion())
    if v == 0x20000000'u or v < 0x10100000'u:
      check sslExIndexClass(v) == 1
    else:
      check sslExIndexClass(v) == 0

# --- the session cache's insert policy ---------------------------------------
#
# `offerSession` is the whole policy the OpenSSL new-session callback applies, as
# ordinary Nim: `onNewSession` only reads the slot out of the SSL's ex_data and
# turns the bool into OpenSSL's 1/0 (took ownership / did not). So the policy is
# testable with real SSL_SESSION objects and no handshake, which is what these do.
# A declined session stays OURS here (OpenSSL would have freed it), so every test
# frees what it offered.

proc SSL_SESSION_new(): pointer {.cdecl, dynlib: DLLSSLName, importc.}
proc SSL_SESSION_free(session: pointer) {.cdecl, dynlib: DLLSSLName, importc.}

proc session(): pointer =
  result = SSL_SESSION_new()
  doAssert not result.isNil, "SSL_SESSION_new failed"

suite "closed session cache (#441)":
  test "an open cache should take ownership of an offered session":
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")
    check slot.offerSession(session())
    check cache.sessionCount == 1
    check cache.hasSession("example.com:443")
    cache.close()

  test "a second session for the same origin should replace the first":
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")
    check slot.offerSession(session())
    check slot.offerSession(session())     # frees the first, keeps one entry
    check cache.sessionCount == 1
    cache.close()

  test "close should empty the cache and mark it closed":
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")
    check slot.offerSession(session())
    check not cache.isClosed
    cache.close()
    check cache.isClosed
    check cache.sessionCount == 0

  test "a ticket arriving after close should be declined, not cached":
    # The #441 scenario: the slot belongs to a connection checked out rather than
    # pooled (a live WebSocket or SSE stream), so it survives client.close() and
    # the server can still send it a TLS 1.3 NewSessionTicket. Taking ownership
    # there leaks the SSL_SESSION, since nothing will free the table again.
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")   # minted while the cache was open
    cache.close()
    let s = session()
    check not slot.offerSession(s)                # ownership stays with OpenSSL
    check cache.sessionCount == 0
    check not cache.hasSession("example.com:443")
    SSL_SESSION_free(s)                           # we still own it in this test

  test "a closed cache should not be reopened by a later insert":
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")
    cache.close()
    var offered: seq[pointer]
    for _ in 1 .. 5:
      let s = session()
      offered.add s
      check not slot.offerSession(s)
    check cache.isClosed        # still closed
    check cache.sessionCount == 0
    for s in offered: SSL_SESSION_free(s)

  test "every slot on a closed cache should decline, not just the one that closed it":
    let cache = newTlsSessionCache()
    let a = cache.newSlot("a.example:443")
    let b = cache.newSlot("b.example:443")
    check a.offerSession(session())
    cache.close()
    let s = session()
    check not b.offerSession(s)
    check cache.sessionCount == 0
    SSL_SESSION_free(s)

  test "close should be idempotent":
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")
    check slot.offerSession(session())
    cache.close()
    cache.close()                # must not double-free the entry it already freed
    check cache.sessionCount == 0
    check cache.isClosed

  test "a nil session or a nil slot should be declined":
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")
    check not slot.offerSession(nil)
    check cache.sessionCount == 0
    let s = session()
    check not SessionSlot(nil).offerSession(s)   # no slot on the SSL's ex_data
    SSL_SESSION_free(s)
    cache.close()
