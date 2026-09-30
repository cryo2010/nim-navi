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

# --- rejection evicts the origin's session -----------------------------------
#
# For TLS <= 1.2 the new-session callback fires INSIDE the handshake, before
# navi's post-handshake hostname/IP, SPKI-pin and verify-callback checks can run,
# so a session from a peer navi then refuses was already cached. Under TLS 1.3 the
# ticket instead arrives on the first reads, i.e. it can land AFTER the rejection.
# `rejectSession` covers both: it evicts the stored entry and marks the slot so
# `offerSession` declines anything later (#440).
#
# The `verifyPeer` / `postHandshakeVerify` tests drive the real entry points
# against a memory-BIO SSL that never handshook, so it has no peer certificate and
# every check fails the way a rejected peer's would. That exercises the wiring
# (the slot reaching `rejectSession` through the `except` path), not a stub.

import navi/backend/api

var sharedCtx: SslContext
  ## One SSL_CTX for every `memSsl` below. A fresh context per call leaked one per
  ## test: `SSL_free` releases the SSL, not the context behind it, and a context
  ## with the system trust store loaded is a few hundred KB (#438).

proc memSsl(slot: SessionSlot): SslPtr =
  ## An unhandshaken client SSL on memory BIOs, with `slot` linked into its
  ## ex_data by `applySession` exactly as a real connect does. The caller frees
  ## the SSL (which frees its two BIOs); the context is shared and outlives it.
  if sharedCtx.isNil: sharedCtx = newTlsContext(defaultTls())
  newClientSslMem(sharedCtx, "example.com", verify = true, slot = slot).ssl

suite "post-handshake rejection drops the session (#440)":
  test "rejectSession should evict the origin's entry and mark the slot":
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")
    check slot.offerSession(session())
    check cache.sessionCount == 1
    check not slot.isRejected
    slot.rejectSession()
    check slot.isRejected
    check cache.sessionCount == 0
    check not cache.hasSession("example.com:443")
    cache.close()

  test "a TLS 1.3 ticket arriving after the rejection should be declined":
    # The ticket lands during the first reads, after the pin check already raised,
    # so evicting alone would let it re-populate the entry we just dropped.
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")
    slot.rejectSession()
    let s = session()
    check not slot.offerSession(s)      # ownership stays with OpenSSL
    check cache.sessionCount == 0
    SSL_SESSION_free(s)                 # we still own it in this test
    cache.close()

  test "a rejection should drop only the rejected origin's session":
    let cache = newTlsSessionCache()
    let bad = cache.newSlot("bad.example:443")
    let good = cache.newSlot("good.example:443")
    check bad.offerSession(session())
    check good.offerSession(session())
    bad.rejectSession()
    check not cache.hasSession("bad.example:443")
    check cache.hasSession("good.example:443")
    check cache.sessionCount == 1
    check not good.isRejected
    check good.offerSession(session())  # the healthy origin still caches
    check cache.sessionCount == 1
    cache.close()

  test "rejectSession should be idempotent and fine with nothing cached":
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")
    slot.rejectSession()                # no entry to evict
    slot.rejectSession()                # must not double-free anything
    check slot.isRejected
    check cache.sessionCount == 0
    cache.close()

  test "rejectSession on a closed cache or a nil slot should be a no-op":
    # Call sites pass whatever slot the connection has, which is nil when
    # resumption is off, and the cache may already be closed under them.
    rejectSession(nil)
    check not SessionSlot(nil).isRejected
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")
    check slot.offerSession(session())
    cache.close()
    slot.rejectSession()
    check slot.isRejected
    check cache.sessionCount == 0

  test "applySession should clear the mark so a re-raced address caches again":
    # sync's connectAcross reuses ONE slot across the addresses it re-races, so a
    # first address that failed verification must not stop the address that works
    # from caching its session.
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")
    slot.rejectSession()
    check slot.isRejected
    let ssl = memSsl(slot)              # applySession runs inside newClientSslMem
    check not slot.isRejected
    check slot.offerSession(session())
    check cache.sessionCount == 1
    SSL_free(ssl)
    cache.close()

  test "verifyPeer should evict the session when the identity check fails":
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")
    let ssl = memSsl(slot)
    check slot.offerSession(session())  # what TLS <=1.2 cached mid-handshake
    expect ValueError:
      verifyPeer(ssl, "example.com", verify = true, slot = slot)
    check slot.isRejected
    check cache.sessionCount == 0
    SSL_free(ssl)
    cache.close()

  test "verifyPeer with verification off should leave the cache alone":
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")
    let ssl = memSsl(slot)
    check slot.offerSession(session())
    verifyPeer(ssl, "example.com", verify = false, slot = slot)
    check not slot.isRejected
    check cache.sessionCount == 1
    SSL_free(ssl)
    cache.close()

  test "postHandshakeVerify should evict the session when the pin check fails":
    # The #440 failure scenario: an interception proxy with a chain-valid but
    # unpinned certificate over TLS 1.2. Its session was cached during the
    # handshake and would be re-offered to whoever answers next.
    var cfg = defaultTls()
    cfg.pinnedKeys = @["ZmFrZSBwaW4gdGhhdCBtYXRjaGVzIG5vdGhpbmc="]
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")
    let ssl = memSsl(slot)
    check slot.offerSession(session())
    expect ValueError:
      postHandshakeVerify(ssl, "example.com", cfg, slot)
    check slot.isRejected
    check cache.sessionCount == 0
    SSL_free(ssl)
    cache.close()

  test "postHandshakeVerify should evict the session when the callback rejects":
    var cfg = defaultTls()
    cfg.verifyCallback = proc(leafDer: string): bool {.gcsafe, raises: [CatchableError].} =
      false
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")
    let ssl = memSsl(slot)
    check slot.offerSession(session())
    expect ValueError:
      postHandshakeVerify(ssl, "example.com", cfg, slot)
    check slot.isRejected
    check cache.sessionCount == 0
    SSL_free(ssl)
    cache.close()

  test "postHandshakeVerify with no pins and no callback should accept and cache":
    # The overwhelmingly common config must not go anywhere near the eviction path.
    let cache = newTlsSessionCache()
    let slot = cache.newSlot("example.com:443")
    let ssl = memSsl(slot)
    postHandshakeVerify(ssl, "example.com", defaultTls(), slot)
    check not slot.isRejected
    check slot.offerSession(session())
    check cache.sessionCount == 1
    SSL_free(ssl)
    cache.close()
