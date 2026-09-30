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
