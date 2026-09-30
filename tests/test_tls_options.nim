## Which libraries may be handed an SSL_OP_* bit (#444).
##
## navi sets `SSL_OP_NO_RENEGOTIATION` on every TLS context it builds, and that
## option NUMBER (0x40000000) is OpenSSL 1.1.0's. The option-bit space is not
## shared across the libraries navi can load: OpenSSL 1.0.x spends 0x40000000 on
## `SSL_OP_NETSCAPE_DEMO_CIPHER_CHANGE_BUG`, and LibreSSL spends it on
## `SSL_OP_NO_DTLSv1` while numbering its own `SSL_OP_NO_RENEGOTIATION` 0x00040000
## (which is `SSL_OP_ALLOW_UNSAFE_LEGACY_RENEGOTIATION` on OpenSSL). Setting the
## bit on one of those libraries therefore does not refuse renegotiation, it flips
## an unrelated flag, so `addCtxOptions` must recognise which family it is talking
## to before it sets anything.
##
## The decision is pure logic over `OPENSSL_VERSION_NUMBER`, so it is testable for
## the libraries this host cannot install; whether the bit really lands on the host's
## own library is covered by the interop scripts.
import unittest
import std/openssl
import navi/backend/openssl_ctx

suite "the SSL_OP option-mask gate (#444)":
  test "LibreSSL should be excluded whatever version it claims":
    # LibreSSL pins OPENSSL_VERSION_NUMBER at 0x20000000, which sorts ABOVE
    # 1.1.0 and must not be taken for it. (It also keeps SSL_CTX_set_options as a
    # macro, so the symbol lookup declines it a second time.)
    check not isOpenSsl11OrNewer(0x20000000'u)

  test "OpenSSL 1.0.x and older should be excluded":
    check not isOpenSsl11OrNewer(0x10002000'u)        # 1.0.2
    check not isOpenSsl11OrNewer(0x1000100f'u)        # 1.0.1a
    check not isOpenSsl11OrNewer(0x0090700f'u)        # 0.9.7

  test "OpenSSL 1.1.0 and newer should be included":
    check isOpenSsl11OrNewer(0x10100000'u)            # exactly 1.1.0
    check isOpenSsl11OrNewer(0x1010107f'u)            # 1.1.1g
    check isOpenSsl11OrNewer(0x30000000'u)            # 3.0
    check isOpenSsl11OrNewer(0x30500000'u)            # 3.5

  test "the boundary should fall exactly at 1.1.0":
    check not isOpenSsl11OrNewer(0x10100000'u - 1)
    check isOpenSsl11OrNewer(0x10100000'u)

  test "the gate should agree with the library the suite links":
    # Not a tautology: it pins the answer for this host, and it is the assertion
    # that fails if the version probe itself stops working.
    let v = uint(getOpenSSLVersion())
    if v == 0x20000000'u or v < 0x10100000'u:
      check not isOpenSsl11OrNewer(v)
    else:
      check isOpenSsl11OrNewer(v)
