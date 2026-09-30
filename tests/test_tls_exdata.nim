## Allocating the session cache's `ex_data` index (#439).
##
## `sslExIndexClass` (tested in test_tls_session.nim) only decides which
## `CRYPTO_EX_INDEX` class the FALLBACK draws from. On the libraries whose class
## numbering differs -- LibreSSL, OpenSSL 1.0.x -- `SSL_get_ex_new_index` is a real
## export, so the fallback never runs and that constant is not what is under test
## there. These drive `newSslExIndex` itself against whatever library the suite
## links, which is the code path every TLS connection takes (resumption is on by
## default).
import unittest
import std/[openssl, dynlib]
import navi/backend/openssl_ctx

suite "allocating the ex_data index (#439)":
  # `sslExIndexClass` above only decides the FALLBACK's class. On the libraries
  # whose numbering differs -- LibreSSL, OpenSSL 1.0.x -- `SSL_get_ex_new_index` is
  # a real export, so the fallback never runs and the class constant is not what is
  # under test. These drive the allocator itself against whatever this host linked.
  test "an allocated index should be usable":
    # Negative is OpenSSL's failure return; 0 is a perfectly good index.
    check newSslExIndex() >= 0

  test "two allocations should not hand out the same index":
    let a = newSslExIndex()
    let b = newSslExIndex()
    check a >= 0
    check b >= 0
    check a != b

  test "the allocation should go through the library's own entry point":
    # Resolved by hand exactly as `newSslExIndex` does (a `dynlib` importc would
    # abort the process on a library that only has the macro). Where the symbol is
    # exported, navi must be drawing from the SSL class counter the library itself
    # uses, so our index has to fall in the same ascending sequence as a direct
    # call's -- which is what makes the index registered rather than merely unique.
    let lib = loadLibPattern(DLLSSLName)
    let sym = if lib.isNil: nil else: lib.symAddr("SSL_get_ex_new_index")
    if sym.isNil:
      # OpenSSL 1.1.0+ (a macro there), where the fallback's class is the tested
      # path and the suite above covers it. Still assert the allocator works.
      check newSslExIndex() >= 0
    else:
      type ExNewIndexProc = proc(argl: clong, argp: pointer,
        newf, dupf, freef: pointer): cint {.cdecl, gcsafe, raises: [].}
      let direct = cast[ExNewIndexProc](sym)
      let ours = newSslExIndex()
      let theirs = direct(0, nil, nil, nil, nil)
      check ours >= 0
      check theirs == ours + 1

