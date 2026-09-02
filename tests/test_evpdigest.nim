## EVP hardware-digest helper (#194). On Linux libcrypto loads and EVP is used;
## everywhere else `evpAvailable()` is false and callers fall back to `checksums`.
## This cross-checks EVP against `checksums` wherever EVP is live, so the two paths
## are proven identical (the digest-auth and ws-accept vectors cover the fallback).

import unittest
import std/strutils
import checksums/md5, checksums/sha2, checksums/sha1
import navi/backend/evpdigest

proc md5Ref(s: string): string = $toMD5(s)

proc sha256Ref(s: string): string =
  var h = initSha_256()
  h.update(s)
  $h.digest()

proc sha1RawRef(s: string): string =
  let d = Sha1Digest(secureHash(s))
  result = newString(d.len)
  for i in 0 ..< d.len: result[i] = char(d[i])

suite "EVP digests":
  test "EVP hashes should match checksums when EVP is available":
    if evpAvailable():
      for s in ["", "abc", "the quick brown fox", "navi:" & '\255'.repeat(40)]:
        check evpMd5Hex(s) == md5Ref(s)
        check evpSha256Hex(s) == sha256Ref(s)
        check evpSha1Raw(s) == sha1RawRef(s)
    else:
      # Non-Linux (or libcrypto unavailable): the digest-auth / ws-accept vectors
      # in test_digest / test_ws exercise the checksums fallback instead.
      check not evpAvailable()
