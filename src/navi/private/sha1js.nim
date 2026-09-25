## Pure-Nim SHA-1 (FIPS 180-4), for the JavaScript target.
##
## `checksums/sha1` reaches `std/endians`, which is native-only (it calls
## `copyMem`), so a `nim js` build cannot use it. navi needs SHA-1 in exactly one
## place, the Sec-WebSocket-Accept hash of the opening handshake (RFC 6455 1.3):
## one hash per connection over a ~60-byte input, so this plain uint32 transform
## is more than fast enough. Native builds keep using OpenSSL EVP or `checksums`.
##
## Only the `uint32` and `uint64` operations Nim compiles identically on both
## targets are used (add, xor, and, or, not, shifts), so the digest is the same
## on js as it is natively; `tests/test_ws.nim` cross-checks it against
## `checksums/sha1` on the native build.

proc rotl(x: uint32, n: int): uint32 {.inline.} =
  (x shl n) or (x shr (32 - n))

proc sha1Raw*(msg: string): string =
  ## The raw 20-byte SHA-1 digest of `msg` (bytes, not hex).
  var h = [0x67452301'u32, 0xEFCDAB89'u32, 0x98BADCFE'u32,
           0x10325476'u32, 0xC3D2E1F0'u32]
  # FIPS 180-4 5.1.1 padding: a 0x80 byte, zeros up to 56 mod 64, then the
  # message length in bits as a 64-bit big-endian integer.
  var data = msg
  data.add '\x80'
  while data.len mod 64 != 56: data.add '\0'
  let bits = uint64(msg.len) * 8'u64
  for shift in countdown(56, 0, 8):
    data.add char(uint8((bits shr uint64(shift)) and 0xFF'u64))
  var w: array[80, uint32]
  var pos = 0
  while pos < data.len:
    for i in 0 ..< 16:
      let o = pos + i * 4
      w[i] = (uint32(uint8(data[o])) shl 24) or
             (uint32(uint8(data[o + 1])) shl 16) or
             (uint32(uint8(data[o + 2])) shl 8) or
              uint32(uint8(data[o + 3]))
    for i in 16 ..< 80:
      w[i] = rotl(w[i - 3] xor w[i - 8] xor w[i - 14] xor w[i - 16], 1)
    var a = h[0]
    var b = h[1]
    var c = h[2]
    var d = h[3]
    var e = h[4]
    for i in 0 ..< 80:
      var f, k: uint32
      if i < 20:
        f = (b and c) or ((not b) and d)
        k = 0x5A827999'u32
      elif i < 40:
        f = b xor c xor d
        k = 0x6ED9EBA1'u32
      elif i < 60:
        f = (b and c) or (b and d) or (c and d)
        k = 0x8F1BBCDC'u32
      else:
        f = b xor c xor d
        k = 0xCA62C1D6'u32
      let t = rotl(a, 5) + f + e + k + w[i]
      e = d
      d = c
      c = rotl(b, 30)
      b = a
      a = t
    h[0] = h[0] + a
    h[1] = h[1] + b
    h[2] = h[2] + c
    h[3] = h[3] + d
    h[4] = h[4] + e
    pos += 64
  result = newString(20)
  for i in 0 ..< 5:
    for j in 0 ..< 4:
      result[i * 4 + j] = char(uint8((h[i] shr (24 - j * 8)) and 0xFF'u32))
