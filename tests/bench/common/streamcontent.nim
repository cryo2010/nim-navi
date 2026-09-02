## Fixed-block content + SHA-1 for the streaming workloads. One reusable 1 MiB block
## is streamed `size/blockSize` times (+ remainder), hashed incrementally as it flies,
## so both sides stay constant-memory. Each side hashes only its own bytes, so the
## client's block and the server's block need not match -- only each side's
## bytes-and-hash must agree end to end.
##
## The hash goes through OpenSSL's EVP SHA-1 (SHA-NI hardware-accelerated, ~GB/s), NOT
## Nim's pure-software `checksums/sha1` (~0.8 GB/s): in a bulk-download bench the hash
## runs on every byte, so a software hash bottlenecks the client's core and unfairly
## understates throughput vs Go/Rust/Node (which all hash with hardware SHA-1). navi
## already links libcrypto (-d:ssl), so this reuses it (std/openssl's DLLUtilName).

import std/openssl

const blockSize* = 1 shl 20   # 1 MiB

proc fillBlock*(): string =
  ## One 1 MiB block of non-trivial (LCG) bytes, built once. Non-zero so a TLS
  ## record layer can't collapse the transfer to nothing.
  result = newString(blockSize)
  var x = 0x12345678'u32
  for i in 0 ..< blockSize:
    x = x * 1664525'u32 + 1013904223'u32
    result[i] = char(x shr 24)

# EVP streaming-digest entry points not exposed by std/openssl; bound here against
# libcrypto (loaded via std/openssl's DLLUtilName, exactly as navi's backends do).
{.push cdecl, dynlib: DLLUtilName, importc.}
proc EVP_MD_CTX_new(): pointer
proc EVP_MD_CTX_free(ctx: pointer)
proc EVP_sha1(): pointer
proc EVP_DigestInit_ex(ctx, typ, engine: pointer): cint
proc EVP_DigestUpdate(ctx: pointer, d: pointer, cnt: csize_t): cint
proc EVP_DigestFinal_ex(ctx: pointer, md: pointer, s: ptr cuint): cint
{.pop.}

type Sha1State* = object
  ctx: pointer                 ## EVP_MD_CTX*; freed by `hex`

proc newSha1State*(): Sha1State =
  result.ctx = EVP_MD_CTX_new()
  discard EVP_DigestInit_ex(result.ctx, EVP_sha1(), nil)

proc update*(s: var Sha1State, data: string) =
  if data.len > 0:
    discard EVP_DigestUpdate(s.ctx, unsafeAddr data[0], csize_t(data.len))

proc hex*(s: var Sha1State): string =
  ## Finalize to a lowercase hex digest and free the context.
  var md: array[20, uint8]
  var n: cuint
  discard EVP_DigestFinal_ex(s.ctx, addr md[0], addr n)
  EVP_MD_CTX_free(s.ctx)
  s.ctx = nil
  const hexd = "0123456789abcdef"
  result = newStringOfCap(40)
  for b in md:
    result.add hexd[int(b shr 4)]
    result.add hexd[int(b and 0x0f)]
