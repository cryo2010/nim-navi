## Fixed-block content for the 1 GiB streaming workloads. One reusable 1 MiB block
## is streamed `size/blockSize` times (+ remainder), hashed incrementally as it
## flies, so both client and server stay constant-memory regardless of size. Each
## side hashes only its own bytes, so the client's block and the server's block
## need not match -- only each side's bytes-and-hash must agree end to end.

import std/strutils
import checksums/sha1
export sha1                 # newSha1State / update / finalize / SecureHash / `$`

const blockSize* = 1 shl 20   # 1 MiB

proc fillBlock*(): string =
  ## One 1 MiB block of non-trivial (LCG) bytes, built once. Non-zero so a TLS
  ## record layer can't collapse the transfer to nothing.
  result = newString(blockSize)
  var x = 0x12345678'u32
  for i in 0 ..< blockSize:
    x = x * 1664525'u32 + 1013904223'u32
    result[i] = char(x shr 24)

proc stampBlock*(blk: var string, idx: int) =
  ## Write the block index into the first 8 bytes so the repeated 1 MiB blocks are
  ## no longer byte-identical. A whole-block reorder or duplication on the wire then
  ## changes the SHA-1 (otherwise invisible, since every block would hash the same).
  ## Constant-memory: the caller reuses one block and re-stamps it per chunk.
  var v = uint64(idx)
  for k in countdown(7, 0):
    blk[k] = char(v and 0xff'u64)
    v = v shr 8

proc hex*(st: var Sha1State): string =
  ## Finalize to a lowercase hex digest.
  ($SecureHash(st.finalize())).toLowerAscii
