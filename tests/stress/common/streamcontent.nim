## Fixed-block content + throughput accounting for the streaming workloads.
##
## Content: one reusable 1 MiB block is streamed `size/blockSize` times (+ remainder),
## hashed incrementally as it flies, so both client and server stay constant-memory
## regardless of size. Each side hashes only its own bytes, so the client's block and
## the server's block need not match -- only each side's bytes-and-hash must agree
## end to end.
##
## Throughput: the streaming cells are measured in megabytes, not in transfers (a
## 1 GiB transfer is minutes long, so "3 done" says almost nothing about the rate).
## `StreamRate` carries the cumulative byte count and the previous report's marker so
## every client -- async, sync and js -- prints the same shape: cumulative MB first,
## then MB/s over the interval since the last line. It is a ref so the sync clients
## can capture it in the producer/`each` callback that does their reporting, and so
## the async clients can hang it off the `Progress` they already pass around (no
## globals: procs are gcsafe-checked under chronos).
##
## The SHA-1 half is native-only (checksums/sha1 uses copyMem); the rate half is
## shared with the js client, which hashes via Node's crypto instead.

import std/strutils
when not defined(js):
  import checksums/sha1
  export sha1             # newSha1State / update / finalize / SecureHash / `$`

const blockSize* = 1 shl 20   # 1 MiB; also the MB divisor for every report line

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

when not defined(js):
  proc hex*(st: var Sha1State): string =
    ## Finalize to a lowercase hex digest.
    ($SecureHash(st.finalize())).toLowerAscii

# --- throughput accounting --------------------------------------------------

const mbDiv = 1048576.0       # 1 shl 20 as a float: MB everywhere means MiB

proc megabytes*(bytes: float): int =
  ## Whole megabytes (MiB) moved. Float in: the js backend overflow-checks int at
  ## 2^31 and a soak crosses 2 GiB in seconds, so js keeps its byte total as a float.
  int(bytes / mbDiv)

proc megabytes*(bytes: int): int =
  bytes div blockSize

proc mbPerSec*(bytes, seconds: float): string =
  ## "51.2": megabytes per second over `seconds`, one decimal. A window at or below
  ## a millisecond carries no meaningful rate and would divide by zero or render
  ## `inf`, so it prints "n/a"; so does a negative window (a stepped clock) or a
  ## negative byte count. Everything downstream of this guard is finite.
  if seconds <= 1e-3 or bytes < 0.0: return "n/a"
  let rate = bytes / mbDiv / seconds
  if rate != rate: return "n/a"           # nan (unreachable past the guards above)
  formatFloat(rate, ffDecimal, 1)

proc mbPerSec*(bytes: int, seconds: float): string =
  mbPerSec(bytes.float, seconds)

type StreamRate* = ref object
  ## Cumulative bytes moved plus the previous report line's marker. Ref so the sync
  ## clients can capture it in the callback they report from, and so it survives
  ## across transfers (the old sync lines restarted at 0 every transfer and never
  ## showed a cumulative total).
  total*: int        ## cumulative bytes moved across every transfer so far
  transfers*: int    ## completed transfers; the sync clients report from inside a
                     ## callback and cannot reach a `var int` in their caller
  lastTotal: int     ## `total` as of the previous report line
  lastAt: float      ## monotonic-ish timestamp of the previous report line
  startedAt: float   ## timestamp the run began, for the whole-run average

proc newStreamRate*(now: float): StreamRate =
  StreamRate(total: 0, transfers: 0, lastTotal: 0, lastAt: now, startedAt: now)

proc add*(r: StreamRate, n: int) =
  ## Count `n` more bytes on the wire. Called per chunk, so it stays this cheap.
  r.total += n

proc due*(r: StreamRate, now: float, everySeconds: int): bool =
  ## Has the report cadence elapsed since the last line?
  now - r.lastAt >= everySeconds.float

proc mark*(r: StreamRate, now: float): tuple[mb: int, rate: string] =
  ## Close the reporting interval at `now`: cumulative MB and the interval's MB/s.
  result = (megabytes(r.total), mbPerSec(r.total - r.lastTotal, now - r.lastAt))
  r.lastTotal = r.total
  r.lastAt = now

proc elapsed*(r: StreamRate, now: float): float =
  now - r.startedAt

proc summary*(bytes: float, seconds: float, dir: string,
              transfers, retried: int): string =
  ## The tail of the final "== ... passed (...) ==" line: megabytes and the
  ## whole-run average first, transfer count demoted to a secondary field.
  ## e.g. "4096MB rx in 61s, 67.1 MB/s, 4 transfers, 0 retried"
  $megabytes(bytes) & "MB " & dir & " in " & $int(seconds) & "s, " &
    mbPerSec(bytes, seconds) & " MB/s, " & $transfers & " transfers, " &
    $retried & " retried"

proc summary*(bytes: int, seconds: float, dir: string,
              transfers, retried: int): string =
  summary(bytes.float, seconds, dir, transfers, retried)
