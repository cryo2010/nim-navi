## Per-minute status + memory reporter for the stress workloads (native).
##
## Tallies HTTP status codes in a small table and never holds a response body,
## so a multi-hour soak stays flat in memory. RSS is read from `/proc/self/statm`
## (Linux; everything here is Dockerized) so it also catches C-side allocations
## (OpenSSL, zlib) that a Nim-heap number would miss; `getOccupiedMem()` is printed
## alongside to separate a Nim-heap leak from a C-side one.

import std/[tables, strutils, algorithm]

type StatusCounter* = ref object
  counts*: Table[int, int]   ## HTTP status -> count
  errors*: int               ## transport failures / exceptions (soak continues)
  ops*: int                  ## completed requests (any outcome)
  lastOps: int               ## ops as of the previous report line
  lastElapsed: float         ## elapsed (s) as of the previous report line

proc newStatusCounter*(): StatusCounter =
  StatusCounter(counts: initTable[int, int]())

proc tally*(c: StatusCounter, status: int) =
  ## Record a response's status code, then let the response go out of scope.
  c.counts.mgetOrPut(status, 0).inc
  c.ops.inc

proc fail*(c: StatusCounter) =
  ## Record a transport failure / exception without aborting the soak.
  c.errors.inc
  c.ops.inc

proc rssBytes*(): int =
  ## Resident set size of this process, or 0 if unavailable (non-Linux). statm's
  ## second field is RSS in pages; the page size is 4096 on the Linux targets we
  ## run under (this is a memory-trend signal, not exact accounting).
  when defined(linux):
    try:
      let fields = readFile("/proc/self/statm").split()
      if fields.len >= 2:
        return parseInt(fields[1]) * 4096
    except CatchableError: discard
  0

proc fmtBytes*(n: int): string =
  if n >= 1 shl 30: $(n div (1 shl 20)) & "MB"     # >1GiB still shown in MB for trend
  elif n >= 1 shl 20: $(n div (1 shl 20)) & "MB"
  elif n >= 1 shl 10: $(n div (1 shl 10)) & "KB"
  else: $n & "B"

proc opsPerSec*(ops: int, seconds: float): float =
  ## Throughput over a window, guarded: a zero, negative or sub-millisecond window
  ## (and an empty window) yields 0.0 instead of a division by zero or inf/nan.
  if ops <= 0 or seconds <= 0.001: 0.0 else: ops.float / seconds

proc fmtRate*(ops: int, seconds: float): string =
  ## The throughput number with one decimal, e.g. "751.9". Never inf/nan.
  formatFloat(opsPerSec(ops, seconds), ffDecimal, 1)

proc render*(c: StatusCounter): string =
  ## "200x45123 503x12 err3" — sorted by status for stable output.
  var keys: seq[int]
  for k in c.counts.keys: keys.add k
  keys.sort()
  var parts: seq[string]
  for k in keys: parts.add $k & "x" & $c.counts[k]
  if c.errors > 0: parts.add "err" & $c.errors
  if parts.len == 0: "(no requests yet)" else: parts.join(" ")

proc report*(label: string, c: StatusCounter, elapsed: float, final = false) =
  ## One report line. RSS is the soak's memory-flatness signal; the ops/s field is
  ## the rate over the interval since the previous line (the first line's window
  ## starts at the run's start), so a throughput dip shows where it happened.
  ## `final` prints the whole-run average instead and leaves the interval
  ## bookkeeping untouched. The bookkeeping lives in the counter so the async and
  ## sync clients, which drive report() on their own cadences, print the same thing.
  let rss = rssBytes()
  let rssStr = if rss > 0: fmtBytes(rss) else: "n/a"
  let winOps = if final: c.ops else: c.ops - c.lastOps
  let winSecs = if final: elapsed else: elapsed - c.lastElapsed
  if not final:
    c.lastOps = c.ops
    c.lastElapsed = elapsed
  echo label, " ", c.render, " | RSS ", rssStr,
       " | heap ", fmtBytes(getOccupiedMem()),
       " | ", fmtRate(winOps, winSecs), " ops/s",
       " | t=", elapsed.int, "s"

# --- chaos tallies ----------------------------------------------------------

type ChaosCounter* = ref object
  ## Per-mode outcome tallies for the chaos phase, rendered as a second per-
  ## interval line next to the canary's StatusCounter (which is untouched). Each
  ## mode counts ok (a valid Response), expectedErr (the mode's expected typed
  ## navi error) and otherErr (a different catchable typed error -- tolerated for
  ## tolerant modes, tallied but not enforced). A strict-mode miss or an
  ## invariant violation never lands here: it hard-fails via a CHAOS-FAIL(...)
  ## quit in chaos.nim before it could be tallied.
  ok*: Table[string, int]
  expectedErr*: Table[string, int]
  otherErr*: Table[string, int]
  ops*: int                    ## total chaos interactions (any outcome)

proc newChaosCounter*(): ChaosCounter =
  ChaosCounter(ok: initTable[string, int](),
               expectedErr: initTable[string, int](),
               otherErr: initTable[string, int]())

proc tallyOk*(c: ChaosCounter, mode: string) =
  c.ok.mgetOrPut(mode, 0).inc; c.ops.inc

proc tallyExpected*(c: ChaosCounter, mode: string) =
  c.expectedErr.mgetOrPut(mode, 0).inc; c.ops.inc

proc tallyOther*(c: ChaosCounter, mode: string) =
  c.otherErr.mgetOrPut(mode, 0).inc; c.ops.inc

proc renderChaos*(c: ChaosCounter): string =
  ## "<mode> ok/exp/other ..." sorted by mode for stable, diffable output (seed
  ## reproducibility compares these tallies across runs).
  var modes: seq[string]
  for k in c.ok.keys: modes.add k
  for k in c.expectedErr.keys:
    if k notin c.ok: modes.add k
  for k in c.otherErr.keys:
    if k notin c.ok and k notin c.expectedErr: modes.add k
  modes.sort()
  var parts: seq[string]
  for m in modes:
    parts.add m & " " & $c.ok.getOrDefault(m, 0) & "/" &
      $c.expectedErr.getOrDefault(m, 0) & "/" & $c.otherErr.getOrDefault(m, 0)
  if parts.len == 0: "(no chaos ops yet)" else: parts.join(" ")

proc reportChaos*(label: string, c: ChaosCounter, fd: int) =
  ## The chaos interval line: per-mode ok/exp/other + live memory + fd. `label`
  ## already carries the " chaos" suffix (e.g. "[requests h1 chronos chaos]").
  ## fd < 0 (non-Linux) renders as "n/a".
  let rss = rssBytes()
  let rssStr = if rss > 0: fmtBytes(rss) else: "n/a"
  let fdStr = if fd >= 0: $fd else: "n/a"
  echo label, " ", c.renderChaos, " | RSS ", rssStr,
       " | heap ", fmtBytes(getOccupiedMem()), " | fd ", fdStr
