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

proc fmtBytes(n: int): string =
  if n >= 1 shl 30: $(n div (1 shl 20)) & "MB"     # >1GiB still shown in MB for trend
  elif n >= 1 shl 20: $(n div (1 shl 20)) & "MB"
  elif n >= 1 shl 10: $(n div (1 shl 10)) & "KB"
  else: $n & "B"

proc render*(c: StatusCounter): string =
  ## "200x45123 503x12 err3" — sorted by status for stable output.
  var keys: seq[int]
  for k in c.counts.keys: keys.add k
  keys.sort()
  var parts: seq[string]
  for k in keys: parts.add $k & "x" & $c.counts[k]
  if c.errors > 0: parts.add "err" & $c.errors
  if parts.len == 0: "(no requests yet)" else: parts.join(" ")

proc report*(label: string, c: StatusCounter, elapsed: float) =
  ## One report line. RSS is the soak's memory-flatness signal.
  let rss = rssBytes()
  let rssStr = if rss > 0: fmtBytes(rss) else: "n/a"
  echo label, " ", c.render, " | RSS ", rssStr,
       " | heap ", fmtBytes(getOccupiedMem()),
       " | t=", elapsed.int, "s"
