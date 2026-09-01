## Benchmark reporter (native). Records per-request latency into a flat histogram
## and, at end of run, emits one RESULT line the harness tabulates:
##
##   RESULT\t<name>\t<reqs>\t<secs>\t<reqs_per_sec>\t<p50_ms>\t<p99_ms>\t<p999_ms>\t<mb_per_sec>
##
## <name> is the client identity (e.g. navi-async, go); the (workload, proto) cell
## is supplied by run.sh, which groups RESULT lines into one ranked table per cell.
## A periodic liveness line (ops + RSS + heap) is printed during long runs so a soak
## still shows memory flatness. RSS is from /proc/self/statm (Linux; Dockerized).

import std/strutils
import ./histogram

type BenchRecorder* = ref object
  hist: Histogram
  ops*: int                  ## completed requests/transfers (any 2xx-ish outcome)
  bytes*: int64              ## cumulative payload bytes, for MB/s (streaming)
  errors*: int               ## surfaced transport failures (a benchmark should see none)

proc newBenchRecorder*(): BenchRecorder =
  BenchRecorder(hist: initHistogram())

proc record*(r: BenchRecorder, us: int64, nbytes: int64 = 0) =
  ## One completed op that took `us` microseconds and moved `nbytes` payload bytes.
  r.hist.record(us)
  inc r.ops
  r.bytes += nbytes

proc fail*(r: BenchRecorder) = inc r.errors

proc merge*(r: BenchRecorder, o: BenchRecorder) =
  ## Fold a per-worker recorder into a per-cell one.
  r.hist.merge(o.hist); r.ops += o.ops; r.bytes += o.bytes; r.errors += o.errors

proc rssBytes*(): int =
  ## Resident set size (Linux). statm field 2 is RSS in pages; page size 4096 on the
  ## Linux targets. A memory-trend signal, not exact accounting.
  when defined(linux):
    try:
      let fields = readFile("/proc/self/statm").split()
      if fields.len >= 2: return parseInt(fields[1]) * 4096
    except CatchableError: discard
  0

proc fmtBytes*(n: int): string =
  if n >= 1 shl 20: $(n div (1 shl 20)) & "MB"
  elif n >= 1 shl 10: $(n div (1 shl 10)) & "KB"
  else: $n & "B"

proc report*(label: string, r: BenchRecorder, elapsed: float) =
  ## One liveness line during the run (not the final RESULT).
  let rss = rssBytes()
  echo label, " ops=", r.ops, (if r.errors > 0: " err" & $r.errors else: ""),
       " | RSS ", (if rss > 0: fmtBytes(rss) else: "n/a"),
       " | heap ", fmtBytes(getOccupiedMem()), " | t=", elapsed.int, "s"

proc resultLine*(name: string, r: BenchRecorder, secs: float): string =
  let rps = (if secs > 0: r.ops.float / secs else: 0.0)
  let mbps = (if secs > 0: r.bytes.float / secs / 1e6 else: 0.0)
  "RESULT\t" & name & "\t" & $r.ops & "\t" & formatFloat(secs, ffDecimal, 3) &
    "\t" & $int(rps + 0.5) &
    "\t" & formatFloat(r.hist.percentileMs(50), ffDecimal, 3) &
    "\t" & formatFloat(r.hist.percentileMs(99), ffDecimal, 3) &
    "\t" & formatFloat(r.hist.percentileMs(99.9), ffDecimal, 3) &
    "\t" & formatFloat(mbps, ffDecimal, 1)

proc emitResult*(name: string, r: BenchRecorder, secs: float) =
  ## Print the final RESULT line (harness greps `^RESULT`).
  echo resultLine(name, r, secs)
