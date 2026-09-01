## Dependency-light latency histogram for the benchmark reporter. Log-bucketed on a
## base-2 scale with `bucketsPerDoubling` linear sub-buckets per octave: fixed
## relative error (~1.1% at 64/doubling) in flat memory (~1800 int buckets)
## regardless of sample count. The SAME bucketing is reproduced in the Go/Rust/Node/
## Python reference clients (bucket = floor(log2(us) * 64)) so the p50/p99/p999
## columns are comparable across languages.
##
## Values are microseconds. `record` is O(1); `percentileUs` is O(buckets).

import std/math

const
  bucketsPerDoubling* = 64
  maxValueUs* = 300_000_000            # 300 s ceiling; anything slower clamps here

let maxBuckets = int(floor(log2(float(maxValueUs)) * bucketsPerDoubling)) + 1

type Histogram* = object
  counts: seq[int]
  total*: int

proc initHistogram*(): Histogram =
  Histogram(counts: newSeq[int](maxBuckets), total: 0)

proc bucketOf(us: int64): int =
  var v = us
  if v < 1: v = 1                      # clamp sub-microsecond to the first bucket
  result = int(floor(log2(float(v)) * bucketsPerDoubling))
  if result < 0: result = 0
  elif result >= maxBuckets: result = maxBuckets - 1

proc record*(h: var Histogram, us: int64) =
  inc h.counts[bucketOf(us)]
  inc h.total

proc merge*(h: var Histogram, o: Histogram) =
  ## Fold a per-worker histogram into a per-cell one.
  for i in 0 ..< h.counts.len: h.counts[i] += o.counts[i]
  h.total += o.total

proc percentileUs*(h: Histogram, p: float): float =
  ## The value (microseconds) at percentile `p` in 0..100; 0 when empty. The
  ## bucket's representative value is its geometric midpoint.
  if h.total == 0: return 0
  let target = max(1, int(ceil(p / 100.0 * float(h.total))))
  var cum = 0
  for i in 0 ..< h.counts.len:
    cum += h.counts[i]
    if cum >= target:
      return pow(2.0, (float(i) + 0.5) / bucketsPerDoubling)
  return pow(2.0, (float(h.counts.len - 1) + 0.5) / bucketsPerDoubling)

proc percentileMs*(h: Histogram, p: float): float = h.percentileUs(p) / 1000.0
