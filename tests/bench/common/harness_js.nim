## Shared js-backend helpers for the benchmark workloads (compiled to js). Config
## from process.env, a high-resolution timer + a latency histogram matching the
## native one (bucket = floor(log2(us) * 64)), and the same RESULT line the native
## reporter emits so the harness tabulates js alongside every other client.

import std/[strutils, math]

proc envJs*(name, dflt: cstring): cstring {.importjs: "(process.env[#] ?? #)".}
proc nowMs*(): float {.importjs: "Date.now()".}
proc nowUs*(): float {.importjs: "(performance.now() * 1000)".}
proc rssMb*(): int {.importjs: "Math.round(process.memoryUsage().rss / 1048576)".}
proc heapUsedMb*(): int {.importjs: "Math.round(process.memoryUsage().heapUsed / 1048576)".}
proc setIntervalJs*(cb: proc (), ms: int): int {.importjs: "setInterval(#, #)".}
proc clearIntervalJs*(id: int) {.importjs: "clearInterval(#)".}

type JsCfg* = object
  host*, proto*, mode*: string
  basePort*, servers*, clients*, concurrency*, reportSeconds*, streamBytes*: int
  seconds*, warmupSeconds*: float

proc envInt(name: string, def: int): int =
  let v = $envJs(name.cstring, "".cstring)
  if v.len == 0: def else: parseInt(v)

proc envFloat(name: string, def: float): float =
  let v = $envJs(name.cstring, "".cstring)
  if v.len == 0: def else: parseFloat(v)

proc loadJsCfg*(): JsCfg =
  JsCfg(
    host: $envJs("NAVI_HOST", "127.0.0.1"),
    proto: $envJs("NAVI_PROTO", "h2"),
    mode: $envJs("NAVI_MODE", "pooled"),
    basePort: envInt("NAVI_BASE_PORT", 9443),
    servers: max(1, envInt("NAVI_SERVERS", 5)),
    clients: max(1, envInt("NAVI_CLIENTS", 3)),
    concurrency: max(1, envInt("NAVI_CONCURRENCY", 8)),
    reportSeconds: max(1, envInt("NAVI_REPORT_SECONDS", 60)),
    streamBytes: envInt("NAVI_STREAM_BYTES", 1073741824),
    seconds: envFloat("NAVI_SECONDS", 20.0),
    warmupSeconds: envFloat("NAVI_WARMUP_SECONDS", 2.0))

proc cold*(c: JsCfg): bool = c.mode == "cold"

type JsPool* = object
  bases: seq[string]
  next: int

proc initJsPool*(cfg: JsCfg): JsPool =
  for i in 0 ..< cfg.servers:
    result.bases.add "https://" & cfg.host & ":" & $(cfg.basePort + i)

proc pick*(p: var JsPool): string =
  result = p.bases[p.next]; p.next = (p.next + 1) mod p.bases.len

# --- latency histogram (same log-bucket scheme as common/histogram.nim) ---
const bucketsPerDoubling = 64
let maxBuckets = int(floor(log2(300_000_000.0) * bucketsPerDoubling)) + 1

type JsBench* = ref object
  counts: seq[int]
  ops*, errors*: int
  bytes*: float                        # float: js int overflow-checks at 2^31

proc newJsBench*(): JsBench =
  JsBench(counts: newSeq[int](maxBuckets))

proc record*(b: JsBench, us: float, nbytes: float = 0.0) =
  var v = us
  if v < 1.0: v = 1.0
  var idx = int(floor(log2(v) * bucketsPerDoubling))
  if idx < 0: idx = 0
  elif idx >= maxBuckets: idx = maxBuckets - 1
  inc b.counts[idx]; inc b.ops; b.bytes += nbytes

proc note*(b: JsBench) = inc b.errors

proc percentileMs(b: JsBench, p: float): float =
  if b.ops == 0: return 0
  let target = max(1, int(ceil(p / 100.0 * float(b.ops))))
  var cum = 0
  for i in 0 ..< b.counts.len:
    cum += b.counts[i]
    if cum >= target:
      return pow(2.0, (float(i) + 0.5) / bucketsPerDoubling) / 1000.0
  0

proc emitResult*(b: JsBench, name: string, secs: float) =
  let rps = (if secs > 0: b.ops.float / secs else: 0.0)
  let mbps = (if secs > 0: b.bytes / secs / 1e6 else: 0.0)
  echo "RESULT\t", name, "\t", b.ops, "\t", formatFloat(secs, ffDecimal, 3),
       "\t", int(rps + 0.5),
       "\t", formatFloat(b.percentileMs(50), ffDecimal, 3),
       "\t", formatFloat(b.percentileMs(99), ffDecimal, 3),
       "\t", formatFloat(b.percentileMs(99.9), ffDecimal, 3),
       "\t", formatFloat(mbps, ffDecimal, 1)
