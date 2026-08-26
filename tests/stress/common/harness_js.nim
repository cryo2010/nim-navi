## Shared js-backend helpers for the stress workloads (compiled to js). std/os and
## the native /proc reporter don't apply under `nim js`, so config comes from
## process.env and RSS from process.memoryUsage(). A small status counter mirrors
## the native reporter's shape.

import std/[strutils, tables]

proc envJs*(name, dflt: cstring): cstring {.importjs: "(process.env[#] ?? #)".}
proc nowMs*(): float {.importjs: "Date.now()".}
proc rssMb*(): int {.importjs: "Math.round(process.memoryUsage().rss / 1048576)".}
proc setIntervalJs*(cb: proc (), ms: int): int {.importjs: "setInterval(#, #)".}
proc clearIntervalJs*(id: int) {.importjs: "clearInterval(#)".}

type JsCfg* = object
  host*, proto*: string
  basePort*, servers*, clients*, concurrency*, reportSeconds*, streamBytes*: int
  seconds*: float

proc envInt(name: string, def: int): int =
  let v = $envJs(name.cstring, "".cstring)
  if v.len == 0: def else: parseInt(v)

proc loadJsCfg*(): JsCfg =
  JsCfg(
    host: $envJs("NAVI_HOST", "127.0.0.1"),
    proto: $envJs("NAVI_PROTO", "h2"),
    basePort: envInt("NAVI_BASE_PORT", 9443),
    servers: max(1, envInt("NAVI_SERVERS", 5)),
    clients: max(1, envInt("NAVI_CLIENTS", 3)),
    concurrency: max(1, envInt("NAVI_CONCURRENCY", 32)),
    reportSeconds: max(1, envInt("NAVI_REPORT_SECONDS", 60)),
    streamBytes: envInt("NAVI_STREAM_BYTES", 1073741824),
    seconds: parseFloat($envJs("NAVI_SECONDS", "60")))

type JsPool* = object
  bases: seq[string]
  next: int

proc initJsPool*(cfg: JsCfg): JsPool =
  for i in 0 ..< cfg.servers:
    result.bases.add "https://" & cfg.host & ":" & $(cfg.basePort + i)

proc pick*(p: var JsPool): string =
  result = p.bases[p.next]; p.next = (p.next + 1) mod p.bases.len

type JsCounter* = ref object
  counts: Table[int, int]
  errors*, ops*: int

proc newJsCounter*(): JsCounter = JsCounter(counts: initTable[int, int]())
proc tally*(c: JsCounter, s: int) = c.counts.mgetOrPut(s, 0).inc; inc c.ops
proc note*(c: JsCounter) = inc c.errors; inc c.ops

proc render(c: JsCounter): string =
  var parts: seq[string]
  for k, v in c.counts: parts.add $k & "x" & $v
  if c.errors > 0: parts.add "err" & $c.errors
  if parts.len == 0: "(none)" else: parts.join(" ")

proc report*(c: JsCounter, label: string, start: float) =
  echo label, " ", c.render, " | RSS ", rssMb(), "MB | t=",
       int((nowMs() - start) / 1000.0), "s"
