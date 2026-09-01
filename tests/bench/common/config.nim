## Shared, backend-agnostic config for the navi benchmark workloads. Mirror of
## tests/stress/common/config.nim (same NAVI_* dimensions and gap policy) with two
## bench-only knobs: `mode` (pooled vs a fresh connection per request) and
## `warmupSeconds` (an unmeasured prelude before the timed window). Parses the env
## into one `Config` for a single cell (one workload x one backend x one protocol).
## No navi import, so every backend and `nim js` peer (harness_js) can share the shape.

import std/[os, strutils]

type
  Config* = object
    workload*: string        ## requests|ws|sse|streamUpload|streamDownload
    proto*: string           ## h1|h2|h3 (concrete; "all" is expanded by run.sh)
    backend*: string         ## sync|asyncdispatch|chronos|js (label; the binary is the backend)
    host*: string
    basePort*: int           ## first server port; instance i listens on basePort+i
    servers*: int            ## number of server instances to round-robin
    seconds*: float          ## measured duration
    warmupSeconds*: float    ## unmeasured warmup before the timed window
    mode*: string            ## pooled (reuse connections) | cold (fresh conn per request)
    clients*: int            ## navi clients per backend
    concurrency*: int        ## in-flight requests per client (async fan-out width)
    reqCompression*: string  ## none|gzip|deflate (request body; native only)
    respCompression*: string ## none|gzip|deflate|br|zstd (asked via x-want-encoding)
    reportSeconds*: int      ## per-report cadence
    streamBytes*: int        ## stream transfer size (bytes)
    cert*: string            ## CA/cert path for TLS verification

proc getInt(name: string, def: int): int =
  let v = getEnv(name, "")
  if v.len == 0: def else: parseInt(v)

proc getFloat(name: string, def: float): float =
  let v = getEnv(name, "")
  if v.len == 0: def else: parseFloat(v)

proc loadConfig*(backend: string): Config =
  ## Read one cell's config. `backend` is the label for this binary.
  result = Config(
    workload: getEnv("NAVI_WORKLOAD", "requests"),
    proto: getEnv("NAVI_PROTO", "h2"),
    backend: backend,
    host: getEnv("NAVI_HOST", "127.0.0.1"),
    basePort: getInt("NAVI_BASE_PORT", 9443),
    servers: max(1, getInt("NAVI_SERVERS", 5)),
    seconds: getFloat("NAVI_SECONDS", 20.0),
    warmupSeconds: getFloat("NAVI_WARMUP_SECONDS", 2.0),
    mode: getEnv("NAVI_MODE", "pooled"),
    clients: max(1, getInt("NAVI_CLIENTS", 3)),
    concurrency: max(1, getInt("NAVI_CONCURRENCY", 8)),
    reqCompression: getEnv("NAVI_REQ_COMPRESSION", "gzip"),
    respCompression: getEnv("NAVI_RESP_COMPRESSION", "gzip"),
    reportSeconds: max(1, getInt("NAVI_REPORT_SECONDS", 60)),
    streamBytes: getInt("NAVI_STREAM_BYTES", 1073741824),
    cert: getEnv("NAVI_CERT", ""))

proc cold*(c: Config): bool = c.mode == "cold"

proc label*(c: Config): string =
  ## The tag prefixed to every report line, e.g. "[requests h2 chronos]".
  "[" & c.workload & " " & c.proto & " " & c.backend & "]"

proc expectedVersion*(c: Config): string =
  ## The exact HTTP version a version-pinned cell must negotiate on every request. A
  ## cell exists to exercise one protocol, so any upgrade OR downgrade is a failure.
  ## Returns "" only where the check can't apply: js (the runtime hides the version)
  ## and WebSocket (an h1 upgrade whose client doesn't dial a version).
  if c.backend == "js": return ""
  case c.proto
  of "h1": "HTTP/1.1"
  of "h2": "HTTP/2"
  of "h3": "HTTP/3"
  else: ""

proc checkVersion*(c: Config, got: string) =
  ## Fail hard if a request did not negotiate the pinned protocol, so a benchmark
  ## cell can never post a number for the wrong protocol. No-op when uncheckable.
  let want = c.expectedVersion
  if want.len > 0 and got != want:
    stderr.writeLine c.label & " FAIL: wrong protocol -- expected " & want &
      ", got '" & got & "' (NAVI_PROTO=" & c.proto & " must negotiate exactly that)"
    quit(1)

type VersionGate* = object
  ## Version check for a long-lived stream that can upgrade across reconnects (SSE):
  ## an h3 SSE stream necessarily begins on h2 and switches to h3 only after an
  ## Alt-Svc reconnect, so h2 is tolerated during an h3 run until the upgrade; every
  ## other mismatch fails immediately, and `finish` requires the pinned version to
  ## have actually been reached.
  cfg: Config
  want: string
  sawExpected: bool

proc initVersionGate*(c: Config): VersionGate =
  VersionGate(cfg: c, want: c.expectedVersion)

proc sample*(g: var VersionGate, got: string) =
  if g.want.len == 0 or got.len == 0: return
  if got == g.want:
    g.sawExpected = true
  elif g.want == "HTTP/3" and got == "HTTP/2":
    discard                             # h3 SSE begins on h2 until the Alt-Svc reconnect
  else:
    g.cfg.checkVersion(got)             # any other upgrade/downgrade: hard-fail now

proc finish*(g: VersionGate) =
  if g.want.len > 0 and not g.sawExpected:
    stderr.writeLine g.cfg.label & " FAIL: never negotiated " & g.want &
      " over the whole run (NAVI_PROTO=" & g.cfg.proto & ")"
    quit(1)

proc skipReason*(c: Config): string =
  ## Non-empty when this cell cannot run on this build/backend, so the client prints
  ## it and exits 0 (a skip, not a failure). run.sh avoids most of these; the client
  ## double-checks (e.g. a build without -d:naviHttp3).
  if c.proto == "h3":
    when not defined(naviHttp3):
      return "skip: h3 needs a -d:naviHttp3 build (use the h3 image)"
  if c.backend == "js":
    if c.workload == "streamUpload": return "skip: js cannot stream request bodies"
    if c.proto == "h3": return "skip: js/undici has no HTTP/3"
  ""
