## Shared, backend-agnostic config for the navi stress workloads.
##
## Parses the `NAVI_*` env into one `Config` for a single cell
## (one workload × one backend × one protocol), and provides the defensive gap
## check a client uses to skip an impossible cell (e.g. h3 on a non-`-d:naviHttp3`
## build). The workload×backend×protocol *matrix* is iterated by run.sh; each
## client binary runs exactly one cell. No navi import here, so every backend and
## `nim js` can share it.

import std/[os, strutils]

type
  Config* = object
    workload*: string        ## requests|ws|sse|streamUpload|streamDownload
    proto*: string           ## h1|h2|h3 (concrete; "all" is expanded by run.sh)
    backend*: string         ## sync|asyncdispatch|chronos|js (label; the binary is the backend)
    host*: string
    basePort*: int           ## first server port; instance i listens on basePort+i
    servers*: int            ## number of server instances to round-robin
    seconds*: float          ## soak duration
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
    seconds: getFloat("NAVI_SECONDS", 60.0),
    clients: max(1, getInt("NAVI_CLIENTS", 3)),
    concurrency: max(1, getInt("NAVI_CONCURRENCY", 8)),
    reqCompression: getEnv("NAVI_REQ_COMPRESSION", "gzip"),
    respCompression: getEnv("NAVI_RESP_COMPRESSION", "gzip"),
    reportSeconds: max(1, getInt("NAVI_REPORT_SECONDS", 60)),
    streamBytes: getInt("NAVI_STREAM_BYTES", 1073741824),
    cert: getEnv("NAVI_CERT", ""))

proc label*(c: Config): string =
  ## The tag prefixed to every report line, e.g. "[requests h2 chronos]".
  "[" & c.workload & " " & c.proto & " " & c.backend & "]"

proc expectedVersion*(c: Config): string =
  ## The exact HTTP version a version-pinned cell must negotiate on every request. A
  ## cell exists to exercise one protocol, so any upgrade OR downgrade to a different
  ## version is a failure -- including an h1 cell that ends up on h2. Returns "" only
  ## where the check can't apply: js (the runtime chooses/hides the version) and
  ## WebSocket (an h1 upgrade whose client doesn't dial a version).
  if c.backend == "js": return ""
  case c.proto
  of "h1": "HTTP/1.1"
  of "h2": "HTTP/2"
  of "h3": "HTTP/3"
  else: ""

proc checkVersion*(c: Config, got: string) =
  ## Fail the run hard if a request did not negotiate the pinned protocol. The cell
  ## exists to exercise exactly that protocol, so a silent upgrade or downgrade to a
  ## different version must fail rather than pass green and hide the regression. A
  ## no-op when the version isn't checkable ("" expected).
  let want = c.expectedVersion
  if want.len > 0 and got != want:
    stderr.writeLine c.label & " FAIL: wrong protocol -- expected " & want &
      ", got '" & got & "' (NAVI_PROTO=" & c.proto & " must negotiate exactly that)"
    quit(1)

type VersionGate* = object
  ## Version check for a long-lived stream that can upgrade across reconnects (SSE).
  ## A per-request `checkVersion` is too strict there for one case only: an h3 SSE
  ## stream necessarily begins on h2 and switches to h3 only after an Alt-Svc
  ## reconnect. So h2 is tolerated during an h3 run (until the upgrade); every other
  ## mismatch fails immediately, and `finish` requires the pinned version to have
  ## actually been reached.
  cfg: Config
  want: string
  sawExpected: bool

proc initVersionGate*(c: Config): VersionGate =
  VersionGate(cfg: c, want: c.expectedVersion)

proc sample*(g: var VersionGate, got: string) =
  ## Record one observed version of the stream's current connection. Strict: any
  ## version other than the pinned one hard-fails now, except the unavoidable h2 ->
  ## h3 warmup of an h3 SSE stream.
  if g.want.len == 0 or got.len == 0: return
  if got == g.want:
    g.sawExpected = true
  elif g.want == "HTTP/3" and got == "HTTP/2":
    discard                             # h3 SSE begins on h2 until the Alt-Svc reconnect
  else:
    g.cfg.checkVersion(got)             # any other upgrade/downgrade: hard-fail now

proc finish*(g: VersionGate) =
  ## End of run: the pinned version must have been negotiated at least once (catches
  ## an h3 SSE stream that stayed on h2 and never actually upgraded).
  if g.want.len > 0 and not g.sawExpected:
    stderr.writeLine g.cfg.label & " FAIL: never negotiated " & g.want &
      " over the whole run (NAVI_PROTO=" & g.cfg.proto & ")"
    quit(1)

proc skipReason*(c: Config): string =
  ## Non-empty when this cell cannot run on this build/backend, so the client
  ## should print it and exit 0 (a skip, not a failure). run.sh avoids most of
  ## these, but the client double-checks (e.g. a build without -d:naviHttp3).
  if c.proto == "h3":
    when not defined(naviHttp3):
      return "skip: h3 needs a -d:naviHttp3 build (use the h3 image)"
  if c.backend == "js":
    if c.workload == "streamUpload": return "skip: js cannot stream request bodies"
    if c.proto == "h3": return "skip: js/undici has no HTTP/3"
  # h3 WebSocket (Extended CONNECT, RFC 9220) needs a background reader, so it is
  # async-only: the sync backend serves WebSocket over HTTP/1.1 only.
  if c.workload == "ws" and c.proto == "h3" and c.backend == "sync":
    return "skip: h3 WebSocket is async/chronos only (sync is h1-only)"
  ""
