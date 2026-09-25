## Shared, backend-agnostic config for the navi stress workloads.
##
## Parses the `NAVI_*` env into one `Config` for a single cell
## (one workload × one backend × one protocol), and provides the defensive gap
## check a client uses to skip an impossible cell (e.g. h3 on a non-`-d:naviHttp3`
## build). The workload×backend×protocol *matrix* is iterated by run.sh; each
## client binary runs exactly one cell. No navi import here, so every backend and
## `nim js` can share it.

import std/[os, strutils]
import payloads

type
  Config* = object
    workload*: string        ## requests|ws|sse|streamUpload|streamDownload
    proto*: string           ## h1|h2|h3 (concrete; "all" is expanded by run.sh)
    backend*: string         ## sync|asyncdispatch|chronos|js (label; the binary is the backend)
    host*: string
    basePort*: int           ## first server port; instance i listens on basePort+i
    servers*: int            ## server instances to round-robin (NAVI_SERVER_COUNT)
    seconds*: float          ## soak duration
    clients*: int            ## concurrent navi client instances (NAVI_CLIENT_COUNT)
    concurrency*: int        ## in-flight requests per client (async fan-out width)
    reqCompression*: string  ## none|gzip|deflate (request body; native only, octet/text only)
    respCompression*: string ## none|gzip|deflate|br|zstd (asked via x-want-encoding)
    contentTypes*: string    ## csv of {octet,text,json,form}; restricts the /echo rotation
    reportSeconds*: int      ## per-report cadence
    streamBytes*: int        ## stream transfer size (bytes)
    cert*: string            ## CA/cert path for TLS verification
    chaos*: ChaosConfig      ## opt-in misbehaving-server attack config (NAVI_CHAOS*)

  ChaosConfig* = object
    ## Parsed NAVI_CHAOS* knobs for one cell. `enabled` mirrors NAVI_CHAOS != none;
    ## when off everything downstream returns before allocating so an off run is
    ## byte-identical. `modes` is the requested set ("all" -> empty means "every
    ## applicable mode"; a csv narrows it), validated at parse time so a typo
    ## hard-fails naming the bad token (same discipline as NAVI_CONTENT_TYPES).
    enabled*: bool
    raw*: string             ## the NAVI_CHAOS value as given (for skip notices)
    modesAll*: bool          ## NAVI_CHAOS=all: run every mode applicable to the proto
    modes*: seq[string]      ## explicit csv of mode names (empty when modesAll)
    conc*: int               ## NAVI_CHAOS_CONC: chaos workers (async) / interleave basis (sync)
    seed*: uint64            ## NAVI_CHAOS_SEED: schedule PRNG seed, mixed with the cell id
    portBand*: int           ## NAVI_CHAOS_PORTBAND: chaos ports = base + band {+0,+1,+2,+99}
    watchdog*: int           ## NAVI_CHAOS_WATCHDOG: seconds a worker may go without progress
    fdSlack*: int            ## NAVI_CHAOS_FD_SLACK: FD leak bound
    heapSlackMb*: int        ## NAVI_CHAOS_HEAP_SLACK_MB: Nim heap leak bound
    rssSlackMb*: int         ## NAVI_CHAOS_RSS_SLACK_MB: RSS leak bound
    selfTest*: string        ## NAVI_CHAOS_SELFTEST: ""|fd|mem|hang (deliberate leak/hang)

## Every mode name the chaos catalog knows, across all protocols. Used both to
## validate a NAVI_CHAOS csv and (by chaos.nim) to build the schedule. Applicability
## to a given protocol is filtered separately in the schedule.
const chaosModeNames* = [
  "stall", "slowbody", "truncate", "garbage", "vanish", "badframes",
  "zerowindow", "headerbomb", "redirectloop",
  # port-selected accept-time modes: named so a csv can request them directly.
  "vanish-on-accept", "stall-on-accept"]

proc getInt(name: string, def: int): int =
  let v = getEnv(name, "")
  if v.len == 0: def else: parseInt(v)

proc getFloat(name: string, def: float): float =
  let v = getEnv(name, "")
  if v.len == 0: def else: parseFloat(v)

proc loadChaos(workload, proto, backend: string): ChaosConfig =
  ## Parse the NAVI_CHAOS* knobs. NAVI_CHAOS defaults to "none" -> a disabled,
  ## zero-allocation config. "all" runs every applicable mode; a csv narrows it,
  ## hard-failing at startup on an unknown token naming it (mirroring the
  ## NAVI_CONTENT_TYPES validation) so a typo never silently narrows coverage.
  let raw = getEnv("NAVI_CHAOS", "none").strip()
  if raw.len == 0 or raw == "none":
    return ChaosConfig(enabled: false)
  result.enabled = true
  result.raw = raw
  if raw == "all":
    result.modesAll = true
  else:
    for tok in raw.split(','):
      let m = tok.strip()
      if m.len == 0: continue
      if m notin chaosModeNames:
        stderr.writeLine "[" & workload & " " & proto & " " & backend &
          "] FAIL: invalid NAVI_CHAOS token '" & m & "' (allowed: " &
          chaosModeNames.join(",") & " or none|all)"
        quit(1)
      result.modes.add m
  result.conc = max(1, getInt("NAVI_CHAOS_CONC", 8))
  result.seed = uint64(getInt("NAVI_CHAOS_SEED", 1))
  result.portBand = getInt("NAVI_CHAOS_PORTBAND", 2000)
  result.watchdog = max(1, getInt("NAVI_CHAOS_WATCHDOG", 60))
  result.fdSlack = getInt("NAVI_CHAOS_FD_SLACK", 8)
  result.heapSlackMb = getInt("NAVI_CHAOS_HEAP_SLACK_MB", 32)
  result.rssSlackMb = getInt("NAVI_CHAOS_RSS_SLACK_MB", 128)
  result.selfTest = getEnv("NAVI_CHAOS_SELFTEST", "").strip()

proc loadConfig*(backend: string): Config =
  ## Read one cell's config. `backend` is the label for this binary.
  result = Config(
    workload: getEnv("NAVI_WORKLOAD", "requests"),
    proto: getEnv("NAVI_PROTO", "h2"),
    backend: backend,
    host: getEnv("NAVI_HOST", "127.0.0.1"),
    basePort: getInt("NAVI_BASE_PORT", 9443),
    servers: max(1, getInt("NAVI_SERVER_COUNT", 5)),
    seconds: getFloat("NAVI_SECONDS", 60.0),
    clients: max(1, getInt("NAVI_CLIENT_COUNT", 3)),
    concurrency: max(1, getInt("NAVI_CONCURRENCY", 8)),
    reqCompression: getEnv("NAVI_REQ_COMPRESSION", "gzip"),
    respCompression: getEnv("NAVI_RESP_COMPRESSION", "gzip"),
    contentTypes: getEnv("NAVI_CONTENT_TYPES", "octet,text,json,form"),
    reportSeconds: max(1, getInt("NAVI_REPORT_SECONDS", 60)),
    streamBytes: getInt("NAVI_STREAM_BYTES", 1073741824),
    cert: getEnv("NAVI_CERT", ""),
    chaos: loadChaos(getEnv("NAVI_WORKLOAD", "requests"),
                     getEnv("NAVI_PROTO", "h2"), backend))
  # js is excluded from chaos entirely: hostile-input handling there is undici's,
  # not navi's; js cells can't enforce the protocol pin; and node FD/heap metrics
  # measure libuv/V8, not this library. Disable it here so every js client is a
  # byte-identical off run and only needs a skipReason-style notice (below).
  if result.backend == "js":
    result.chaos = ChaosConfig(enabled: false)
  # A typo in NAVI_CONTENT_TYPES must not silently narrow (or empty) the rotation:
  # hard-fail at startup, naming the bad token, so the soak never runs miscoverage.
  let (ok, bad) = validContentTypes(result.contentTypes)
  if not ok:
    stderr.writeLine "[" & result.workload & " " & result.proto & " " &
      result.backend & "] FAIL: invalid NAVI_CONTENT_TYPES token '" & bad &
      "' (allowed: octet,text,json,form)"
    quit(1)

proc label*(c: Config): string =
  ## The tag prefixed to every report line, e.g. "[requests h2 chronos]".
  "[" & c.workload & " " & c.proto & " " & c.backend & "]"

proc stressBodies*(): seq[string] =
  ## A rotation of request/response body shapes for the echo workload, so the
  ## buffered path exercises more than one tiny string. Each is fixed content so the
  ## echo can be byte-verified. Covers: empty (Content-Length 0 / END_STREAM), tiny,
  ## small binary, either side of the 16 KiB h2 DATA-frame boundary, past the 64 KiB
  ## initial flow-control window (forces WINDOW_UPDATE), a larger buffered body, and
  ## a highly-compressible body (big compression ratio -> inflate output-buffer path).
  result = @[
    "",                                    # empty
    "payload",                             # tiny text
    "\x00\x01\x02\xfd\xfe\xff binary",     # small binary
    repeat("a", 16383),                    # just under the 16 KiB frame boundary
    repeat("b", 16385),                    # just over it (second DATA frame)
    repeat("c", 65536),                    # past the 64 KiB initial send window
    repeat("z", 262144)]                   # 256 KiB, highly compressible

proc bodiedVerbNames*(): seq[string] = @["POST", "PUT", "PATCH"]

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

proc chaosSkipNotice*(c: Config): string =
  ## Non-empty when NAVI_CHAOS was requested but this cell will not run the chaos
  ## phase, so the client prints it (not a failure). Only js is excluded outright
  ## here; the proto-has-no-applicable-modes skip is decided in chaos.nim where
  ## the schedule is built. Reads the raw env because config.chaos is already
  ## force-disabled for js.
  let raw = getEnv("NAVI_CHAOS", "none").strip()
  if raw.len == 0 or raw == "none": return ""
  if c.backend == "js":
    return "chaos skip: js is excluded (hostile-input handling is undici's, and " &
      "js cells can't pin the protocol or measure navi's FD/heap)"
  ""

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
    # navi/js can't pin undici's HTTP version and the js backend can't verify which
    # was negotiated, so an h1 and an h2 js cell would run identical, unchecked code.
    # Collapse to one js cell (h1) rather than imply protocol coverage we don't have.
    if c.proto != "h1":
      return "skip: js/undici protocol not selectable by navi; the h1 js cell is the js coverage"
  # Sync h3 WebSocket is driven by a background pump thread, so a --threads:off build
  # cannot run it (websocketH3 raises); skip rather than hard-fail the cell.
  when not compileOption("threads"):
    if c.workload == "ws" and c.proto == "h3" and c.backend == "sync":
      return "skip: sync h3 WebSocket needs a --threads:on build"
  ""
