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
    concurrency: max(1, getInt("NAVI_CONCURRENCY", 32)),
    reqCompression: getEnv("NAVI_REQ_COMPRESSION", "gzip"),
    respCompression: getEnv("NAVI_RESP_COMPRESSION", "gzip"),
    reportSeconds: max(1, getInt("NAVI_REPORT_SECONDS", 60)),
    streamBytes: getInt("NAVI_STREAM_BYTES", 1073741824),
    cert: getEnv("NAVI_CERT", ""))

proc label*(c: Config): string =
  ## The tag prefixed to every report line, e.g. "[requests h2 chronos]".
  "[" & c.workload & " " & c.proto & " " & c.backend & "]"

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
  ""
