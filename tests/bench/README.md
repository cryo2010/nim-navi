# navi benchmarks

Focused, Dockerized, cross-language benchmarks split by **workload** (mirroring
`tests/stress/`), with protocol, client, server count, and runtime as configurable
`NAVI_*` dimensions. Each cell runs every applicable client against N fast Go TLS
servers and prints one ranked table of **throughput + latency percentiles** per
`(workload, protocol)`: navi's four clients (sync / asyncdispatch / chronos / js)
alongside Go, Rust, Node, Python, and Nim `std/httpclient` reference clients.

Clients are **time-boxed** (`NAVI_SECONDS`) after an unmeasured warmup and record
per-operation latency into a shared log-bucketed histogram (`bucket = floor(log2(us)
* 64)`), so p50/p99/p999 are comparable across languages. The streaming workloads
verify a SHA-1 and fail hard on mismatch.

## Tasks

| Task | Workload | Metric |
| --- | --- | --- |
| `nimble benchRequests` | buffered GET/POST/PUT at /echo (gzip) | req/s + latency |
| `nimble benchWs` | WebSocket text echo round-trips (h1) | round-trips/s + latency |
| `nimble benchSse` | SSE event consumption | events/s + latency |
| `nimble benchStreamUpload` | stream up; SHA-1 verified | MB/s + latency |
| `nimble benchStreamDownload` | stream down; SHA-1 verified | MB/s + latency |
| `nimble bench` | short smoke of all five | n/a |

## Configuration (env)

| Var | Default | Meaning |
| --- | --- | --- |
| `NAVI_PROTO` | `h2` | `h1` \| `h2` \| `h3` \| `all` (h3 is navi-only + needs the h3 image) |
| `NAVI_CLIENT` | `all` | navi client: `sync` \| `asyncdispatch` \| `chronos` \| `js` \| `all` |
| `NAVI_THREADS` | (cores) | navi native clients run this many client THREADS in one process (one event loop per thread; one navi client per thread; total concurrency split across them; throughput merged in-process). Set `1` for single-thread. (`NAVI_PROCS` is a legacy alias.) |
| `NAVI_LANGS` | `all` | reference langs to include: `all` \| `navi` \| `go` \| `rust` \| `node` \| `python` \| `std` (csv) |
| `NAVI_SERVER_COUNT` | `5` | fast Go server instances; clients round-robin across them |
| `NAVI_SECONDS` | `20` | measured window per cell |
| `NAVI_WARMUP_SECONDS` | `2` | unmeasured warmup before the window |
| `NAVI_MODE` | `pooled` | `pooled` (reuse connections) \| `cold` (fresh connection per request) |
| `NAVI_CLIENT_COUNT` | `3` | concurrent navi client instances per cell |
| `NAVI_CONCURRENCY` | `8` | in-flight ops per client (fan-out width) |
| `NAVI_REQ_COMPRESSION` | `none` | request body encoding: `none` \| `gzip` \| `deflate` (navi native only). Off by default: no reference client gzips its request body |
| `NAVI_RESP_COMPRESSION` | `gzip` | response encoding navi asks for via `x-want-encoding`: `none` \| `gzip` \| `deflate` \| `br` \| `zstd`. The server gzips for every client (they all send `Accept-Encoding: gzip`), so this stays on |
| `NAVI_STREAM_BYTES` | `1073741824` | bytes per streaming transfer (1 GiB; lower for a smoke) |
| `NAVI_NETEM` | `0` | `1` adds a lossy-link regime (`tc netem`; needs `--cap-add=NET_ADMIN`, added automatically) |
| `NAVI_NETEM_DELAY` / `NAVI_NETEM_LOSS` | `25ms` / `1.5%` | netem link parameters |

## Coverage

- **h3 is navi-only** -- Go/Rust/Node/Python have no stable HTTP/3 client, so they
  skip h3 cells (printed, not silent). h3 is fronted by Caddy (Alt-Svc) like stress.
- **std/httpclient** is requests + h1 only; **js** cannot stream uploads and has no h3.
- **WebSocket** is an h1 upgrade, so `benchWs` runs h1 only across all languages.

## Fair comparison

Every row in a cell has to do the same work and be measured the same way, or the
ranking measures the harness instead of the clients. What keeps that true:

- **Multi-core:** `NAVI_THREADS` (default = cores) runs one navi client per thread in a
  single process (one event loop per thread) and merges their throughput: a
  single-process, all-cores comparison, matching how Go/Rust use every core in one
  process. Set `NAVI_THREADS=1` to measure single-core (per-event-loop) efficiency.
  The clients build under plain `orc`: navi's shared process-globals were hardened for
  the one-client-per-thread model (the HPACK Huffman table is a `const` flat table;
  the codec/TLS lazy-loader state is `{.threadvar.}`), so no atomic refcounting is
  needed. `--threads:on` is required for the in-process threads.
- **Hardware hash:** the streaming clients verify integrity with OpenSSL's SHA-1
  (SHA-NI), matching Go/Rust/Node. Nim's software `checksums/sha1` (~0.8 GB/s) would
  otherwise bottleneck navi's core and understate its download throughput.
- **The same request:** `NAVI_REQ_COMPRESSION` defaults to `none`, because only navi's
  native clients honored it, so navi alone paid a `deflate` (and the origin an
  `inflate`) per POST/PUT while every other row sent the body plain. Every client now
  sends the same 9-byte `payload-x`. Set the knob explicitly to measure request
  compression. Response gzip stays on for everyone: they all send
  `Accept-Encoding: gzip` and all inflate the ~8 KiB reply.
- **The same measured window:** every client divides ops and bytes by the REAL elapsed
  window, not the nominal `NAVI_SECONDS`. All of them keep a unit that started before
  the deadline and finished after it, so that overshoot belongs in the denominator;
  charging it to some rows and not others inflated them, by ~30% for a streaming cell
  where one transfer straddles the deadline.
- **The same offered load:** the native runner splits `NAVI_CLIENT_COUNT *
  NAVI_CONCURRENCY` across its threads so the per-thread widths sum to exactly that
  total, which is what every reference client drives. A per-thread ceil used to hand
  navi up to `threads - 1` extra in-flight workers (30 on 10 cores, 32 on 16, 40 on 20
  against the default 24), raising its throughput and worsening its p99.
- **The same TLS work:** every client verifies the origin's certificate against the
  harness CA in `NAVI_CERT`, as navi does by default. Skipping it (the reference
  clients all used to) saves the chain build, the hostname match and, on the rustls
  path, two RSA verifies per connection. run.sh therefore signs the servers' leaf with
  a throwaway CA rather than serving one self-signed cert as both anchor and leaf,
  which rustls rejects outright (`CaUsedAsEndEntity`).
- **The same upload framing:** all five stream `/upload` chunked. navi cannot do
  otherwise (its h1 writer always chunks a producer body and drops a caller's
  `Content-Length`), and Go, Rust and Node used to send a length-delimited body, which
  the origin reads more cheaply.

Where a cell is *not* comparable, and no knob fixes it:

- **Cores.** The `CORES` column says how many each row can use. `nim/navi-js`,
  `js/node`, `python/httpx` and both `nim/std-*` rows are one event loop on one core;
  the rest use `NAVI_THREADS`. Read `REQ/S` down the column with that in mind.
- **`nim/navi-sync` offers less load.** A blocking client runs one sequential request
  per thread, so it drives `NAVI_THREADS` in-flight requests rather than the cell's
  `clients * concurrency`. That understates it, and is inherent to the client.
- **`MB/s` in the `requests` cell.** Every row reports 0 there: the unit is a request,
  so the column only carries meaning in the streaming, SSE and ws cells.
- **Coverage gaps** (see above): h3 is navi-only, `std/httpclient` is h1 + requests
  only, `js` cannot stream uploads, and `js/node` skips `streamUpload` on h2.

The throughput numbers this harness printed before these fixes are not comparable with
what it prints now, so any previously quoted figure needs re-measuring.

## Notes

- Not in CI (Docker + h3 toolchain + multi-minute runs are too heavy). Run manually.
- The `h3` numbers include the Caddy proxy hop and compare navi clients only.
- nimble does not propagate a task's exit code (nim-lang/nimble#1802); read the
  `== <workload>: all cells ran ==` banner / the docker exit code for pass/fail.

## Examples

```
NAVI_PROTO=h2 NAVI_SECONDS=20 nimble benchRequests          # h2 requests, all languages
NAVI_LANGS=navi NAVI_PROTO=all nimble benchRequests         # navi clients only, h1/h2/h3
NAVI_STREAM_BYTES=$((64*1024*1024)) nimble benchStreamDownload
NAVI_NETEM=1 NAVI_PROTO=all nimble benchRequests            # lossy link: h3 vs h2
nimble bench                                                # smoke all five (10s cells)
```
