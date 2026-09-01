# navi benchmarks

Focused, Dockerized, cross-language benchmarks split by **workload** (mirroring
`tests/stress/`), with protocol, backend, server count, and runtime as configurable
`NAVI_*` dimensions. Each cell runs every applicable client against N fast Go TLS
servers and prints one ranked table of **throughput + latency percentiles** per
`(workload, protocol)`: navi's four backends (sync / asyncdispatch / chronos / js)
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
| `nimble bench` | short smoke of all five | — |

## Configuration (env)

| Var | Default | Meaning |
| --- | --- | --- |
| `NAVI_PROTO` | `h2` | `h1` \| `h2` \| `h3` \| `all` (h3 is navi-only + needs the h3 image) |
| `NAVI_BACKEND` | `all` | navi backends: `sync` \| `asyncdispatch` \| `chronos` \| `js` \| `all` |
| `NAVI_LANGS` | `all` | reference langs to include: `all` \| `navi` \| `go` \| `rust` \| `node` \| `python` \| `std` (csv) |
| `NAVI_SERVERS` | `5` | fast Go server instances; clients round-robin across them |
| `NAVI_SECONDS` | `20` | measured window per cell |
| `NAVI_WARMUP_SECONDS` | `2` | unmeasured warmup before the window |
| `NAVI_MODE` | `pooled` | `pooled` (reuse connections) \| `cold` (fresh connection per request) |
| `NAVI_CLIENTS` | `3` | clients per backend |
| `NAVI_CONCURRENCY` | `8` | in-flight ops per client (fan-out width) |
| `NAVI_STREAM_BYTES` | `1073741824` | bytes per streaming transfer (1 GiB; lower for a smoke) |
| `NAVI_NETEM` | `0` | `1` adds a lossy-link regime (`tc netem`; needs `--cap-add=NET_ADMIN`, added automatically) |
| `NAVI_NETEM_DELAY` / `NAVI_NETEM_LOSS` | `25ms` / `1.5%` | netem link parameters |

## Coverage

- **h3 is navi-only** — Go/Rust/Node/Python have no stable HTTP/3 client, so they
  skip h3 cells (printed, not silent). h3 is fronted by Caddy (Alt-Svc) like stress.
- **std/httpclient** is requests + h1 only; **js** cannot stream uploads and has no h3.
- **WebSocket** is an h1 upgrade, so `benchWs` runs h1 only across all languages.

## Notes

- Not in CI (Docker + h3 toolchain + multi-minute runs are too heavy). Run manually.
- The `h3` numbers include the Caddy proxy hop and compare navi backends only.
- nimble does not propagate a task's exit code (nim-lang/nimble#1802); read the
  `== <workload>: all cells ran ==` banner / the docker exit code for pass/fail.

## Examples

```
NAVI_PROTO=h2 NAVI_SECONDS=20 nimble benchRequests          # h2 requests, all languages
NAVI_LANGS=navi NAVI_PROTO=all nimble benchRequests         # navi backends only, h1/h2/h3
NAVI_STREAM_BYTES=$((64*1024*1024)) nimble benchStreamDownload
NAVI_NETEM=1 NAVI_PROTO=all nimble benchRequests            # lossy link: h3 vs h2
nimble bench                                                # smoke all five (10s cells)
```
