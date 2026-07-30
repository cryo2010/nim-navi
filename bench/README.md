# HTTP client benchmark

A Dockerized, apples-to-apples benchmark of navi against Nim's stdlib, Go, Rust,
Node.js, and Python, over **TLS + gzip**, exercising **every HTTP method** across
**pooled**, **cold**, and **concurrent** phases.

```
nimble bench                                    # build the image + run (default load)
NAVI_BENCH_ITERS=5000 nimble bench              # heavier pooled load

# or directly:
docker build -f bench/Dockerfile -t navi-bench .
docker run --rm navi-bench
docker run --rm -e NAVI_BENCH_ITERS=8000 -e NAVI_BENCH_COLD_ITERS=1000 \
  -e NAVI_BENCH_CONC=128 -e NAVI_BENCH_CONC_ITERS=8000 navi-bench
```

## What it measures

Each client runs the same workload against one local TLS server — seven requests
per iteration (`GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS`), requesting gzip
and decoding the response — in **three phases**, each printing a table of wall-clock
`REQ/S` (higher is better), sorted with each client relative to the fastest:

- **pooled** (`NAVI_BENCH_ITERS`, default 500): one kept-alive connection reused,
  one request at a time. Steady-state per-request cost; the TLS handshake is paid
  once and amortized to near zero.
- **cold** (`NAVI_BENCH_COLD_ITERS`, default 200): a fresh TCP + TLS connection
  per request (via `Connection: close` / disabled pooling). This is the
  connection-**setup** cost the pooled phase hides — a full handshake every time,
  so it runs fewer iterations.
- **concurrent** (`NAVI_BENCH_CONC` in flight, default 64; `NAVI_BENCH_CONC_ITERS`
  total, default 3000): pooled, but with many requests outstanding at once — each
  client using its idiomatic concurrency (threads for the sync/blocking ones,
  tasks/goroutines/promises for the async ones). Measures sustained throughput
  under load rather than single-request latency. `std/httpclient` is excluded (it
  does not pool, so concurrency would only multiply its handshake storm).

## Clients

| Label | Stack |
| --- | --- |
| `navi-sync` | navi, sync backend |
| `navi-async` | navi, `asyncdispatch` backend |
| `std-sync` | Nim `std/httpclient` (sync) + `zippy` to gunzip |
| `std-async` | Nim `AsyncHttpClient` + `zippy` to gunzip |
| `go` | Go `net/http` |
| `rust` | Rust `reqwest` (blocking) |
| `node` | Node.js built-in `https` + `Agent` (gunzip via `zlib`) |
| `python` | Python `requests` (`Session`) |

## Fairness notes

- **HTTP/1.1 for everyone.** The server disables h2, so all clients are compared
  on the same protocol (`std/httpclient` is h1-only). navi/Go/reqwest also support
  h2; that is a separate comparison.
- **Two sequential phases + one concurrent.** Pooled and cold send one request at
  a time (per-request cost); the concurrent phase keeps `NAVI_BENCH_CONC` in flight
  (sustained throughput). Each client uses its idiomatic concurrency: OS threads for
  the sync/blocking ones (navi-sync, Rust, Python), tasks/goroutines/promises for
  the async ones (navi-async, Go, Node).
- **Compression is equalized.** The server gzips when the client asks. navi, Go,
  reqwest, and Python `requests` decode transparently; `std/httpclient` and Node's
  `https` don't, so they gunzip with `zippy` / `zlib` to do the same work.
- **Verification is off** (self-signed target) purely to avoid per-client CA
  plumbing. The TLS handshake and per-byte symmetric crypto are still exercised;
  verification is a one-time per-connection cost that a pooled run amortizes to
  near zero, so it does not move the pooled numbers.
- Sequential requests (no client-side concurrency), so this measures
  **per-request overhead**, not multiplexing.
- **Nagle is off for everyone** (`TCP_NODELAY`). navi, Go, and reqwest all set it;
  without it, the TLS handshake's final flight plus the first request stall ~40ms
  on the peer's delayed ACK on every fresh connection — which the cold phase makes
  brutally visible. (Adding the cold phase is what surfaced that navi wasn't
  setting it; it now does, on both OpenSSL backends.)

## What you'll see

Indicative figures from one `docker run` (localhost, HTTP/1.1 + gzip), relative to
the fastest client in each phase:

| client | pooled | cold | concurrent (64) |
| --- | --- | --- | --- |
| navi-sync | **100%** | 83% | **100%** |
| navi-async | 94% | **86%** | 48% |
| Go `net/http` | 91% | 39% | 85% |
| Rust `reqwest` | 81% | **100%** | 88% |
| Node `https` | 45% | 48% | 48% |
| Python `requests` | 43% | 31% | 11% |
| `std/httpclient` | 1% | times out | (excluded) |

- **Pooled** (one request at a time): navi is fastest, Go close behind, reqwest a
  step back; the compiled/pooling clients are a few thousand req/s. Node and Python
  land at ~2000 — solid, but carrying VM/interpreter overhead.
- **Cold** (fresh connection per request) is decided by TLS **session resumption**:
  reqwest and navi resume (abbreviated handshake) and stay on top; Node resumes too
  (its `Agent` caches sessions), so it beats Go despite a lower pooled number; Go
  and Python full-handshake every time and fall to the bottom.
- **Concurrent** (64 in flight) is a **multi-core** story: the thread/goroutine
  clients — navi-sync, Rust, Go — scale ~3-4x by spreading TLS + parsing across
  cores. The single-event-loop clients — navi-async and Node — roughly double (one
  core for app logic, I/O overlapped), landing together at ~half the threaded
  clients. Python **does not scale** (the GIL serializes the work; 64 threads
  contend for it). Note this is localhost with ~no network latency, so it rewards
  CPU parallelism; against a latency-bound server the event-loop clients would
  close much of the gap by overlapping the waits.
- **`std/httpclient` is ~90x slower even pooled** (no connection reuse — effectively
  always cold) and is excluded from the concurrent phase.

Numbers depend on the host and its cores/load; Nim is `-d:release`, Rust `--release`,
Go default, Node/Python interpreted. Increase the iteration counts for steadier
numbers on the fast clients.
