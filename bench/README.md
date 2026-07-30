# HTTP client benchmark

A Dockerized, apples-to-apples benchmark of navi against Nim's stdlib and the Go
and Rust clients, over **TLS + gzip**, exercising **every HTTP method**.

```
nimble bench                                    # build the image + run (default load)
NAVI_BENCH_ITERS=5000 nimble bench              # heavier pooled load

# or directly:
docker build -f bench/Dockerfile -t navi-bench .
docker run --rm navi-bench
docker run --rm -e NAVI_BENCH_ITERS=8000 -e NAVI_BENCH_COLD_ITERS=1000 navi-bench
```

## What it measures

Each client runs the same workload against one local TLS server — seven requests
per iteration (`GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS`), requesting gzip
and decoding the response — in **two phases**, each printing a table of wall-clock
`REQ/S` (higher is better), sorted with each client relative to the fastest:

- **pooled** (`NAVI_BENCH_ITERS`, default 500): one kept-alive connection reused
  across requests. Steady-state per-request cost; the TLS handshake is paid once
  and amortized to near zero.
- **cold** (`NAVI_BENCH_COLD_ITERS`, default 200): a fresh TCP + TLS connection
  per request (via `Connection: close` / disabled pooling). This is the
  connection-**setup** cost the pooled phase hides — a full handshake every time,
  so it runs fewer iterations.

## Clients

| Label | Stack |
| --- | --- |
| `navi-sync` | navi, sync backend |
| `navi-async` | navi, `asyncdispatch` backend |
| `std-sync` | Nim `std/httpclient` (sync) + `zippy` to gunzip |
| `std-async` | Nim `AsyncHttpClient` + `zippy` to gunzip |
| `go` | Go `net/http` |
| `rust` | Rust `reqwest` (blocking) |

## Fairness notes

- **HTTP/1.1 for everyone.** The server disables h2, so all six are compared on
  the same protocol (`std/httpclient` is h1-only). navi/Go/reqwest also support
  h2; that is a separate comparison.
- **Compression is equalized.** The server gzips when the client asks. navi, Go,
  and reqwest decode transparently; `std/httpclient` doesn't decompress at all,
  so those two clients gunzip with `zippy` to do the same work.
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

- **Pooled: navi, Go, and Rust are the same order of magnitude** — a few thousand
  req/s, because they all pool the TLS connection and amortize the handshake.
  Run-to-run ordering among these three is within noise; navi is competitive,
  sometimes fastest.
- **Cold: throughput drops to hundreds of req/s** for the pooling clients — that
  gap *is* the connection-setup cost (a full TCP + TLS handshake per request).
  navi lands alongside Go here; Rust's `reqwest`/rustls has the fastest cold
  setup. This phase is where navi's owned transport (connect, handshake) actually
  runs, and it is invisible in the pooled phase.
- **`std/httpclient` is ~50-90x slower even pooled** (tens of req/s): it does not
  reuse the TLS connection here, so it pays a fresh handshake on nearly every
  request — effectively always cold. That is why the default iteration counts are
  modest, and why the std clients typically time out of the cold phase entirely.

Numbers are indicative, not authoritative: they depend on the host and its load
(this is a `docker run` on your machine), and Nim binaries are `-d:release`, Rust
`--release`, Go default (already optimized). Increase `NAVI_BENCH_ITERS` for
steadier numbers on the fast clients.
