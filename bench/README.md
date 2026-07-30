# HTTP client benchmark

A Dockerized, apples-to-apples benchmark of navi against Nim's stdlib and the Go
and Rust clients, over **TLS + gzip**, exercising **every HTTP method**.

```
docker build -f bench/Dockerfile -t navi-bench .
docker run --rm navi-bench                      # 3000 iterations (default)
docker run --rm -e NAVI_BENCH_ITERS=8000 navi-bench
```

## What it measures

Each client runs the same workload against one local TLS server: a warmup, then
`NAVI_BENCH_ITERS` iterations of seven requests each — `GET, POST, PUT, PATCH,
DELETE, HEAD, OPTIONS` — over a **pooled** connection, requesting gzip and
decoding the response. The result is wall-clock `REQ/S` (higher is better),
sorted, with each client shown relative to the fastest.

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
  near zero, so it does not move the numbers.
- Sequential requests (no client-side concurrency), so this measures
  **per-request overhead**, not multiplexing.

## What you'll see

- **navi, Go, and Rust are the same order of magnitude** — a few thousand req/s,
  because they all pool the TLS connection and amortize the handshake. Run-to-run
  ordering among these three is within noise; navi is competitive, sometimes
  fastest.
- **`std/httpclient` is ~50-90x slower** (tens of req/s): it does not reuse the
  TLS connection here, so it pays a fresh handshake on nearly every request. That
  is the single biggest finding, and it is why the default iteration count is
  modest -- otherwise the two std clients dominate the wall-clock time.

Numbers are indicative, not authoritative: they depend on the host and its load
(this is a `docker run` on your machine), and Nim binaries are `-d:release`, Rust
`--release`, Go default (already optimized). Increase `NAVI_BENCH_ITERS` for
steadier numbers on the fast clients.
