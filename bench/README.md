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

## navi protocol matrix (HTTP/1.1 vs HTTP/2 vs HTTP/3)

After the cross-language phases, a navi-only matrix runs each protocol cold and
pooled against one **Caddy** origin that speaks all three, so h1/h2/h3 are
compared on the same server (GET-only — connection and protocol overhead, not the
seven-method body workload). It uses the `asyncdispatch` backend built with
`-d:naviHttp3`:

| PROTOCOL | MODE | REQ/S (higher is better) |
| --- | --- | --- |
| h1 / h2 / h3 | pooled | steady-state, connections reused (h1 keep-alive, h2/h3 mux) |
| h1 / h2 / h3 | cold | fresh connection + handshake per request |

**h3 cold is not a pure QUIC dial.** HTTP/3 is only reached after an h1/h2
response advertises `Alt-Svc: h3` (navi has no way to pre-seed that), so a cold h3
request first does an h1/h2 discovery round trip, then the QUIC handshake — the
matrix labels this honestly and it explains why `h3 / cold` is the slowest cell.
The matrix is skipped if the h3 toolchain or Caddy is unavailable.

### Where h3 beats h2 (`NAVI_BENCH_NETEM=1`)

On a clean loopback, h3 has no advantage to show and pays its overhead (userspace
QUIC, per-packet crypto), so it roughly ties h2 sequentially. h3's win appears
when the **network** is the bottleneck and requests are **concurrent**: over a
lossy link, h2's single connection suffers TCP head-of-line blocking across its
streams (one lost packet stalls them all), while h3's streams are independent.

```
NAVI_BENCH_NETEM=1 nimble bench          # adds a second matrix under tc netem
```

This re-runs the matrix with concurrency (`NAVI_BENCH_CONC`, default 16) under
`tc qdisc ... netem delay <NAVI_BENCH_NETEM_DELAY> loss <NAVI_BENCH_NETEM_LOSS>`
on loopback (defaults 25ms each way, 1.5% loss). It needs `tc` (iproute2, in the
image) and the `NET_ADMIN` capability (the `nimble bench` task grants it via
`--cap-add=NET_ADMIN`). In this regime **h3 pooled pulls ahead of h2 pooled**
(observed ~1.6x at 24 concurrent), the head-of-line-blocking win. It is skipped
cleanly if the capability is unavailable.

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
- **Sequential, one request at a time.** This measures per-request cost, not
  concurrent throughput. The interpreted async clients (Node, Python) are built
  for concurrency, which a sequential loop does not exercise -- with many in-flight
  requests (undici, `aiohttp`) they would fare better than they do here.
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

Indicative figures from one `docker run` (sequential, localhost, HTTP/1.1 + gzip),
relative to the fastest client in each phase:

| client | pooled | cold |
| --- | --- | --- |
| navi (sync / async) | **100%** | 86% |
| Go `net/http` | 90% | 41% |
| Rust `reqwest` | 80% | **100%** |
| Node `https` | 44% | 52% |
| Python `requests` | 44% | 33% |
| `std/httpclient` | 1% | times out |

- **Pooled: navi is fastest**, with Go close behind and reqwest a step back; all
  four compiled/pooling clients are a few thousand req/s. Node and Python land at
  ~2000 req/s — solid, but carrying VM/interpreter overhead. Run-to-run ordering
  among navi/Go/reqwest is within noise.
- **Cold** (fresh connection per request) is where TLS **session resumption**
  decides it: reqwest and navi resume (abbreviated handshake) and stay on top;
  Node resumes too (its `Agent` caches sessions), so it beats Go despite a lower
  pooled number; Go and Python do a full handshake every time and fall to the
  bottom. This is the phase navi's owned transport + resumption targets, and it is
  invisible in the pooled phase.
- **`std/httpclient` is ~90x slower even pooled**: it does not reuse the TLS
  connection here, so it is effectively always cold, and times out of the cold
  phase entirely.

Numbers depend on the host and its load; Nim is `-d:release`, Rust `--release`,
Go default, Node/Python interpreted. Increase `NAVI_BENCH_ITERS` for steadier
numbers on the fast clients.
