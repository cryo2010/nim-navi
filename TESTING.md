# Testing

A registry of every test in navi, what it verifies, and how it runs. navi is an
HTTP **client** with four backends (sync/OpenSSL, asyncdispatch/OpenSSL,
chronos/BearSSL, js/fetch), and the tests are organized so a single sans-io core
is validated once and each backend is validated as a thin adapter over it. Tests
fall into six groups:

1. [Default unit + integration suite](#default-suite-nimble-test) (`nimble test`)
2. [Cross-backend compile checks](#cross-backend-compile-checks)
3. [Interop suites](#interop-suites) (live servers: openssl / nghttpd / Docker)
4. [Memory-safety checks](#memory-safety-checks) (leak / valgrind / sanitizers)
5. [Fuzzing](#fuzzing)
6. [Benchmarks](#benchmarks) (performance, not correctness)

The **CI** column says which check runs it on every PR (see
`.github/workflows/`). "local" means it is not wired into per-PR CI and is run on
demand; "nightly" runs on a schedule. A green PR is **29 checks**: 18 from
`ci.yml`, 10 from `fuzz.yml`, 1 from `badssl.yml` (`live.yml` is nightly only).

## Running at a glance

```sh
nimble test               # default unit + integration suite (orc)
NAVI_MM=arc nimble test   # same suite under the arc memory manager
NAVI_SANITIZE=1 nimble test   # same suite under AddressSanitizer + UBSan

# Interop (each stands up a real server and exits non-zero on failure):
nimble interop            # HTTP/2 vs nghttpd (needs nghttpd + openssl)
nimble mtls               # mutual-TLS client cert (needs openssl)
nimble tlsFallback        # handshake-aware address fallback (needs openssl + python3)
nimble tlsVersion         # TLS min/max version pinning (needs openssl w/ TLS 1.3)
nimble happyEyeballs      # RFC 8305 address racing (needs openssl)
nimble cipherSuite        # cipher / ciphersuite selection (needs openssl w/ TLS 1.3)
nimble servers            # h2 vs nginx / Caddy / h2o (needs Docker + openssl)
nimble httpbin            # full httpbin breadth, 4 backends (needs Docker + openssl)
nimble chronosCafile      # chronos custom-CA verify (needs openssl + chronos)
nimble wsjs               # navi/js WebSocket under Node
nimble jsCookieJar        # navi/js cookie jar under Node
nimble badssl             # cert-verification conformance vs badssl.com (network)
nimble live               # real public servers/CDNs (network; nightly)

# Memory safety:
nimble leak               # 800k-request heap-growth check (per mm)
nimble valgrind           # Valgrind memcheck of the TLS client path (Docker)
nimble leakSanitize       # codec-FFI (zlib/brotli/zstd) leaks under LeakSanitizer

# Fuzzing (Docker libFuzzer) and benchmarks:
nimble fuzz               # coverage-guided libFuzzer over a sans-io target
nimble bench              # client benchmark: navi vs std/httpclient, Go, Rust
```

---

## Default suite (`nimble test`)

Compiles and runs every `tests/test_*.nim`. `nimble test` delegates to
`tests/run.sh` (nimble does not propagate a task's exit code, nim-lang/nimble#1802,
so CI runs the script directly under `set -e`). In CI it runs three times: the
**`test`** job over the memory-manager matrix (`orc` and `arc`) and the
**`sanitize`** job under AddressSanitizer + UBSan (`NAVI_SANITIZE=1`, which also
switches to `-d:useMalloc`). All tests below are covered by those three runs.

Configuration: the repo-root `config.nims` adds `src` to the path and `-d:ssl`
(navi needs OpenSSL for https; library consumers must likewise build with
`-d:ssl`). Shared helper: `tests/support.nim` (in-process TCP/echo server the
end-to-end suites drive).

### Sans-io core (unit — bytes in, response out, no sockets)

| Test | Verifies |
|------|----------|
| `test_h1.nim` | HTTP/1.1 request serialization and the incremental response parser |
| `test_h2_frame.nim` | HTTP/2 frame encode/decode and SETTINGS handling |
| `test_h2_conn.nim` | Sans-io h2 client connection driven by simulated server frames: send/receive flow control, DoS limits, retry classification, max concurrent streams, CONTINUATION on send, frame padding, interim responses + trailers, connection errors, flow control on reset streams |
| `test_h2_hpack.nim` | HPACK decode/encode against RFC 7541 Appendix C; malformed input rejected without crashing; DoS bounds enforced |
| `test_h2_hpack_corpus.nim` | HPACK decoder conformance against the http2jp/hpack-test-case corpus (multiple independent encoders, shared dynamic table) |
| `test_h2_huffman.nim` | HPACK Huffman decode/encode against RFC 7541 Appendix C.4/C.6 vectors |
| `test_ws.nim` | Sans-io WebSocket: RFC 6455 handshake, frame codec, close handshake, message assembly, and a client end-to-end pass |

### Entry modules (end-to-end against an in-process server)

| Test | Verifies |
|------|----------|
| `test_entries.nim` | Sync entry end to end, plus config wiring: TLS session resumption, per-phase timeouts (connect/read/total), TLS version pinning, and TLS cipher selection (incl. an unusable `ciphers`/`cipherSuites` raising rather than being ignored) |
| `test_async.nim` | asyncdispatch entry end to end |
| `test_chronos.nim` | chronos entry end to end, plus the chronos TLS config guard (BearSSL rejects cipher selection up front) |

### Features

| Test | Verifies |
|------|----------|
| `test_cookies.nim` | Cookie jar expiry (Max-Age and Expires) and domain/path matching (RFC 6265) |
| `test_digest.nim` | Digest auth: the pure computation (RFC 2617 vector) and the 401-challenge/retry flow end to end |
| `test_stream_decompress.nim` | Streaming-response decompression: the incremental decoder fed across chunk boundaries, the `stream()` path decoding a body, and stacked `Content-Encoding` |

### WebSocket client adapters

| Test | Verifies |
|------|----------|
| `test_ws_async.nim` | Async WebSocket client (navi/asyncdispatch) against an in-process echo server built from the same sans-io core |
| `test_ws_chronos.nim` | chronos WebSocket client (navi/chronos) against the same in-process echo server |

---

## Cross-backend compile checks

navi's four entries are guarded to different backends, so each must be built the
way its consumers will build it. These jobs are compile-only unless noted.

| Check | CI | Verifies |
|------|----|----------|
| `compile` job (`nim check`, matrix orc/arc) | **yes** (`compile entries`) | `navi`, `navi/asyncdispatch`, and `navi/chronos` each type-check with `-d:ssl` (the OpenSSL/ALPN path the plain-TCP unit suite never builds) under both memory managers |
| `nim js src/navi/js.nim` | **yes** (`compile navi/js`) | The js entry compiles under the JavaScript backend (the only job that builds it) |
| `tests/js_fallback_{async,chronos}.nim` | **yes** (`compile navi/js`) | A library written on navi/asyncdispatch or navi/chronos also builds under `nim js`, where the entry falls back to navi/js (portable middleware/config/params) |
| `tests/interop/jsws.sh` | **yes** (`compile navi/js`) | navi/js WebSocket client runs under Node 22+ (global WebSocket) against a native echo server |
| `tests/interop/js_cookiejar.sh` | **yes** (`compile navi/js`) | navi/js opt-in cookie jar replays a cookie across requests under Node (undici has no cookie store), and the default does not |

---

## Interop suites

Each stands up a real server (openssl `s_server`, the nghttp2 reference, or
containers) and runs navi's client against it, exiting non-zero on failure. The
per-PR ones live in the **`interop`**, **`multiserver`**, and **`httpbin`** CI
jobs; the `interop` job runs the first seven rows below as sequential steps under
`set -e`, so any one failing turns the single `nghttpd interop (http/2)` check
red (read the job log for the failing step). File streaming additionally has its
own **`streaming`** matrix job (four separate checks) — see the row below and the
[File streaming](#file-streaming) coverage summary.

| Suite (script → driver) | CI | Verifies |
|------|----|----------|
| `run.sh` → `nghttpd_{sync,async}.nim` | **yes** (`interop`) | HTTP/2 against nghttpd (nghttp2 reference): navi's HPACK **encoder**, ALPN, real h2 wire framing, multiplexing, receive-side flow control, PADDED-flag handling (a second nghttpd runs with `-b` padding), and a streamed `bodyStream` upload over h2 (sync and the async mux) |
| `mtls.sh` → `mtls.nim` | **yes** (`interop`) | Mutual TLS: an `openssl s_server -Verify 1` rejects clients without a CA-signed cert, exercising `TlsConfig.certFile`/`keyFile` |
| `tls_fallback.sh` → `tls_fallback.nim` | **yes** (`interop`) | Handshake-aware address fallback (sync): a dead endpoint (accepts TCP then drops the handshake) plus a good TLS server on the same port; navi falls through to the good address |
| `tls_version.sh` → `tls_version.nim` | **yes** (`interop`) | TLS version pinning: TLS-1.2-only and TLS-1.3-only servers; a `minVersion`/`maxVersion` pin excluding the server's version fails the handshake |
| `happy_eyeballs.sh` → `happy_eyeballs.nim` | **yes** (`interop`) | Happy Eyeballs (RFC 8305): a blackholed first address (192.0.2.1, SYN dropped) plus a good server; navi races the addresses and reaches the good one in ~the attempt delay instead of stalling |
| `cipher_suite.sh` → `cipher_suite.nim` | **yes** (`interop`) | Cipher selection: servers pinned to one TLS 1.2 cipher and one TLS 1.3 ciphersuite; `TlsConfig.ciphers`/`cipherSuites` honored (matching name connects, non-matching fails the handshake) |
| `ca_verify.sh` → `ca_verify.nim` | **yes** (`interop`) | Private-CA verification (sync): a server cert signed by a throwaway CA; navi trusts it via `TlsConfig.caFile` and rejects the same server without the CA (system trust lacks that root) |
| `streaming.sh` → `streaming_client.nim` (+ `streaming_server.nim` for h1) | **yes** (`file streaming …`, 4 checks) | File streaming (sync) as a matrix of protocol × direction: for http/1.1 (a local Nim server) and http/2 (nghttpd), upload via `bodyStream` and download via `stream()`/`each`. Each check asserts the transfer used that protocol (`res.httpVersion`) and the bytes hash-match a 3 MiB original |
| `servers.sh` → `servers_{sync,async}.nim` | **yes** (`multiserver`) | h2 client against three unrelated stacks (nginx, Caddy/Go, h2o) over TLS via docker compose, plus the chronos h1+TLS leg; ALPN negotiation and a 256 KiB body (receive flow control) |
| `httpbin.sh` → `httpbin_test.nim`, `httpbin_js.nim` | **yes** (`httpbin`) | Full httpbin breadth (every method, bodies, auth, redirects, decompression, cookies) behind Caddy (TLS+h2) across all four backends; also streaming download via `stream()`/`each` on all four and `bodyStream` upload on the native backends (buffered on js); offline (never published to the host) |
| `badssl.nim` (`badssl.yml`) | **yes** (`badssl TLS conformance`) | Certificate-verification conformance: navi rejects invalid server certs with verification on (the default) and accepts a valid one. Hits badssl.com (network) |
| `chronos_cafile.sh` → `chronos_cafile.nim` | local | Custom-CA verification for chronos/BearSSL (`TlsConfig.caFile`): a server cert signed by a private CA is verified against that CA (uses a dNSName SAN, which BearSSL matches) |
| `live.nim` (`live.yml`) | nightly | Real public servers/CDNs (Google, Cloudflare, …) to catch h2/TLS bugs only independent stacks provoke. Network; never a per-PR gate |

### File streaming

Streaming is verified per **backend × direction**, always by hashing the transfer
against the original. Upload uses a pull-based `bodyStream` producer; download uses
the `stream()` handle: `stream(url)` returns a headers-first `StreamResponse`, and
`each`/`drain` pull the body chunk by chunk.

The download `chunk` is per backend: `string` on the native backends (sync,
asyncdispatch, chronos), moved out of navi's read buffer with no copy, and
`seq[byte]` on js (its bytes come from a JS Uint8Array). The consumer is awaited on
the async backends, so a slow consumer applies cooperative backpressure: over h2
the stream's receive window is only replenished (`ackRecv`) after each chunk is
taken, so the peer stalls that one stream without blocking the mux reader or the
other multiplexed streams; over h1 the awaited consumer pauses the read loop. The
`nghttpd_async` interop asserts a 256 KiB body reaches `each` in **more than one
call** (incremental, not buffered whole) and that the mux heap stays flat across
5000 requests (no leak or deadlock in the drain path). Handle lifetime is covered
per backend: a full drain returns the connection to the pool (and it is reused),
and a failed drain closes rather than pools it.

| | `navi` (sync) | `navi/asyncdispatch` | `navi/chronos` | `navi/js` |
| --- | :---: | :---: | :---: | :---: |
| Download (`stream`/`each`) | ✓ | ✓ | ✓ | ✓ |
| Upload (`bodyStream`) | ✓ | ✓ | ✓ | ✓ buffered |

Where each is exercised:

- **Dedicated `streaming` job** (4 checks) — sync, both directions, over http/1.1
  and http/2, asserting the protocol and a 3 MiB hash match (`streaming.sh`).
- **httpbin job** — download via `stream()`/`each` on all four backends;
  `bodyStream` upload on the three native backends, and buffered on js
  (`httpbin_test.nim` builds for sync/async/chronos, `httpbin_js.nim` for js).
- **nghttpd `interop` job** — streamed `bodyStream` upload over real h2 on the
  sync backend and the async mux, plus the incremental `each` drain over the mux.
- **Backpressure (sans-io, `test_h2_conn`)** — the flow-control gating an awaited
  `each`/`drain` consumer relies on: a `sinkMode` stream holds its stream
  `WINDOW_UPDATE` past the replenish threshold until `ackRecv` releases it (a normal
  stream replenishes eagerly, as the control case), so a slow consumer stalls the
  peer. Also covers incremental `takeBody` drain and the running-total size cap.
- **Unit** — `test_entries`/`test_async`/`test_chronos` cover headers-first,
  full-drain-pools-and-reuses, failed-drain-closes, and an incremental cap;
  `test_stream_decompress` decodes a streamed body through `each`.

`navi/js` **buffers** `bodyStream` (drains the producer, then sends one body):
`fetch` cannot reliably stream a request body. See the backend matrix in the
README.

---

## Memory-safety checks

navi holds C resources (OpenSSL `SSL_CTX`, zlib/brotli/zstd contexts), so leaks
need more than a Nim-heap check. Each runs under both memory managers where a
reference cycle could differ (`arc` does not collect cycles).

| Check | CI | Verifies |
|------|----|----------|
| `sanitize` job → `tests/run.sh` | **yes** (`sanitizers`) | The whole default suite rebuilt under AddressSanitizer + UBSan (`-d:useMalloc`): heap overflows, use-after-free, and UB in the sans-io parsers a passing test would hide. Leak detection off (short-lived test processes leak by design) |
| `leak.nim` (matrix orc/arc) | **yes** (`leak check`) | Every verb + `request` in a 100,000× loop (800k requests) against an in-process keep-alive server; asserts the Nim heap stays flat (an orc/arc gap would mean a reference cycle) |
| `leak_valgrind.nim` (matrix orc/arc, Docker) | **yes** (`valgrind leak check`) | navi's HTTPS request loop under Valgrind memcheck; fails on any definite/indirect leak (e.g. a per-connection `SSL_CTX` that `getOccupiedMem` and LeakSanitizer miss) |
| `leak_sanitize.nim` | **yes** (`leak check (codec FFI…)`) | Codec-FFI leaks LeakSanitizer sees but `getOccupiedMem` cannot: zlib / libbrotlidec / libzstd contexts a dropped `=destroy`/defer would leak (`detect_leaks=1`) |

---

## Fuzzing

Coverage-guided libFuzzer over navi's sans-io decoders. `fuzz.yml` runs two jobs
per PR, each a matrix over five targets (`hpack`, `h1`, `frame`, `huffman`,
`h2conn`) — 10 checks total. Targets live in `tests/fuzz/`.

| Task | CI | Verifies |
|------|----|----------|
| `tests/fuzz/run.sh <target> replay` | **yes** (`replay <target>`, ×5) | Replays the committed seed corpus (`tests/fuzz/seeds/`) under ASan/UBSan — fast, portable, deterministic, any compiler; a regression on a known input fails |
| `tests/fuzz/run.sh <target> 120` | **yes** (`fuzz <target>`, ×5) | 120s coverage-guided libFuzzer run over each target (HTTP/1.1 parser, HPACK, h2 frame, Huffman, h2 connection); a crash uploads a reproducer and exits non-zero |
| `nimble fuzz` | local | The same targets via Docker libFuzzer for on-demand local runs |

---

## Benchmarks

Performance measurement, not pass/fail (CI does not gate on throughput).

| Task | Verifies |
|------|----------|
| `nimble bench` | Dockerized HTTP-client benchmark: navi vs `std/httpclient`, Go, and Rust |
| `nimble stress` | Dockerized backend stress test exercising every backend (sync, asyncdispatch, chronos, js) over TLS + WebSockets with middleware and multiple clients |
