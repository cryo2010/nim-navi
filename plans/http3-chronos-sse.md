# HTTP/3 for navi/chronos, and streaming/SSE over HTTP/3

## Context (correcting the premise)

The ask was "add H3 SSE support for navi/chronos." Investigation shows the goal
needs more than a port, because two assumptions don't hold:

1. **SSE-over-h3 works on no backend today.** navi's h3 is **buffered
   `request()`-only**. The C driver (`h3client.cpp`) accumulates the whole
   response body (`Stream.body.append` in `on_recv_data`) and hands it over in one
   shot via `navi_h3_take_response`; there is no incremental read. `h3TransportAsync`
   only hooks the buffered `transport()` path — `openStreamConn`/`stream()` has **no
   h3 branch**. So `stream()`/`sse()` silently fall back to h2/h1. (An earlier "h3
   SSE" stress run got events because the SSE stream rode **h2** through Caddy, not
   h3.) `docs/http3.md` itself lists streaming/SSE over h3 as conditional future
   work ("as long as `h3mux`/`h3glue` surface the same `StreamResponse` handle").

2. **chronos h3 is now feasible** (the docs say otherwise, but they're stale).
   `docs/http3.md` says "chronos is BearSSL; ngtcp2 needs OpenSSL, so no h3."
   Since then the chronos backend was moved to **OpenSSL over its transport** (the
   memory-BIO pump in `chronos_tls.nim`), so ngtcp2's OpenSSL-3.5 requirement is
   satisfied. chronos also has UDP (`chronos/transports/datagram.nim`) and
   low-level fd readiness. So the two historical blockers are gone.

Therefore the real work is three pieces, sequenced so each ships value and the
hard parts are proven on the reference backend (asyncdispatch) before chronos:

- **A. h3 streaming layer** (incremental-read C API + an `h3glue` that surfaces
  navi's `StreamResponse`), wired into `stream()`/`sse()` on asyncdispatch. This
  makes SSE-over-h3 real and testable on the reference backend. *Prerequisite for
  the actual goal; missing on every backend today.*
- **B. `quic_chronos.nim`** — a chronos-native QUIC driver (the twin of
  `quic_async.nim`), giving chronos buffered `request()` h3 parity.
- **C. Wire the h3 streaming layer (from A) into chronos**, so SSE/stream() ride
  h3 on chronos too.

Recommended order: **A → B → C**. A is the riskiest/most valuable and validates
the streaming design on the mature backend; B is a mechanical event-loop port; C
reuses A.

## Reusable as-is (no change)

- `src/navi/backend/quic.nim` FFI + `h3client.cpp` step-function core
  (`navi_h3_new/fd/send/recv/timeout_ms/handle_timeout/handshake_done/bind/submit/
  stream_done/stream_reset/take_response/close`) — event-loop-agnostic.
- `src/navi/core/altsvc.nim` — Alt-Svc parse/record/`h3Endpoint`, backend-agnostic.
- `src/navi/backend/openssl_ctx.nim` — TLS context/verify, already shared.
- `Http3Response`, `QuicError` types.

---

## Phase A — Streaming/SSE over h3 (on asyncdispatch first)

### A1. Incremental-read C/FFI API (`h3client.cpp` + `quic.nim`)
The driver already buffers `Stream.body` incrementally via nghttp3's `on_recv_data`.
Add a non-buffering drain so navi can pull chunks as they land:

- `navi_h3_read_body(c, sid, buf, cap, outEof) -> ssize` — copy up to `cap` bytes
  of stream `sid`'s currently-accumulated body into `buf`, erase them from the
  internal `std::string`, and set `outEof` when the stream has ended and the buffer
  is drained. Returns bytes copied (0 = nothing available yet, not EOF).
- `navi_h3_response_headers(c, sid, ...)` — take just the status + headers once
  they've arrived (nghttp3 `on_recv_header`/end-of-headers), so `stream()` can
  return a headers-first handle before the body is done.
- Keep `take_response` for the buffered path (unchanged).
- Add the matching `{.importc.}` decls in `quic.nim`.

Backpressure: nghttp3/QUIC stream flow control replenishes as the C buffer is
drained; navi's awaited `each`/`drain` per-chunk pull gates it (mirrors the h2
window model).

### A2. `h3glue` — surface a `StreamResponse` over h3
New `src/navi/backend/h3glue.nim` (the h3 twin of `h2glue.nim`): given a
`QuicConnAsync` + submitted `sid`, produce the same headers-first handle the h1/h2
paths return, whose chunk-pull calls `navi_h3_read_body` and whose completion maps
QUIC stream-close/reset to navi's EOF/`UnprocessedError`. The reader loop already
completes a per-stream future on `stream_done`; extend it to also wake a
`recvReady[sid]`-style future when new body bytes arrive (so a parked chunk pull
wakes), exactly like the h2 mux's `recvReady`.

### A3. Wire `stream()`/SSE h3 path (`asyncdispatch_impl.nim`)
- Add an h3 branch to `openStreamConn` mirroring `h3TransportAsync`: if h3 is
  enabled, TLS, and Alt-Svc has an endpoint, submit the request on the shared
  `QuicConnAsync` and return the `h3glue` `StreamResponse` (readChunk pulls body
  incrementally). Redirect/digest/close semantics match the h2 branch.
- `sse()`/`stream()` need **no changes** — they ride `openStreamConn`.
- Streamed **request** bodies (`bodyStream`) over h3 can stay out of scope for A
  (map to nghttp3 `read_body` later); document it, like today.

### A4. Tests
`tests/interop/http3/` gains a streaming case: `stream_async_test.nim` (SSE +
`stream()` download over h3 against Caddy), asserting `res.httpVersion == "HTTP/3"`
and body integrity. Reuse the existing Caddy h3 origin/harness.

---

## Phase B — `quic_chronos.nim` (chronos QUIC driver)

Port `quic_async.nim` to chronos. Same shape (`QuicConnChronos` with `c`, fd,
`waiters`, `wakeup`, `alive`, `readerDone`), different event-loop primitives:

| asyncdispatch (`quic_async.nim`) | chronos equivalent |
| --- | --- |
| `AsyncFD` + `register`/`unregister` | chronos `AsyncFD` register, or a `DatagramTransport` wrapping the driver's UDP fd |
| `addRead(fd, cb)` one-shot readiness | chronos `addReader2(fd, cb)` (low-level), or `DatagramTransport` recv |
| `sleepAsync(ms)` (heap timer) | `sleepAsync(milliseconds(ms))` (chronos `Duration`) |
| `newFuture[void]` / `complete` / `fail` | chronos `Future` (strict `raises`/`gcsafe`) |
| `asyncCheck reader(qc)` | `asyncSpawn reader(qc)` |
| readiness-or-timer via future `or` | `await one(readFut, timerFut)` (chronos `one`/`race`) |
| raw `sockSend`/`sockRecv` on the fd | unchanged (OS syscalls on the driver's fd) |

Recommended socket integration: **reuse the C driver's raw UDP fd** (`navi_h3_fd`)
and wait on it with chronos's low-level fd readiness — the closest 1:1 port,
minimal C change. (A `DatagramTransport` is more idiomatic but would require the C
driver to not own the socket; defer unless the raw-fd path fights chronos's
strict-effects.) The chronos reader must satisfy `{.async: (raises: [...]).}` and
`gcsafe`, like `h2mux_chronos.nim`.

Wire buffered h3 in `chronos_impl.nim` mirroring `asyncdispatch_impl.nim`: add
`altSvc`/`h3conns` fields to the chronos `Navi`, the `getH3Conn`/`h3Transport`
procs, the Alt-Svc record on responses, and h3 teardown in `close`. All under
`when defined(naviHttp3)`.

## Phase C — streaming h3 on chronos
Reuse Phase A's `h3glue` design over `QuicConnChronos`; add the h3 branch to the
chronos `openStreamConn`. Since `h3glue` is written against the FFI + a small
per-driver "submit/read/close" interface, factor that interface so both drivers
satisfy it (a concept/obj with `submit`, `readBody`, `streamDone`, `abandon`),
letting one `h3glue` serve both backends.

---

## Files

- **New:** `src/navi/backend/h3glue.nim` (A2); `src/navi/backend/quic_chronos.nim`
  (B); `tests/interop/http3/stream_async_test.nim`,
  `tests/interop/http3/dispatch_chronos_test.nim`,
  `tests/interop/http3/stream_chronos_test.nim`.
- **Modify:** `src/navi/backend/h3client.cpp` + `quic.nim` (A1 incremental API);
  `src/navi/private/asyncdispatch_impl.nim` (A3 stream h3 branch);
  `src/navi/private/chronos_impl.nim` (B/C wiring); `src/navi/chronos.nim` if it
  gates the quic import; `tests/interop/http3/run.sh` (add chronos + streaming
  legs); `navi.nimble` (h3 tasks); `docs/http3.md` (it's **stale** — remove the
  "no h3 on chronos / BearSSL" claim, document chronos h3 + streaming).

## Gap-policy / stress follow-up
Once chronos h3 lands, update the stress harness gap policy
(`tests/stress/common/config.nim`) to allow `h3 + chronos`, and the TESTING.md
matrix. (Left as a small final step, not part of the core feature.)

## Verification
- **A (asyncdispatch streaming h3):** `tests/interop/http3` streaming test — SSE
  receives events and `stream()` downloads a payload over `HTTP/3` (assert
  `httpVersion`), hash-verified, against the Caddy h3 origin. Also the stress
  `stressSse`/`stressStreamDownload` with `NAVI_PROTO=h3 NAVI_BACKEND=asyncdispatch`
  now genuinely ride h3 (confirm via a server/log check, since Caddy also serves h2).
- **B (chronos buffered h3):** `dispatch_chronos_test.nim` — Alt-Svc upgrade + a
  buffered GET/POST over h3 on chronos, mirroring `dispatch_async_test.nim`.
- **C (chronos streaming h3):** `stream_chronos_test.nim` — SSE/stream() over h3 on
  chronos.
- Full unit suite stays green; the h3 image already builds the ngtcp2/nghttp3/
  OpenSSL-3.5 toolchain.

## Reference: the `vortex` package (nim-http-server)

The user's `vortex` HTTP **server** already drives ngtcp2 + nghttp3 over both
chronos and asyncdispatch (`src/vortex/http3/ngtcp2/vq_ngtcp2.{cpp,h}`,
`eventloop.nim`, `adapters/chronos.nim`), with the same toolchain navi uses
(`-lngtcp2 -lngtcp2_crypto_ossl -lnghttp3 -lssl -lcrypto`, OpenSSL QUIC binding).
Not reusable as code (server role vs navi's client; its own selector eventloop;
server-callback shim ABI). Studied its actual integration:

- **Phase B correction:** vortex's `adapters/chronos.nim` runs **chronos as a guest
  inside vortex's own selector loop** ("chronos's own fds cannot wake our
  selector"), the *inverse* of what navi needs (navi runs under chronos and drives
  the QUIC fd from within chronos's loop). So it is **not** a Phase B template.
  navi already has the QUIC-pump sequence in `quic_async.nim`; the chronos port
  just swaps asyncdispatch's `addRead`/`sleepAsync` for chronos's fd-readiness +
  `sleepAsync(Duration)`.
- **Phase A (the useful reference):** vortex's `vq_ngtcp2.cpp` streams the body via
  an `on_body` callback and, crucially, uses **deferred flow-control consume**
  (`h3DeferredConsume` -> `ngtcp2_conn_extend_max_stream_offset` on *consume*), so
  QUIC backpressures on the app's read rate. navi's `h3client.cpp` instead extends
  the window **immediately on receive** (buffers the whole body, no backpressure).
  Phase A's `navi_h3_read_body` should move navi to the deferred-consume model so
  streaming stays constant-memory under a slow reader -- vortex is the reference.

## Risks
- **chronos strict effects.** The reader/`h3glue` must satisfy chronos's
  `raises`/`gcsafe` tracking (as `h2mux_chronos.nim` does); the OpenSSL FFI in the
  C driver is outside Nim's effect system, so the boundary needs care.
- **Raw fd vs DatagramTransport.** If chronos's low-level fd readiness fights the
  driver's owned socket, fall back to a `DatagramTransport` (needs the C driver to
  accept an externally-owned fd or to expose send/recv buffers instead of the fd).
- **Incremental-read correctness.** Draining `Stream.body` mid-flight must not race
  nghttp3's callbacks; the drain happens on the reader's thread between `step`s
  (single-threaded async), so it's safe if `read_body` only touches the buffer.
- **h3 is `-d:naviHttp3` + heavy toolchain** (ngtcp2/nghttp3/OpenSSL 3.5); all new
  code stays behind `when defined(naviHttp3)` and out of default CI, like today.
- **Scope.** A alone is a meaningful feature (streaming/SSE over h3, all backends
  that have h3). B+C bring chronos in. Each phase is independently shippable.
