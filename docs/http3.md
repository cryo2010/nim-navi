# Plan: HTTP/3 (optional, native OpenSSL backends)

## Goal

Offer HTTP/3 as an **opt-in** capability on the native OpenSSL backends (`navi`
sync, `navi/asyncdispatch`, and `navi/chronos`), reusing navi's existing request/response and
streaming surface unchanged. h3 is negotiated per origin via Alt-Svc (or an
HTTPS DNS record), transparently, so calling code does not change:

```nim
let api = newNavi(NaviConfig(http: {H1, H2, H3}))   # H3 opt-in
let r = await api.get("https://example.com")         # may upgrade to h3 on a later request
echo r.httpVersion                                    # "HTTP/3" once upgraded
```

The QUIC transport, QPACK, and h3 framing are **delegated to a linked C library**
(ngtcp2 + nghttp3). navi writes the socket/event integration, the discovery
policy, and the glue to its own request model. Pure-Nim h1/h2 stays the
dependency-free default; nothing here is compiled unless `-d:naviHttp3` is set.

## Non-goals

- **No hand-rolled QUIC or QPACK.** Reimplementing QPACK and a QUIC state machine
  is not worth it; we link the same stack curl uses.
- **h3 on chronos (as of the OpenSSL migration).** The chronos backend now links
  OpenSSL like the other native backends, so ngtcp2's crypto binding works there
  too: `navi/chronos` gets HTTP/3 (buffered + streaming/SSE) via a chronos-native
  QUIC driver (`backend/quic_chronos.nim`). This bullet originally read "no h3 on
  chronos" back when chronos was BearSSL.
- **No h3 work on `navi/js`.** The browser/Node runtime already does h3 under
  `fetch`; it needs nothing from navi.
- **No h3 server, no 0-RTT early data (initially), no WebSocket-over-HTTP/3.**
  These are follow-ups, not part of the first cut.
- **No change to the default protocol.** `initNaviConfig()` stays `{H1, H2}`;
  H3 is never implied.

## Decisions and open questions

- **Library: ngtcp2 + nghttp3.** Reference pairing, sans-I/O C, MIT-licensed,
  used in production by curl. quiche (adds a Rust build) and lsquic (another large
  C dep) are alternatives; ngtcp2+nghttp3 fits navi's "link a C lib, drive I/O
  ourselves" model best and shares a TLS story with our existing OpenSSL path.
- **TLS crypto for QUIC: OpenSSL 3.5+ QUIC API** via ngtcp2's OpenSSL crypto
  binding. (OpenSSL gained a usable client QUIC TLS interface in the 3.2 series
  and stabilized it since; pin the minimum at build time. quictls/BoringSSL remain
  fallback options if a target's OpenSSL is too old.) **Open question:** minimum
  OpenSSL version to require when `-d:naviHttp3` is set.
- **Discovery: Alt-Svc first, HTTPS RR later.** Connect over h1/h2 as today; if
  the response carries `alt-svc: h3=":443"`, cache it per origin and use h3 for
  subsequent requests to that origin. HTTPS/SVCB DNS lookup (`alpn=h3`, enabling
  h3 on the *first* request) is a later enhancement. **Open question:** honor
  Alt-Svc `ma` (max-age) and persistence scope (per client vs process).
- **No happy-eyeballs racing of h3 vs h2 initially.** First cut upgrades only
  after Alt-Svc is seen. Racing UDP-h3 against TCP-h2 on the first request is a
  later optimization.
- **Feature scope at launch:** GET/POST/verbs, request/response headers, response
  streaming (`stream()`), request streaming (`bodyStream`), decompression, cookies,
  redirects, retries, timeouts, SSE (it rides on `stream()`), auth. **WebSocket is
  not offered over h3.**

## Dependency strategy

h3 is an **optional feature**, gated exactly like brotli/zstd are today:

- `-d:naviHttp3` enables compilation of the h3 modules and links `-lngtcp2
  -lnghttp3` (plus the ngtcp2 OpenSSL crypto binding). Without the flag, none of
  it is compiled and navi has no new dependency.
- `when defined(naviHttp3):` guards the config plumbing so `H3 in config.http` is
  only reachable in an h3 build; otherwise selecting H3 is a compile-time or
  fail-fast runtime error with a clear message (mirroring the current comment in
  `core/request.nim` about H3 silently falling back).
- The libraries load at link time (they are the transport; unlike brotli/zstd
  there is no lazy-load story). Document the system packages
  (`libngtcp2`, `libnghttp3`, and the ngtcp2 OpenSSL crypto lib) in the README
  Requirements section under the optional-features list.

## Architecture

Mirror the existing h2 split. Today:

- `proto/h2/{frame,hpack,huffman,conn}.nim` — sans-io HTTP/2 (bytes in, events out).
- `backend/h2mux.nim` — the async multiplexer that drives a real connection.
- `core/h2glue.nim` — maps navi `Request`/`Response` onto the h2 conn.

For h3, ngtcp2+nghttp3 own the sans-io codec role (QUIC + QPACK + h3 frames), so
navi adds a **transport driver** and **glue**, not a codec:

- `backend/quic.nim` (new) — thin FFI to ngtcp2 + nghttp3 (types, callbacks,
  `{.importc.}`/`{.passL.}`), plus a `QuicConn` that owns:
  - the connected **UDP socket** (one per origin connection),
  - the `ngtcp2_conn` + `nghttp3_conn` pair and their callback wiring,
  - the TLS handshake via the OpenSSL crypto binding (reusing `TlsConfig`:
    verification, `caFile`, min/max version, ALPN fixed to `h3`),
  - a **timer** for QUIC loss-recovery/ACK deadlines (`ngtcp2_conn_get_expiry`).
- `backend/h3mux.nim` (new) — the async analog of `h2mux.nim`: opens/uses a
  `QuicConn` per origin, submits requests as QUIC streams (h3 is natively
  multiplexed, one bidi stream per request), and surfaces the same internal
  request/response/stream handles the h2 mux does, so `stream()` and SSE work
  unchanged.
- `core/h3glue.nim` (new) — maps navi `Request` -> nghttp3 request submit
  (`:method`/`:scheme`/`:authority`/`:path` pseudo-headers, header list, body via
  a read callback for `bodyStream`) and nghttp3 response events -> navi `Response`
  / `StreamResponse` chunks. This is the h3 twin of `h2glue.nim` and should share
  its header/pseudo-header helpers where possible.

Selection point (async), extending the existing dispatch in
`private/asyncdispatch_impl.nim` where today:

```nim
let wantH2 = client.config.wantsH2 and req.url.isTls
```

becomes (sketch):

```nim
let h3Origin = client.config.wantsH3 and req.url.isTls and altSvc.hasH3(req.url)
if h3Origin:      return await h3Request(client, req, ...)   # via h3mux/QuicConn
elif wantH2:      ...                                         # unchanged
else:             ...                                         # h1, unchanged
```

The sync backend (`backend/sync.nim`) gets a blocking `QuicConn` driver (read/
write the UDP socket with the read timeout, service the QUIC timer between reads).
No multiplexing on sync (matching h2, where sync uses `parallel()`), one request
per connection at a time; the connection is still reusable for sequential
requests.

## Config surface

- `NaviConfig.http` already is `set[HttpVersion]` (`{H1, H2}` by default). Add the
  `H3` enum value (currently a comment in `core/request.nim`) and a `wantsH3`
  helper alongside `wantsH2`. Uncommenting `H3` is the one change to the public
  enum.
- `H3 in http` means "allow upgrading to h3 when discovered," not "force h3."
  `{H3}` alone (no H1/H2) is invalid: without an initial h1/h2 connection there is
  nothing to read Alt-Svc from (until HTTPS-RR lands). Validate and reject with a
  clear error, the same spirit as the existing H3-only guard.
- New optional `TlsConfig` / policy knobs (later): Alt-Svc cache TTL override,
  "disable h3" escape hatch per request.

## Discovery and connection lifecycle

1. First request to an origin goes over h1/h2 exactly as today.
2. On the response, parse `Alt-Svc`; if it advertises `h3`, record
   `(origin -> h3 endpoint, expires)` in a per-client `AltSvcCache` (new, small,
   in `core/`).
3. Subsequent requests to that origin open (or reuse) a `QuicConn` and go over h3.
   On QUIC connection failure, fall back to h2/h1 and optionally evict the cache
   entry.
4. `QuicConn` reuse follows the pool/keep-alive model: h3 connections are
   long-lived and multiplexed on async; the async client keeps a
   `TableRef[origin, QuicConn]` next to the existing `muxes` table.

## Event-loop / UDP integration

This is the real work the library does not do for us:

- **asyncdispatch:** register the UDP `AsyncFD`; on readiness, `recv` datagrams and
  feed `ngtcp2_conn_read_pkt`; drain writes via `ngtcp2_conn_writev_stream` /
  `nghttp3_conn_writev_stream`; arm an `addTimer` for the QUIC expiry so ACKs and
  loss recovery fire without inbound traffic.
- **sync:** a select/poll loop over the UDP socket bounded by the read timeout and
  the QUIC expiry, driven inside the blocking request call.
- Cancellation and per-phase timeouts reuse `core/cancel.nim` and the existing
  timeout plumbing; the QUIC idle timeout maps onto `timeouts`.

## Backend matrix after this work

| Capability | `navi` (sync) | `navi/asyncdispatch` | `navi/chronos` | `navi/js` |
| --- | :---: | :---: | :---: | :---: |
| HTTP/3 | opt-in (`-d:naviHttp3`) | opt-in (`-d:naviHttp3`) | opt-in (`-d:naviHttp3`) | runtime |
| h3 multiplexing | sequential per conn | transparent | transparent | runtime |

## Feature parity notes

- **Streaming / SSE:** `stream()` and `sse()` are built on the internal chunk
  pull; as long as `h3mux`/`h3glue` surface the same `StreamResponse` handle, both
  work over h3 with no changes to `navi.nim` or `private/*_impl.nim` SSE code.
- **Request bodies / `bodyStream`:** mapped to an nghttp3 body read callback;
  backpressure comes from QUIC stream flow control (analogous to the h2 window).
- **Decompression, cookies, redirects, retries, auth, timeouts:** all live above
  the transport in `core/`, so they are protocol-agnostic and unaffected.
- **WebSocket:** not offered over h3 (would require Extended CONNECT / RFC 9220);
  `websocket()` stays h1/h2.

## Phasing

1. **FFI + handshake:** `backend/quic.nim` links ngtcp2+nghttp3, completes a QUIC
   handshake to a known h3 server, exchanges one GET, prints the body. Sync only.
   Proves the dependency, the OpenSSL crypto binding, and the UDP/timer loop.
2. **h3glue + buffered requests:** full request/response mapping (verbs, headers,
   bodies), integrated into the sync engine behind Alt-Svc discovery.
3. **Async mux:** `backend/h3mux.nim` for `navi/asyncdispatch` with per-origin
   connection reuse and multiplexing.
4. **Streaming + SSE over h3:** wire the `StreamResponse` path; confirm SSE and
   `stream()` ride h3 unchanged.
5. **HTTPS/SVCB DNS discovery** (first-request h3) and optional h3-vs-h2 racing.

## Testing

- Extend the leak/sanitizer matrix (`tests/leakcheck`) with an h3 target once the
  async path exists (valgrind + ASan/UBSan, fd-leak detection): the UDP socket and
  the C library make fd- and memory-leak coverage especially valuable.
- Interop: add an h3 origin to the Dockerized interop suite (nginx/caddy/h2o all
  speak h3; or an ngtcp2 example server) covering GET/POST, streaming up/down,
  SSE, redirects, and Alt-Svc upgrade from an initial h2 request.
- Unit: Alt-Svc header parsing and the `AltSvcCache` TTL/eviction logic are
  pure-Nim and testable without a network.

## Risks and trade-offs

- **Dependency weight.** Reintroduces a heavy native dependency on exactly the
  backends navi keeps leanest. Mitigated by strict `-d:naviHttp3` gating and
  keeping h1/h2 dependency-free.
- **OpenSSL QUIC maturity/perf.** The QUIC TLS path is newer than the TLS record
  path; performance (userspace congestion control, per-datagram cost) can be
  neutral or worse than h2 on fast, low-loss links. h3's real wins are on lossy /
  high-latency / mobile networks (no TCP head-of-line blocking, faster setup,
  connection migration). This is a robustness/compatibility feature more than a
  universal speedup.
- **Event-loop complexity.** QUIC's timer-driven model is a genuine departure from
  navi's TCP-readiness loop and is the main integration cost.
- **Surface for bugs.** UDP + a C library + a new codec path warrants the leak/
  sanitizer coverage above from the start.

## Alternatives considered

- **quiche / lsquic** instead of ngtcp2+nghttp3: viable; rejected for the first
  cut (quiche adds a Rust toolchain; lsquic is a second large C dep with a
  different API). Revisit if the ngtcp2 OpenSSL binding proves painful on target
  platforms.
- **Link libcurl (built with h3):** gets h3 plus everything else from one dep, but
  effectively turns navi into a libcurl wrapper and abandons the pure-Nim h1/h2
  core. Rejected as contrary to navi's design.
- **OpenSSL 3.5 built-in QUIC as the transport** (driving nghttp3 on it and
  dropping ngtcp2): appealing now that OpenSSL 3.5 ships in current LTS distros
  (e.g. Ubuntu 26.04), since it would cut the QUIC/h3 libraries from two to one.
  Set aside for now: OpenSSL's client QUIC is the newer implementation and has
  reported slower throughput and higher memory use than the dedicated stacks, so
  navi stays on ngtcp2 (what curl/nginx run in production). Revisit once OpenSSL's
  QUIC matures, at which point it would meaningfully lower the dependency floor.
- **Wait.** Do nothing until OpenSSL QUIC and the Nim QUIC ecosystem mature. The
  status quo (`core/request.nim` keeps H3 intentionally absent) remains valid if
  the dependency cost is judged too high today.
