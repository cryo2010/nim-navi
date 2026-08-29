# Changelog

All notable changes to navi are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from 1.0.0
onward (pre-1.0, minor versions may include breaking changes).

## [Unreleased]

### Added
- **Request trailers.** `req.trailers` (a `Headers`, the same shape as `req.headers`)
  sends trailing header fields after the body: chunked transfer-encoding with a
  `Trailer` header on HTTP/1.1, and a trailing HEADERS section on HTTP/2 and HTTP/3
  (e.g. `grpc-status` for a gRPC-style request). Available on `request` and
  `buildRequest` via a `trailers` argument. A buffered body is sent chunked when
  trailers are present. Not supported on the js backend (fetch cannot send request
  trailers; setting them raises). Sync, asyncdispatch, and chronos backends.
- **HTTP/3 response trailers.** `res.trailers` now also surfaces the trailing HEADERS
  section of an HTTP/3 response, matching the existing HTTP/1.1 and HTTP/2 support.

### Fixed
- HTTP/3: certificate verification now runs after the handshake completes (checking
  `SSL_get_verify_result`) instead of aborting the handshake with `SSL_VERIFY_PEER`.
  On a rejected certificate, OpenSSL's in-handshake abort drove ngtcp2's experimental
  OpenSSL QUIC crypto binding to over-release its crypto buffers, tripping an
  assertion (`crypto_ossl_ctx_release_crypto_data`) and killing the process instead
  of raising `QuicError`. navi now rejects an untrusted or hostname-mismatched peer
  cleanly, before any request is sent, matching the post-handshake verification the
  TCP backends already use.

## [0.9.0] - 2026-08-29

### Added
- **Default `User-Agent` and `Accept` headers.** Requests now send
  `User-Agent: navi/<version>` and `Accept: */*` unless the caller sets their own,
  matching Go, curl, axios, and httpx (some servers and WAFs reject a
  User-Agent-less request).
- **`res.text`**: the response body decoded to UTF-8 using the `Content-Type`
  charset (or a leading BOM, else UTF-8), covering UTF-8, ISO-8859-1, Windows-1252,
  and UTF-16. Unlike `res.body` (raw bytes), it yields correct text for non-UTF-8
  responses; an unknown charset falls back to the raw bytes.
- **Response trailers.** `Response.trailers` now surfaces the trailing header fields
  of a chunked HTTP/1.1 response and an HTTP/2 trailing HEADERS block (e.g.
  `grpc-status`); previously they were parsed and discarded.
- **SOCKS5 proxies** (`socks5://` / `socks5h://`, with optional `user:pass@`
  credentials, RFC 1928 + RFC 1929) on the sync, asyncdispatch, and chronos
  backends. Also honors the `ALL_PROXY` env var. HTTP-proxy `CONNECT` now sends
  `Proxy-Authorization` when the proxy URL carries credentials.
- **Unix domain socket transport** via `NaviConfig.unixSocket`: dial a socket path
  (e.g. the Docker daemon) instead of TCP; the URL host/port are used only for the
  Host header and TLS SNI, and proxies are bypassed. Sync/asyncdispatch/chronos on
  POSIX; the js backend raises a clear error.
- **In-memory CA bundle** (`TlsConfig.caBundle`, a PEM string) added to the trust
  store alongside the system roots / `caFile`.
- **Certificate pinning** (`TlsConfig.pinnedKeys`): SPKI SHA-256 pins (base64, HPKP
  form); the peer's public key must match a pin or the connection is rejected.
- **Custom certificate-verification callback** (`TlsConfig.verifyCallback`): a hook
  run after the built-in chain + hostname checks, receiving the peer's leaf
  certificate (DER) and returning whether to accept it.
- **Connection-pool sizing**: `maxIdleConnsPerHost` (configurable per-origin idle
  cap), `maxIdleConns` (global idle cap), and `idleConnTimeout` (evict and close an
  idle pooled connection after a lifetime, never handing out a stale one).
- **Cookie name-prefix enforcement** (RFC 6265bis 5.5): a `__Secure-` cookie must be
  Secure over a secure origin, and a `__Host-` cookie must additionally be host-only
  and scoped to `Path=/`, or it is rejected.
- HTTP/3 now carries **streamed request bodies** (`bodyStream`): an upload is pulled
  chunk by chunk over the h3 request stream (with QUIC stream flow-control
  backpressure), on the sync, asyncdispatch, and chronos backends. Previously a
  request with a `bodyStream` silently fell back to h2/h1.
- The **sync** client gains HTTP/3 `stream()` and SSE: streaming downloads and
  Server-Sent Events ride h3 (discovered via Alt-Svc, upgrading on a reconnect for
  SSE) instead of silently downgrading to h2/h1. `httpVersion` is now exposed on
  `SseStream` across all native backends.

### Fixed
- HTTP/2 and HTTP/3 now validate the response body length against a declared
  `Content-Length`: a stream that ends cleanly (END_STREAM / FIN) but delivered a
  body of a different length is rejected as malformed (RFC 9113 8.1.1 / RFC 9114
  4.1.2) rather than accepted as a complete, truncated response. HEAD responses and
  1xx/204/304 statuses (which carry no body) are exempt. HTTP/1.1 already had this
  guarantee structurally (a `Content-Length` body is delimited by the byte count),
  and `navi/js` inherits it from the `fetch` runtime.
- Premature connection close mid-body no longer produces a silent partial response.
  On HTTP/1.1 and HTTP/2, a length- or chunked-delimited response whose connection
  drops before the body completes now raises instead of returning the truncated body
  as a successful 200. This also fixes a busy-loop hang in the streaming reader
  (`readChunk`/`drain`/SSE) on such a truncated length/chunked body. Complete
  responses and read-until-close bodies are unaffected.
- HTTP/2: after a GOAWAY, the wait for in-flight streams (at or below the last
  stream id) is bounded by a generous idle grace, so a peer that sends GOAWAY and
  then neither delivers the responses nor closes can no longer hang requests
  indefinitely (previously bounded only by an optional read timeout).
- HTTP/3: a request-body producer (`bodyStream`) that raises now resets just its own
  stream instead of failing the whole QUIC connection, so one bad upload no longer
  takes down every other request multiplexed on that origin's connection.
- HTTP/1.1 keep-alive reuse is now safe *and* complete for non-idempotent methods.
  When a pooled connection fails **before any response byte** (the classic keep-alive
  race: the server closed the idle connection), the request was not processed, so it
  is now replayed on a fresh connection even when non-idempotent (POST/PATCH) --
  previously the sync backend errored out. A failure **after** the response began is
  still only retried for idempotent methods, and the async/chronos backends no longer
  fall through unconditionally (which could re-send a request the server had already
  processed). A non-rewindable streamed body (`bodyStream`) is never replayed.
- HTTP/1.1 keep-alive: a connection whose response body was not fully read is no
  longer returned to the pool. A short read (e.g. the peer closing a keep-alive
  connection mid-body, which the async `SSL_set_fd` path surfaces as an EOF) left
  the unread body bytes on the wire; reusing that connection then parsed the stale
  body as the next response's status line, corrupting its version/status. `keepAlive`
  now requires the response to be fully consumed before the connection is pooled.
- HTTP/3: a streamed upload that exceeded a stream's QUIC flow-control window
  stalled (the driver treated `STREAM_DATA_BLOCKED` as fatal and never resumed the
  stream when the window reopened). The connection driver now blocks/unblocks the
  stream correctly and drains all queued datagrams per I/O cycle, so large uploads
  progress at full speed.

## [0.8.0] - 2026-08-28

Theme: HTTP/3 across the async backends, the chronos client's move to full OpenSSL
TLS + HTTP/2 parity, batteries-included middleware, and a live-mutable client
config.

### Added
- **HTTP/3 (QUIC)** on the asyncdispatch and chronos backends, opt-in via
  `-d:naviHttp3`: buffered requests, pull-based streaming downloads, and SSE now
  ride genuine h3 (previously h3 was buffered-`request()`-only, and `stream()` /
  `sse()` silently fell back to h2). Transparent per-connection stream
  multiplexing, `Alt-Svc: h3` upgrades, and per-stream reset/abort handling.
  Requires ngtcp2, nghttp3, and OpenSSL >= 3.5 (#160, #161, #162).
- Batteries-included middleware, imported to match your client: `navi/mw` (sync),
  `navi/asyncdispatch/mw`, `navi/chronos/mw`, `navi/js/mw`. Factories:
  `cache` (an RFC 9111 response-cache subset -- freshness + ETag/Last-Modified
  revalidation over an in-memory `CacheStore`), `rateLimit` (token bucket) and
  `concurrencyLimit` (in-flight cap; native async backends), and `bearer` / `basic`
  auth helpers. Add them to `config.middleware`; they wrap buffered
  `request()` calls (not `stream()`/`sse()`).
- The chronos client reaches full TLS parity with the sync and asyncdispatch
  clients: **HTTP/2** (ALPN-negotiated, with transparent stream multiplexing),
  **TLS 1.3**, **cipher selection**, **mutual TLS** (client certificates in every
  format: PEM, encrypted PEM, PKCS#12, DER, in-memory), and **TLS session
  resumption**. It now runs OpenSSL over its chronos transport instead of the
  bundled BearSSL.

### Changed
- `client.config` is now a mutable, live view of the client's configuration
  (previously read-only). Reconfigure a running client in place, e.g.
  `client.config.headers["authorization"] = "Bearer " & tok`; changes apply from
  the next request on. Exceptions: `tls`, `http`, and `proxy` are bound when
  connections open, so change those before the first request, via a new client,
  or `extend`. `newNavi` now always builds a fresh TLS session cache, so cloning a
  client's config (`newNavi(other.config)`) yields a fully independent client
  rather than one silently sharing the original's session cache.
- The chronos client's TLS is now OpenSSL (previously BearSSL). As a result,
  `https` on `navi/chronos` requires a `-d:ssl` build (it links OpenSSL, like the
  sync and asyncdispatch clients); plaintext `http` is unaffected. Setting
  `tls.ciphers`/`tls.cipherSuites` or `minVersion = tls13` on chronos is now
  honored rather than rejected.

### Fixed
- Cookie jar: replayed cookies are now ordered per RFC 6265 5.4 (cookies with
  longer, more specific paths are sent before shorter ones; cookies with
  equal-length paths keep their creation order). Previously they were emitted in
  storage order, which could send a less-specific duplicate first.
- chronos: disable Nagle on connect (matching the sync and asyncdispatch
  backends). A streamed upload's trailing partial segments were stalling on
  delayed-ACK (~40ms each), collapsing chronos upload throughput by ~10x versus
  the other backends (#167).
- HTTP/2: correct GOAWAY handling. Only streams the server never processed
  (id > last-stream-id) are failed and retried; an in-flight, already-processed
  stream is no longer failed un-retryably, and no new stream is opened after a
  GOAWAY (which had raised a PROTOCOL_ERROR under connection recycling) (#163).
- HTTP/3: credit the DATA payload to QUIC flow control, fixing a connection-level
  receive-window leak that wedged a long-lived h3 connection after ~1 MiB
  cumulative (surfaced as SSE freezing after ~36 reconnects) (#161).
- SSE (asyncdispatch): fix a use-after-free when a stream was closed while a read
  was parked in `sslRead` (the SSL was freed under the parked read, segfaulting at
  teardown), and stop a closed stream from transparently reconnecting (#159).

## [0.7.0] - 2026-08-18

Theme: faster and more resilient connection setup: one shared TLS context per
client, and Happy Eyeballs address racing on every native backend.

### Added
- Happy Eyeballs (RFC 8305) address racing now runs on the asyncdispatch and
  chronos backends too, not just sync. A client interleaves the resolved address
  families and races the connection attempts staggered by ~250ms, so a slow or
  blackholed address no longer stalls the connect until it times out. Handshake-
  aware fallback (drop a TLS-failing address and re-race the rest) is included on
  all three native backends.

### Changed
- Performance: the OpenSSL backends (sync, asyncdispatch) build one shared
  `SSL_CTX` per client and reuse it for every connection, instead of constructing
  and freeing a fresh context (parsing the trust store, wiring verification, ALPN,
  and version/cipher bounds) on each one. Cold, unpooled requests are ~14 to 20%
  faster since that setup is no longer repeated per handshake; the pooled path is
  unchanged. Session resumption is now armed once when the context is built.

## [0.6.0] - 2026-08-16

Theme: security and robustness hardening from a package-wide review, plus the
removal of the last pre-1.0 legacy field.

### Security
- Bound decompression on the buffered path: `maxResponseBytes` is now enforced
  *during* gzip/deflate/brotli/zstd decode, so a compression bomb is aborted
  mid-inflate instead of being fully materialized before the cap was checked.
- HTTP/2: reject a short `WINDOW_UPDATE` / `GOAWAY` frame (was an out-of-bounds
  read / crash), and treat an oversized `WINDOW_UPDATE` increment or
  `SETTINGS_INITIAL_WINDOW_SIZE` as a `FLOW_CONTROL_ERROR` (RFC 9113).
- WebSocket: reject a 64-bit frame length with the high bit set or over a 64 MiB
  cap, instead of crashing on the negative allocation.
- Digest auth is now origin-bound: a 401 Digest challenge is only answered on the
  origin the credentials were configured for, so digest credentials are not sent
  to a cross-origin redirect target (matching `Authorization` stripping).
- Reject CR/LF/NUL in request header names/values and the target host (request
  smuggling / header injection).
- SSE: bound a single event/line to 16 MiB and ignore an out-of-range `retry:`
  value, so a hostile stream cannot exhaust memory or crash the parser.
- Pooled keep-alive reuse no longer replays a non-idempotent request (POST/PATCH)
  on a fresh connection when the reused connection failed after the request may
  have been processed.
- A request with a streamed body (`bodyStream`) is no longer retried: its producer
  cannot be rewound, so a replay would have sent a truncated body.
- TLS to an IP-literal host now verifies the certificate's iPAddress SAN
  (`X509_check_ip`) instead of skipping the identity check, so a chain-valid
  certificate for a different name is no longer accepted for an IP target.

### Fixed
- HTTP/2 mux: release a concurrency slot when a streaming download completes (and
  on GOAWAY), fixing a deadlock where requests queued at `MAX_CONCURRENT_STREAMS`
  could hang.
- HTTP/3: a stream reset/abort now completes its waiter (and raises) instead of
  hanging until the connection closes.
- A malformed or out-of-range URL port (e.g. from a crafted redirect `Location`)
  now raises a clear `ValueError` instead of a cryptic integer-parse crash.

### Removed
- The legacy `NaviConfig.timeout` field. Use `timeouts.total` for the overall
  request deadline (`timeouts.connect` and `timeouts.read` bound the individual
  phases). Breaking; pre-1.0.

## [0.5.0] - 2026-08-10

Theme: Server-Sent Events as a first-class primitive across every backend, plus
the leak- and sanitizer CI matrix that hardens it.

### Added
- Server-Sent Events: `sse()` opens and validates a `text/event-stream` (a
  non-200 or wrong content type fails fast) and returns an `SseStream`, consumed
  with `next` (returns `none` at end) or the break-friendly `each` loop. Available
  on all four backends (#116, #118, #119).
- Transparent SSE reconnection: on a drop the stream resends `Last-Event-ID` and
  honors the server's `retry:` with exponential backoff up to `maxRetryMs`, unless
  `reconnect = false`; `lastEventId()` exposes the resume point. `verb`/`body`/
  headers enable POST-SSE and auth, which the platform `EventSource` cannot do.
  `close()` disposes the stream's dedicated client and its pool (#120).
- Strict sans-io SSE parser (`SseParser`) implementing the WHATWG
  `text/event-stream` grammar, reusable independent of transport (#116).
- `readChunk` pull primitive on `StreamResponse`: pulls the next decoded chunk and
  returns `""` at end (`navi/js`: an empty seq), returning the connection to the
  pool exactly as `drain` does. It is the break-friendly pull form the SSE reader
  builds on; `drain`/`each` remain the push form (#117).
- CI: a per-backend, per-scenario leak-check and sanitizer matrix. Valgrind
  memcheck with file-descriptor-leak detection (`--track-fds`) and ASan/UBSan
  across the sync/asyncdispatch/chronos backends, plus a Node heap- and
  fd-growth check for `navi/js`, over http1, http2, up/down streaming (compressed
  and not), SSE, and WebSocket (#122).

### Docs / tests
- Dockerized SSE reconnection demo with a `nimble` task and a CI check (#121); an
  SSE reconnect interop harness (#120).
- TESTING.md documents the leak/sanitizer matrix (#122).

## [0.4.0] - 2026-08-10

Theme: a full streaming stack (both directions, all backends) and the memory- and
shutdown-correctness fixes it surfaced.

### Added
- Pull-based streaming downloads: `stream()` returns a headers-first
  `StreamResponse` handle, consumed with `each`/`drain`/`close`, across all four
  backends. Each chunk is moved (no copy) from navi's read buffer (#108).
- Cooperative backpressure for streamed responses: awaiting the consumer stalls
  the peer instead of buffering. Over HTTP/2 this is a gated receive window
  replenished per consumed chunk, so a slow reader stalls only its own stream
  (#103, #104, #105, #106).
- Streaming request bodies (`bodyStream`) over HTTP/2, including the async mux;
  buffered on `navi/js` for cross-backend parity (#96, #98, #101).
- Concurrent-streaming interop as a one-command Dockerized task
  (`nimble streamConcurrent`): 50+ simultaneous uploads and downloads multiplexed
  over one h2 connection, verified by SHA-1 (#110).
- CI: four file-streaming checks (http1/http2 x upload/download) (#99) and a
  private-CA (`TlsConfig.caFile`) verification check on the sync backend (#92).
- Self-verifying file-streaming examples and a Dockerized FastAPI h2 demo
  (#97, #98).

### Changed
- **Breaking:** the free `stream(url, sink)` is removed; use
  `stream(url).drain(sink)` or the `each` template. The pull API does not throw on
  non-2xx (inspect `status`) and is not run through middleware (#108).
- **Breaking:** async response sinks take navi's native body type (`string`) and
  are handed each chunk by move rather than copy; `navi/js` keeps `seq[byte]`
  (its bytes come from a JS `Uint8Array`) (#106).

### Fixed
- HTTP/2 mux shutdown crash: `close()` now joins the background reader (via a
  socket shutdown + a `readerDone` future) instead of closing the transport out
  from under it, which orphaned the reader and segfaulted at teardown (#109).
- Sync client leaked idle pooled connections (and their ~85 KB OpenSSL contexts)
  when collected without `close()`; a `=destroy` leak-guard now closes them (#112).
- `StreamResponse` handles leaked their fields: a custom `=destroy` suppresses
  Nim's field destruction, so each handle leaked its header snapshot, parser, and
  key. Fixed by isolating the connection backstop in a small guard type so the
  handle needs no `=destroy` (#113).
- chronos: pass a `Duration` to `withTimeout`, dropping a deprecation warning
  (#94).

### Docs / tests
- TESTING.md records the streaming coverage matrix and backpressure tests
  (#102, #107); the valgrind harness now exercises `stream()` so this leak class
  is covered (#113).
- Unit tests renamed to the `<subject> should <effect>` convention (#93) and every
  previously check-less test now asserts on the raised error (#95).

## [0.3.0] - 2026-08-04

### Added
- TLS min/max version pinning (#85) and cipher-suite selection (#87).
- TLS session resumption across connections (#81).
- Happy Eyeballs (RFC 8305) address racing and handshake-aware address fallback
  on the sync backend (#86, #76).
- Per-phase timeouts: connect / read / total deadlines (#84).

### Changed
- **Breaking:** `Navi` is a `ref object` again, with `newNavi` restored (#74).
- **Breaking:** `newNaviConfig`/`newNavi` renamed to `initNaviConfig`/`initNavi`
  (#73).
- Performance: the sync and asyncdispatch backends now own the socket and drive
  the TLS handshake directly instead of going through `std/net`/`asyncnet`,
  cutting per-connection overhead (#80, #82). First-party `SSL_CTX` builder folds
  ALPN and credentials (#75). `NaviContext` holds the client by ref (#77).

### Fixed
- Security: HPACK decode is bounded to prevent a decompression-bomb DoS (#78).

### Docs / bench
- Added SECURITY.md (#90) and the TESTING.md test registry (#88).
- Dockerized multi-client benchmark (navi vs std/httpclient, Go, Rust) with
  Node.js and Python (requests) clients added (#79, #83).

## [0.2.0] - 2026-07-27

### Added
- Client certificates (mTLS) from encrypted PEM, DER, PKCS#12, and in-memory PEM
  (#70).

### Changed
- Retry policy: `backoffCap` renamed to `maxDelay` (#71).

### Fixed
- Sync backend read-timeout bug (#68).

### Tests
- Local httpbin interop behind Caddy (methods, auth, cookies, streaming) (#69) and
  multi-server + live interop suites (#68).

## [0.1.0] - 2026-07-27

Initial release: a batteries-included HTTP client for Nim with a uniform API
across four backends.

### Added
- **Backends:** sync (OpenSSL), asyncdispatch (OpenSSL), chronos (BearSSL), and
  js (`fetch`), sharing one API. The async entries fall back to `navi/js` under
  `nim js` (#59).
- **HTTP/2:** a sans-io implementation (frame layer, HPACK core + Huffman,
  connection driver), ALPN negotiation, send/receive flow control, CONTINUATION
  frames, `MAX_CONCURRENT_STREAMS` handling, frame-padding stripping, a
  shared-connection async multiplexer, and a multiplexed parallel batch API
  (#54 and the h2 series).
- **HTTP/1.1** request/response with an incremental parser.
- **TLS:** verification on by default; client certificates (mTLS) on the OpenSSL
  backends; chronos custom-CA verification (`TlsConfig.caFile`).
- **Auth:** basic, bearer, and Digest (RFC 7616/2617) with SHA-256 and strongest-
  offered-algorithm negotiation.
- **Cookies:** a per-client jar (RFC 6265; Max-Age and Expires), plus an opt-in js
  cookie jar for runtimes without a cookie store (#47).
- **Decompression:** gzip, deflate, brotli, and zstd, incremental for HTTP/1.1,
  with brotli/zstd loaded lazily.
- **Middleware:** onion-style middleware (replacing lifecycle hooks) that can wrap,
  short-circuit, or observe a request (#48, #53).
- **Ergonomics:** query params (accepts `Table`/`OrderedTable`/bare `{}`),
  cancellation tokens, configurable retry with backoff, response size cap,
  request timeouts, a multipart/form-data helper, http/https proxy support, an
  `options()` verb shortcut, and a cached `res.data` accessor.
- **WebSocket:** RFC 6455 client on all four backends.
- **Connection pooling / keep-alive** with automatic retry on a stale pooled
  connection.

### Security
- TLS certificates verified by default; HPACK bounds, negative `Content-Length`
  rejection, chunk-size bounds, and malformed-input rejection instead of crashes.

[Unreleased]: https://github.com/cryo2010/nim-navi/compare/v0.9.0...HEAD
[0.9.0]: https://github.com/cryo2010/nim-navi/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/cryo2010/nim-navi/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/cryo2010/nim-navi/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/cryo2010/nim-navi/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/cryo2010/nim-navi/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/cryo2010/nim-navi/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/cryo2010/nim-navi/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/cryo2010/nim-navi/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/cryo2010/nim-navi/releases/tag/v0.1.0
