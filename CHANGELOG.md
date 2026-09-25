# Changelog

All notable changes to navi are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from 1.0.0
onward (pre-1.0, minor versions may include breaking changes).

## [Unreleased]

### Added
- **`WsReader.closeCode`** on every client: the peer's close code once a
  streamed message ends in a close frame (1005 when the peer sent none, 1006 on a
  bodiless EOF). `closeAbnormal`, `closeProtocolError` and `closeNoStatus` are now
  re-exported by all four drivers, `navi/js` included, so cross-client code can name
  them without a per-client switch. On `navi/js` the code comes straight from the
  runtime's `CloseEvent`, which already reports 1005 and 1006 for those two cases,
  and `close()` now rejects the reserved 1005/1006/1015 while a frame would still be
  sent, exactly as the native clients do (#289, #396).
- **A `stream` namespace view: `api.stream.get(url)` opens a streaming download.**
  Streaming downloads now have the same two-layer shape as the rest of navi: a
  verb-named sugar over a full-control layer. `api.stream` returns a zero-cost view
  whose seven verb procs (`get`/`post`/`put`/`patch`/`delete`/`head`/`options`)
  open a streaming response, `api.stream.get(url)` (and `await api.stream.get(url)`
  on the async/js backends) mirroring `api.get(url)`. The verb-as-argument
  `api.stream(GET, url)` remains public as the full-control form, exactly as
  `request(verb, ...)` remains beside the verb helpers; the view is sugar over it
  and takes the same `headers`/`params`/`cancel` arguments.
- **A response `sink` on the buffered `request()` API (and every verb helper).**
  Passing `sink =` to `request`/`get`/`post`/`put`/`patch`/`delete`/`head`/`options`
  streams the response body to a caller callback instead of buffering it into
  `res.body`, while keeping the full policy layer (redirects, retries, digest,
  middleware, throw-on-non-2xx). Only the **final surfaced** response's body reaches
  the sink: redirect hops, digest 401 challenges, retried statuses, and (with
  `throwHttpErrors` on) thrown non-2xx bodies never do (the `HttpError` still carries
  its buffered body). A **bool** sink (`GatedBodySink`) returns `false` to stop the
  download early, in which case the request returns normally with the new
  `res.bodyTruncated == true` and `res.body == ""`; a **void** sink (`BodySink`)
  always continues. The sink is awaited on the async backends (backpressure) and
  synchronous on the sync backend; chunks are `string` on the native clients and
  `seq[byte]` on `navi/js`. A gzip/deflate/br/zstd body is decoded before delivery,
  the size cap is enforced through the sinked path, and trailers surface on a full
  drain (absent on an early stop). `HEAD`/`204`/`304` never call the sink. On a
  `-d:naviHttp3` build the h3 leg buffers the final body and delivers it in one call;
  the h1/h2 legs stream it incrementally. Passing no `sink` is unchanged (#369).
- **An async body producer arm for streamed uploads on the async backends.**
  `body` on `navi/asyncdispatch` and `navi/chronos` now accepts an async producer
  (`proc(): Future[string]`) alongside the synchronous `BodyProducer`: the engine
  `await`s each call, so producing a chunk can itself await. This enables piping a
  streaming download into a streaming upload in constant memory
  (`body = proc(): Future[string] {.async.} = return await src.readChunk()`), which
  the synchronous producer could not express (the engine called it inline). The
  producer streams as chunked transfer-encoding (h1) or DATA frames (h2), with
  trailers, identically to the sync producer, and is **not replayable** (sent once,
  never retried/redirected/digest-replayed). Two paths buffer instead of stream:
  h3 drains the producer before sending (its C-side body pull is synchronous), and
  `navi/js` does the same as `fetch` cannot stream a request body; both still await
  each chunk. The sync backend rejects an async producer at compile time (no event
  loop to await it). Its type lives at the backend layer (`AsyncBodyProducer`, like
  `BodySink`), since the `Future` type differs per backend (#367).

### Changed
- **HTTP/2 streamed uploads frame each chunk straight from the producer's buffer.**
  A chunk that fits the send window goes into DATA frames without staging through
  the per-stream send buffer, and the buffer keeps its capacity across chunks instead
  of being reallocated per chunk (#297, #298).
- **HTTP/1.1 streamed uploads coalesce small producer chunks.** Body bytes are
  buffered up to 16 KiB (one TLS record) before each write, and the final chunk plus
  trailers ride the last write, so a chatty producer no longer costs one write and
  one TLS record per chunk. Chunks at or above the threshold are written directly
  (#299).
- **BREAKING: the request `body` is now type-dispatched, and the `json`,
  `multipart`, and `bodyStream` parameters are removed (no deprecation).** The
  `body` argument of `request`/`post`/`put`/`patch` dispatches on its type: a
  `string` is the raw body (default `""`), a `JsonNode` is sent as JSON
  (`application/json`), a `Multipart` as `multipart/form-data`, a `BodyProducer`
  or a closure `BodyIterator` streams a chunked upload, and any other value is
  serialized to JSON via `std/jsonutils` (`application/json`). The `BodyIterator`
  arm ends the body at `finished(it)` rather than a `""` yield, skipping an empty
  mid-stream chunk so it cannot truncate the upload. Migrate `json = X` /
  `multipart = @[...]` / `bodyStream = P` to `body = X`. `buildRequest` now takes
  a `ResolvedBody` (produced by `toBody`) instead of the removed parameters.
  `post`/`put`/`patch` gain streaming (they forward the typed `body`). The `form`
  parameter is unchanged and stays outranked by a typed `body` (precedence:
  typed body > form > raw string). `body = nil` is now a compile error. On the js
  backend a streamed body is still buffered (`fetch` cannot stream a request
  body); the iterator wrapper only returns `""` at the true end of body, so
  buffering cannot truncate it (#365).

### Fixed
- **A closing TLS connection no longer breaks the other live TLS connections on the
  same thread.** OpenSSL's error queue is per THREAD, not per `SSL`, and
  `SSL_get_error` is documented to be reliable only when that queue was empty before
  the I/O call. navi never cleared it, so the entries a teardown leaves behind (the
  decrypt error on the truncated final record after `shutdownConn`'s SHUT_RDWR, which
  the read path swallows as teardown noise, and `SSL_shutdown` in the close path) made
  the NEXT read on an unrelated, healthy connection report `SSL_ERROR_SSL` and raise
  "navi: TLS read failed" -- tearing down its h2 mux and failing every stream on it
  with "navi: http/2 connection closed". A WebSocket-over-h2 soak lost 7 of 8 live
  sockets the moment the first one closed. Every `SSL_connect` / `SSL_read` /
  `SSL_write` now clears the queue first, on all three OpenSSL-driving backends
  (sync, asyncdispatch, chronos).
- **A response `sink` now receives the body of a surfaced 301/302 redirect.** The
  delivery gate still used the old "a non-replayable 307/308 is surfaced" rule, so a
  GET with a body producer that took a 301/302 -- which `followRedirects` surfaces,
  because the hop would replay a spent producer -- had its body withheld from the
  sink. The gate now asks `redirect.preservesBody` with the hop's own verb, the exact
  condition the redirect loop breaks on (#369, #295).
- **The streamed-upload write buffer no longer exceeds `h1CoalesceSize`.** It was
  flushed only after an append pushed it past 16 KiB, so a framed chunk could reach
  almost 32 KiB and spill into a second TLS record. The send loop now flushes before
  an append that would overflow the buffer, and a producer chunk large enough to be
  framed on its own is packed into the same write as the pending bytes instead of
  costing a second one (#299).
- **HTTP/3 fallback no longer replays a request that may already have run.** A
  `QuicError` raised after the request was submitted on the h3 stream now surfaces as
  `QuicSubmittedError`; the h2/h1 fallback only fires for that case when the request
  is idempotent and replayable (no spent body producer), mirroring the h1/h2
  keep-alive-race rule. Pre-submit failures (no connection, connect failure, submit
  rejected) still fall back freely for any method. This covers the streaming-response
  paths (`stream` / SSE over h3) as well as the buffered ones: `awaitHeaders` and the
  incremental body readers are only ever reached with the stream already on the wire,
  so every failure they raise is now a `QuicSubmittedError`, and the sync and async
  `stream` legs gate their fall-through to h2/h1 on the same rule instead of retrying
  a submitted-then-reset POST (#378, #293).
- **A streamed body is no longer replayed through a 301/302 redirect.** A GET/HEAD
  carrying a body producer used to be re-issued to the redirect target with an already
  drained producer; `followRedirects` now returns the 3xx to the caller for a streamed
  body on any hop that would preserve it (301/302 for GET/HEAD, 307/308 for every
  verb), stated once in `redirect.preservesBody` (#295).
- **A caller-supplied `Content-Length` is dropped on the streamed-upload path.** h1
  emitted it next to `Transfer-Encoding: chunked` (a CL.TE smuggling ambiguity) and h2
  forwarded a `content-length` that disagreed with the DATA frames; both now strip it,
  as h3 already did (#294).
- **HTTP/1.1 request trailers are filtered like h2/h3.** `finalChunk` and the
  `Trailer:` advertisement drop forbidden names (`content-length`,
  `transfer-encoding`, `te`, `trailer`, ...); the list now lives in one place
  (`headers.isForbiddenTrailer`) shared by every transport (#296).
- **SSE: the resume id is promoted at dispatch, not at parse.** An `id:` line of an
  event that never completed (the connection dropped mid-event) no longer leaks into
  `Last-Event-ID`, which made the server resume *past* an event the app never saw. A
  bodiless `id:` event still updates the id, per WHATWG (#290).
- **SSE: a `maxSseEventBytes` breach closes the stream handle.** `feed()` raised out
  of `next()` with the connection still open on every client; the handle is now torn
  down before the error surfaces (#292).
- **WebSocket: the 64-bit frame-length guard is no longer bypassable on 32-bit
  builds.** The extended length is accumulated in `uint64` and checked before
  narrowing to `int` (#285).
- **WebSocket: `validate101` enforces the mandatory handshake tokens.** A 101 now
  needs `Upgrade: websocket`, a `Connection` field containing `upgrade`, and exactly
  one `Sec-WebSocket-Accept` (RFC 6455 4.1) (#286, #289).
- **WebSocket: the h1 upgrade request rejects CR/LF/NUL and colliding fields.** Caller
  headers (and the request target / Host) are validated, and fields that collide with
  the built-in handshake headers are dropped; the Extended CONNECT path also strips a
  stray `sec-websocket-key`/`-accept`/`http2-settings` and no longer duplicates
  `sec-websocket-version` (#288, #289).
- **WebSocket: any protocol error fails the connection.** `receive` caught only
  `WsMessageTooLarge`; a masked server frame, a bad close body or code, invalid UTF-8,
  a bad fragmentation sequence, or a decoder error (RSV bits, reserved opcode,
  oversized control frame) now sends a 1002 close, flips `open`, and tears the
  transport down before re-raising, on every client. The same teardown covers the
  two streaming-reader desync paths (`openStreamReader` / `readChunk`), so a direct
  `readChunk` caller no longer leaks the h2/h3 connection (#281, #284).
- **WebSocket: the streaming read path validates text messages as UTF-8** (RFC 6455
  8.1), carrying a code point split across frames and requiring the message to end
  on a boundary; a failure fails the connection like any other protocol error (#282).
- **WebSocket: h2/h3 Extended CONNECT accepts any 2xx**, not only 200 (RFC 8441 /
  RFC 9220) (#287).
- **WebSocket: the streaming reader validates the peer's close frame.**
  `stream()`/`readChunk` took the close frame on trust: a 1-byte body, a code that
  may never appear on the wire (1005/1006/1015, 1004, anything outside
  1000-1014/3000-4999), or a non-UTF-8 reason surfaced as a clean `wmClose` and was
  echoed back to the peer verbatim. They now run the same checks `receive` does --
  one shared `ws.parseClose` -- and fail the connection with 1002 before raising.
  A synthetic EOF (no close frame at all) is still reported as 1006, never
  validated or echoed.
- **WebSocket driver hygiene:** `send`/`ping` on a closed socket raise a clear
  `IOError` instead of poking a torn-down transport; `close(code)` rejects the
  reserved codes 1005/1006/1015; the keepalive-death path drops its stale pending
  read; and a bodiless transport EOF reports 1006 (`closeAbnormal`) consistently on
  the buffered and streaming paths (#289). `WsWriter.write` (and the fin frame sent
  on block exit) raise the same `IOError` on a closed socket, and `close(code)`
  rejects a reserved code only while a close frame would actually be sent, so
  mirroring a received `m.closeCode` back on teardown stays the promised idempotent
  no-op.
- **Keep-alive race: a request dropped before any response is now retried, following
  the same rule as Go `net/http` (RFC 9110 9.2.2).** A connection can be torn down by
  the server at any time -- an idle recycle, a GOAWAY-less close, or a freshly-opened
  connection dropped under load. navi now classifies such a failure precisely:
    - **Provably unprocessed** -- an HTTP/2 REFUSED_STREAM / above-GOAWAY signal, or a
      connection found dead *before the request was written* -- surfaces as
      `UnprocessedError` and is retried for **any** method (it definitely never ran).
    - **Ambiguous** -- the request was written but the connection closed before any
      response HEADERS -- surfaces as a new `KeepAliveRaceError` (an `IOError` subtype).
      This is retried for **idempotent** methods, or for **any** method when the request
      carries an `Idempotency-Key` (or `X-Idempotency-Key`) header vouching that a replay
      is safe. A non-idempotent request (POST/PATCH) *without* such a key is NOT
      auto-retried: once its bytes are on the wire it may already have been processed,
      and replaying could double-apply a side effect (the exact heuristic RFC 9110 9.2.2
      flags as unsafe, and which Go and undici also decline).
    - **A drop AFTER the response began** -- including a `1xx` interim response
      (100-continue / 103 Early Hints) that arrived before the connection dropped --
      stays a plain `IOError` and is never auto-retried (the peer demonstrably began
      responding).
  Previously HTTP/2 had no keep-alive-race handling at all (any close surfaced a generic
  `IOError` the retry policy would not replay even for an idempotent verb on the mux
  path), and the HTTP/1.1 pooled path over-replayed non-idempotent requests on any
  pre-response close. Covers HTTP/1.1 (sync + async) and HTTP/2 (the async mux on
  `navi/asyncdispatch` and `navi/chronos`, and the sync pooled-h2 carrier), on both the
  buffered `request()` and the streaming `stream()`/`sse()` paths. The single replay
  decision lives in one predicate (`retry.replayableAfterError`) shared by every
  reused/pooled fall-through. The classification covers the whole exchange, not just
  the header read: a failure while *writing* the request on a reused connection is the
  same ambiguous race (previously it surfaced as an unclassified transport error and
  was never replayed), an HTTP/2 request still queued behind the mux's serialized send
  chain when the connection died is provably unprocessed, and a `1xx` interim is
  tracked on HTTP/1.1 too (the parser previously discarded it, making a 103-then-drop
  look like a "no response" race). Concurrent requests failed by one HTTP/2 connection
  death now coalesce onto a single fresh connection instead of each opening (and
  leaking) its own. HTTP/3 already re-sends on a QUIC failure via its Alt-Svc
  fallback; see #378 for a related follow-up to gate that fallback for non-idempotent
  requests.

## [0.10.0] - 2026-09-15

### Added
- **WebSocket over Extended CONNECT.** `websocket()` now tunnels over HTTP/2
  (RFC 8441) when `config.http = {H2}` and HTTP/3 (RFC 9220, `-d:naviHttp3`) when
  `{H3}`, in addition to the default HTTP/1.1 Upgrade. Supported on all three native
  clients (sync, asyncdispatch, chronos); the sync h2 path uses a dedicated blocking
  h2 connection and the sync h3 path runs a background pump thread (needs
  `--threads:on`). Over h2/h3 the handshake uses the `:protocol` pseudo-header (no
  `Sec-WebSocket-Key`/`Accept`). The public API is unchanged (#190).
- **Request trailers.** `req.trailers` (a `Headers`, the same shape as `req.headers`)
  sends trailing header fields after the body: chunked transfer-encoding with a
  `Trailer` header on HTTP/1.1, and a trailing HEADERS section on HTTP/2 and HTTP/3
  (e.g. `grpc-status` for a gRPC-style request). Available on `request` and
  `buildRequest` via a `trailers` argument. A buffered body is sent chunked when
  trailers are present. Not supported on the js backend (fetch cannot send request
  trailers; setting them raises). Sync, asyncdispatch, and chronos backends (#178).
- **HTTP/3 response trailers.** `res.trailers` now also surfaces the trailing HEADERS
  section of an HTTP/3 response, matching the existing HTTP/1.1 and HTTP/2 support
  (#178).
- **WebSocket message-size cap.** `websocket(..., maxMessageBytes)` bounds a
  reassembled message across its fragments (the per-frame 64 MiB cap did not); past
  it `receive` closes with 1009 and raises `WsMessageTooLarge`. `0` (default) is
  unlimited. On `navi/js` it is a delivery-time check for parity (#180).
- **WebSocket keepalive.** `websocket(..., keepAlive)` (ms) pings after an idle
  interval while a `receive` is in progress and raises `TimeoutError` (closing the
  connection) if another interval passes with no response, so a dead peer is detected
  instead of blocking forever. `0` (default) is off; a no-op on `navi/js` (#180).
- **WebSocket message streaming.** `ws.stream()` returns a reader for the next inbound
  message consumed a frame at a time with `each`/`readChunk` (no whole-message
  buffering); `ws.stream(writer): …` (and `streamBinary`) sends a message as fragments
  via `writer.write`, closing it automatically at block exit. Mirrors HTTP
  `stream()`/`bodyStream`. On `navi/js` (which owns framing) a read yields the whole
  message as one chunk and a write buffers until the block exits (#181).
- **HTTP/2 keepalive PING.** `config.timeouts.h2KeepAlive` (default 20 s) pings an
  idle HTTP/2 connection and closes it when the peer stops answering, so a dead
  pooled connection is detected instead of wedging the next request (#232).
- **SSE idle timeout on the sync backend.** `sse(..., idleTimeoutMs)` (default 45 s)
  now also exists on the sync client, matching async: it bounds each read and each
  (re)open, so a server that sends headers then goes silent raises `TimeoutError`
  instead of hanging `next()` forever. Any received byte (including a keep-alive `:`
  comment) resets it; `0` disables it (#357).

### Changed
- WebSocket masking keys and the handshake nonce now come from the OS CSPRNG
  (`std/sysrand`) instead of a time-seeded `std/random`, matching RFC 6455 5.3's
  requirement of a strong entropy source (native backends; `navi/js` uses the
  runtime's WebSocket) (#180).
- Proxy configuration (the `config.proxy` URL and the `HTTPS_PROXY`/`ALL_PROXY`/
  `NO_PROXY` env vars) is resolved once at client construction instead of re-read
  and re-parsed on every request attempt; only the cheap `NO_PROXY` host match
  remains per request. A malformed proxy URL now raises `ValueError` when the
  client is built rather than on the first request (#361).
- Performance: fewer copies and syscalls on the SSE and streaming read paths
  (#230); multi-member gzip decoding and other hot-path items (#244, #246);
  digest and WebSocket hashing use OpenSSL EVP (hardware SHA) on Linux (#194,
  #197); WebSocket masking works word-at-a-time (#186); h2 DATA padding no longer
  costs an extra payload copy, content-encoding is resolved once per stream, and
  the streaming decoder reuses its output buffer (#307, #308, #309).
- Internal: the asyncdispatch/chronos backend twins (engine, h2mux, quic,
  middleware) were unified behind shared include fragments (#242, #248-#256), and
  two design-pattern sweeps tightened the codebase with no public API changes
  (#319-#334, #336-#353).

### Fixed
- HTTP/3: certificate verification now runs after the handshake completes (checking
  `SSL_get_verify_result`) instead of aborting the handshake with `SSL_VERIFY_PEER`.
  On a rejected certificate, OpenSSL's in-handshake abort drove ngtcp2's experimental
  OpenSSL QUIC crypto binding to over-release its crypto buffers, tripping an
  assertion (`crypto_ossl_ctx_release_crypto_data`) and killing the process instead
  of raising `QuicError`. navi now rejects an untrusted or hostname-mismatched peer
  cleanly, before any request is sent, matching the post-handshake verification the
  TCP backends already use (#179).
- SSE: reads and reconnects are bounded by `idleTimeoutMs` so a wedged stream
  raises instead of hanging forever (#229, #231), and the asyncdispatch SSE reader
  no longer grows without bound under a flooding server (#257).
- RFC frame validation is enforced for HTTP/2 frames, HPACK Huffman coding, and
  WebSocket frames, with follow-up hardening (#207, #208-#213).
- WebSocket: the h1 and h2 handshakes honor `config.timeouts.read` on all backends
  (#206), and async teardown is idempotent on close and EOF (#202).
- Correctness batch (#234-#241): streaming digest retry no longer sends
  credentials cross-origin; HPACK dynamic-table desync on reset streams;
  `Retry-After` overflow crash; redirect/digest retries replaying an exhausted
  `bodyStream`; a transport leak in `poolTransport`; rejected `Set-Cookie` still
  evicting the stored cookie; h2 GOAWAY truncating in-flight bodies; and padded
  DATA frames leaking stream window credit.
- HTTP/2 batch (#258-#266): chronos cancellation wedging the send chain;
  RST_STREAM after a complete response discarding it; aborted downloads leaving
  server-side zombie streams; leaked waiters and concurrency slots; keepalive PING
  blocked behind a stalled send; `openConnect` hanging when the peer never sends
  SETTINGS; and HPACK desync from `resetStream` during an open header block.
- HTTP/1.1 batch (#269-#273): truncated batch responses returned as complete;
  IPv6 host literals losing their brackets; lenient framing-header parsing
  (chunked substring, TE+CL, multiple/signed `Content-Length`); `Connection:
  close` on a second header line missed; and caller-supplied chunked
  `Transfer-Encoding` sending an unframed body.
- HTTP/3: buffered bodies over 64 KiB were truncated and `maxResponseBytes`
  ignored; headers/trailers over 16 KiB desynced the field parser; `awaitHeaders`
  hung on a stream reset before headers; GOAWAY is now observed (#275-#278);
  receive windows raised to fix a quinn interop stall (#187, #188); and an async
  h3 streaming OOM (#182).
- Streaming downloads (#300-#306): h3 no longer buffers the whole body in C
  memory without backpressure or cap; the `maxResponseBytes` cap applies to
  decoded output on every path (decompression bombs); sink exceptions no longer
  leak the QUIC stream; truncated compressed bodies are rejected instead of
  silently accepted; h2 receive-window overruns raise `FLOW_CONTROL_ERROR`; and
  raw (headerless) DEFLATE decodes identically buffered and streamed.
- Connection lifecycle (#311-#316): transport leaks on a failed h2 preface; dead
  h2 muxes never evicted from the client; `idleConnTimeout` gaps on the
  sync/chronos buffered and async streaming paths; a chronos double-close (double
  `SSL_CTX_free`); `close()` ignoring in-flight connects; and a swallowed
  `CancelledError` on reader join.
- `originKey` canonicalizes consistently and `resolveProxy` validates the proxy
  port (#325); plus the 0.9.0 code-review batch (#191, #193, #195, #196).
- WebSocket over HTTP/3 now honors `config.timeouts.read` (a per-read stall bound)
  and `total` (a handshake deadline); previously only `connect` applied, so a
  stalled h3 WebSocket read hung forever regardless of configured timeouts (#356).
- Async `stream()` opens (connect + TLS + request + response headers, across every
  redirect/digest hop) are bounded by `config.timeouts.total`, and the body reads
  share that same single budget, matching the sync backend's connect-time deadline.
  Previously the open phase was unbounded (#358).
- Sync and batch retries honor `config.timeouts.total` as an overall deadline:
  backoff sleeps are capped to the remaining budget and retrying stops once it is
  spent, per the documented "whole request, including retries/redirects" contract.
  Previously only the async backends enforced this (#359).
- Reused pooled connections re-apply the live `config.timeouts` (read timeout and
  the per-attempt total budget), and reused HTTP/2 connections pick up
  `config.timeouts.h2KeepAlive` changes, including enabling or disabling keepalive,
  honoring the documented live-config contract. Previously the values captured at
  connect time stuck for the connection's lifetime (#360).

### Security
- Digest auth escapes the username when building the `Authorization` header,
  closing a quoted-string injection via a crafted username (#324). See also the
  decompression-bomb caps (#301, #302) and RFC frame validation (#207) under
  Fixed.

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

[Unreleased]: https://github.com/cryo2010/nim-navi/compare/v0.10.0...HEAD
[0.10.0]: https://github.com/cryo2010/nim-navi/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/cryo2010/nim-navi/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/cryo2010/nim-navi/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/cryo2010/nim-navi/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/cryo2010/nim-navi/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/cryo2010/nim-navi/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/cryo2010/nim-navi/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/cryo2010/nim-navi/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/cryo2010/nim-navi/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/cryo2010/nim-navi/releases/tag/v0.1.0
