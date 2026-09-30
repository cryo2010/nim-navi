# Changelog

All notable changes to navi are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from 1.0.0
onward (pre-1.0, minor versions may include breaking changes).

## [Unreleased]

### Added
- **An opt-in `Expect: 100-continue` gate for HTTP/1.1 uploads (`expectContinueMs`).**
  Setting `config.expectContinueMs` (ms; `0`, the default, disables it) makes the h1
  send path put `Expect: 100-continue` on the request head and wait that long for the
  server's interim `100 Continue` before sending the body, so an upload an endpoint
  will refuse (413, 401, 403, ...) costs the head instead of the whole body and the
  producer is never pulled. If the server answers with a final status instead, the
  body is withheld and the connection is **not pooled** afterwards (a peer still
  waiting for that body would read the next request as it). If the server stays
  silent, the body is sent once the timeout lapses (RFC 9110 10.1.1), and a `100` that
  lands after that is discarded like any other interim. A `417 Expectation Failed` is
  surfaced as an ordinary response rather than silently retried without the header,
  since navi's streamed bodies are non-replayable by contract. The header is added to
  a local copy of the request inside the h1 send, so h2/h3 never carry it and a retry,
  redirect or digest replay does not inherit it; it is only ever sent on a request
  that actually has a body. Native clients only (`navi/js` leaves request framing to
  `fetch`) (#392).
- **A bounded `recvWithin` transport op on the three native clients.** A read that
  gives up after a deadline and leaves the connection usable, unlike the (terminal)
  per-read timeout. The sync client only polls readiness, so nothing is consumed on
  expiry; the asyncdispatch and chronos clients **park** the unfinished read on the
  connection and resume it from the next `recvSome`, so its bytes reach the next
  reader instead of being swallowed (asyncdispatch cannot cancel at all, and a
  cancelled chronos read can drop bytes it already took off the transport). This is
  what lets the `Expect: 100-continue` gate wait for the interim response and then
  carry on reading the same connection (#392).
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
- **`TlsConfig.verify` became `TlsConfig.insecureSkipVerify` (#422).** The flag was
  inverted so the zero value is the secure one. Field assignment is unaffected
  (`cfg.tls.verify = false` still compiles, via a `verify`/`verify=` accessor pair),
  but object construction that named the field, `TlsConfig(verify: true, ...)`, no
  longer compiles: pass `insecureSkipVerify: true` to opt out, or drop the field
  entirely to verify. `wantsVerify` is unchanged and still the way to read the flag.
- **`navi/js` marshals body and WebSocket bytes with bulk typed-array copies, and
  its WebSocket queue is a deque.** Every byte of a streamed response body used to
  cross the jsffi boundary through its own dynamic `JsObject` index plus a
  `.to(int)` conversion, and a WebSocket payload through its own `Uint8Array` read
  or write, so a 50 MB download ran 50 million property lookups and int conversions
  and blocked the event loop for the whole of each chunk. Nim's js backend already
  represents a `string` and a `seq[byte]` as a plain JS array of byte values, so
  each conversion is now a single native array copy: `Array.prototype.slice.call`
  from a `Uint8Array` (sink chunks, pull-stream chunks, WebSocket binary receive),
  `new Uint8Array(s)` back out (WebSocket binary send), and one array copy for the
  buffered-body fallback that hands a `.text()` body to a `seq[byte]` sink. It is
  byte-exact for arbitrary binary (no UTF-16 round trip, no `fromCharCode.apply`
  argument-count limit), and a new Node test asserts all 256 byte values survive a
  1 MiB multi-chunk download and a 64 KiB WebSocket echo. `receive` also pops the
  pending-message queue from the head in O(1) instead of `delete(0)`, which was
  O(queue) per message and quadratic under a flooding peer (#412).
- **The streaming WebSocket UTF-8 scanner validates each chunk in place.**
  `scanUtf8` used to build `carry & chunk` and validate that copy, so every streamed
  text frame was copied once more into a chunk-sized temporary just to prepend at
  most three bytes carried over from the last frame. The carried bytes are now
  completed from the head of the new chunk and checked on their own, the rest of the
  chunk is validated in place from that offset (`isValidUtf8` takes an `openArray`),
  and the carry lives in a fixed 4-byte array instead of a string. Scanning a chunk
  now allocates nothing at all (it was two allocations plus a full copy per frame):
  256-byte frames scan ~1.22x faster and 16 KiB frames ~1.02x, allocation-free in
  both cases. Overlongs, surrogates, anything above U+10FFFF, a sequence split at any
  byte boundary and a message that ends part-way through a code point are accepted
  and rejected exactly as before (#414).
- **A received WebSocket message is moved through the assembler instead of being
  copied twice more.** `WsAssembler.offer` took its frame by value, copied the
  payload into its reassembly buffer, and then copied that buffer again into the
  delivered `WsMessage`, so every message cost three copies of its bytes (the
  decoder's included) and briefly held three live copies: a 64 MiB binary message
  moved 192 MiB. `offer` now takes the frame as a `sink` and moves the payload into
  the buffer and the buffer into the message, so an unfragmented message reaches the
  caller with only the decoder's copy and a fragmented one with one copy per
  fragment. The streaming readers no longer pin their opening frame either:
  `readChunk` moves the buffered first frame out rather than returning a copy and
  keeping the original alive for the reader's lifetime (a 64 MiB first frame used to
  stay resident until the reader was collected). Fragmentation, the control-frame
  replies, `maxMessageBytes` and the UTF-8 validation are unchanged, and the close
  echo still carries the whole payload while the message carries only the reason.
  The one visible change for direct users of the sans-io core: `offer` consumes the
  frame it is given, so its `payload` must not be read again afterwards (feed a
  reused `Frame` refilled by `WsDecoder.next`, as the backends do) (#411).
- **HTTP/2 appends header blocks straight from the frame decoder and decodes HPACK
  literals in place.** A HEADERS payload used to be materialized into a `Frame`,
  copied into a local fragment, sliced again to strip the padding and the 5-byte
  priority block, and copied once more into the stream's header buffer -- four
  copies of every header byte (a 100 KiB block moved ~400 KiB) before HPACK even
  ran, with each literal string sliced out a fifth time inside the decoder. HEADERS,
  CONTINUATION and PUSH_PROMISE now go through the same peek path DATA has used
  since #400: padding and priority are skipped by offset arithmetic and the fragment
  is appended to the header buffer with one setLen + copyMem, while `decodeString`
  copies a raw literal once and hands a Huffman-coded one to the decoder as a view
  over the block. Padding and priority validation, the 128 KiB header-block cap (now
  checked before the fragment is copied, not after), CONTINUATION sequencing (RFC
  9113 6.10) and every PROTOCOL_ERROR / FRAME_SIZE_ERROR / COMPRESSION_ERROR
  condition are unchanged (#413).
- **The SSE parser copies each line's value once and moves the event out on
  dispatch.** A line used to be materialized as a string slice, its value sliced out
  of that, and the leading space sliced off again before being appended to the data
  buffer, which `dispatch` then copied into the event: about five copies of every
  payload byte, each one an allocation plus a byte-at-a-time loop. The colon and the
  optional single leading space are now located in the parse buffer and the value is
  copied straight into its field (setLen + copyMem, the shape #400 gave h2), the
  field name is compared in place, `retry:` is accumulated in place with the same
  range check, and the event's buffers are moved rather than copied on dispatch. A
  16 MiB event (the `maxSseEventBytes` ceiling) no longer costs ~80 MB of byte-loop
  copying before delivery: a one-core parse of that event goes from ~41 MB/s to
  ~665 MB/s, and a 100k-small-event stream from ~16 MB/s to ~119 MB/s. Line endings
  (LF, CR, CRLF, and a CRLF split across feeds), the BOM strip, the `data:` join,
  the id-at-dispatch promotion and every size cap are unchanged (#410).
- **The WebSocket frame decoder advances a read cursor instead of front-deleting
  per frame.** `WsDecoder.next` deleted the consumed prefix after every frame, so a
  burst of small messages arriving in one read shifted the whole remainder down once
  per frame (O(N x buffered)); it now advances a cursor and `feed` compacts the
  consumed prefix in place, amortized, the way the h1 parser and the h2 frame decoder
  do. Decoding 1000 40-byte frames out of one 42 KB read goes from ~14.7 ms to
  ~17 us. Framing, fragmentation, control frames and the length caps are unchanged
  (#409).
- **HTTP/2 downloads decode DATA straight into the response body.** The frame
  decoder gained a peek/consume API, so a DATA payload is copied once from the
  decode buffer into the stream body instead of being sliced out into a `Frame`
  first, and the decoder now compacts its buffer in place (moveMem, capacity kept)
  rather than reslicing the leftover partial frame after every read. Nim's string
  slice is an allocation plus a byte-at-a-time loop, which made it ~80% of the
  sans-io h2 receive path; a one-core `feed` + `takeBody` + `ackRecv` microbench
  goes from ~1.6 GB/s to ~17 GB/s. Flow control, size caps and frame validation are
  unchanged (#400).
- **HTTP/2 streamed uploads frame each chunk straight from the producer's buffer.**
  A chunk that fits the send window goes into DATA frames without staging through
  the per-stream send buffer, and the buffer keeps its capacity across chunks instead
  of being reallocated per chunk (#297, #298).
- **HTTP/1.1 streamed uploads coalesce small producer chunks.** Body bytes are
  buffered up to 16 KiB (one TLS record) before each write, and the final chunk plus
  trailers ride the last write, so a chatty producer no longer costs one write and
  one TLS record per chunk. Chunks at or above the threshold are written directly
  (#299).
- **The HTTP/1.1 response parser holds at most one read of body bytes, and shifts
  its parse buffer less.** Chunk data is emitted as it arrives instead of being
  buffered whole, so a server that declares one multi-megabyte chunk no longer parks
  that chunk in the parser and the response size cap can fire mid-chunk; the
  consumed prefix is reclaimed in place (and only once the shift pays for itself)
  rather than by rebuilding the buffer on every read; and body bytes are appended
  straight out of the parse buffer with no slice temporary. A chunk whose data is not
  followed by CRLF is still rejected before the response can complete, so the
  connection is never pooled (#244).
- **A small buffered upload leaves with the request head in one write.** A body up
  to 16 KiB (one TLS record) is packed with the head, so the common small request
  costs one write and one TLS record instead of two; a larger body is still written
  separately and never copied to prepend the head. Streamed chunk sizes are written
  into the output buffer directly, without a formatted temporary per chunk (#244).
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
- **A `-d:naviHttp3` build compiles again with `--threads:off` (#450).** The sync
  WebSocket-over-h3 pump declared its `Channel` and `Thread` state at module scope,
  so `nim check -d:naviHttp3 --threads:off` failed with `undeclared identifier:
  'Channel'` before reaching any of navi's code, even though the sync `websocket`
  entry already refused the h3 transport on a threadless build with a clear error.
  The pump types and procs now live behind `when compileOption("threads")`, matching
  that gate, and the `wkH3` transport arm carries no state when it cannot be built.
  Nim 2 defaults to `--threads:on`, so only a project that opts out was affected;
  everything else in a threadless h3 build (all h3 requests and streaming, h1/h2
  WebSockets, and h3 WebSockets on the async clients, which use no pump thread) was
  already fine and stays so. The CI compile matrix now runs `nim check` over the
  three native entries with `-d:naviHttp3` in both thread modes, so the gap cannot
  reopen.
- **A TLS connection that ends without `close_notify` can no longer truncate a
  read-until-close body (#426).** All three native TLS read paths reported a
  transport close that arrives without a TLS `close_notify` -- an injected RST, a
  bare FIN, a crashed origin -- as the same clean EOF that `SSL_ERROR_ZERO_RETURN`
  produces. For a response with no `Content-Length` and no chunked framing (an
  HTTP/1.0 origin, a `Connection: close` error page, an un-chunked event stream)
  the close is the only body delimiter, so the h1 parser marked the body complete
  and navi returned a silently truncated 200. Length- and chunk-delimited bodies
  were never exposed: the parser already rejects those when they end early.
  The clean-vs-unclean distinction is now kept instead of discarded. Each native
  connection records whether its TLS stream ended with a `close_notify` (the sync
  and asyncdispatch backends in the `SSL_ERROR_SYSCALL` and catch-all branches of
  their `SSL_read` loops, the chronos pump when the transport EOFs or fails before
  OpenSSL reports `ZERO_RETURN`) and exposes it as a `closedCleanly` transport op,
  which is always true for a plaintext connection. The h1 body drain and the
  streamed-body/SSE chunk reader consult it and raise `IOError` ("TLS connection
  closed without close_notify; response may be truncated") when a read-until-close
  body ends on an unauthenticated close. Reads still return an EOF rather than
  raising at the primitive, so an EOF before any response bytes stays a keep-alive
  race the engine replays on a fresh connection, and servers that close idle
  keep-alive connections with a bare FIN keep working. Such a connection is never
  pooled (an until-close body is not reusable in the first place). A new interop
  test, `nimble tlsTruncate`, drives a Python TLS server that cuts the stream with
  and without `unwrap()` and asserts the rejection and the control case on all
  three native clients.
- **TLS session resumption now works on the chronos backend (#431).** `ChronosTls`
  freed its `SSL` without ever calling `SSL_shutdown`, and OpenSSL treats that as a
  bad session: `SSL_free` runs `ssl_clear_bad_session`, which marks the `SSL_SESSION`
  not_resumable. That object is the very one navi cached for the origin (the
  new-session callback stores the pointer OpenSSL handed it), so re-presenting it on
  the next connection bought nothing and every chronos connection did a full
  handshake -- a certificate chain and an extra round trip each time -- while the
  sync and asyncdispatch clients with the same config resumed. Peers also never
  received a `close_notify`. Both `close` and `closeSync` now clear the error queue
  and call `SSL_shutdown` before `SSL_free`, and `close` drains the write-BIO onto
  the transport first (bounded, best effort) so the alert actually reaches the peer.
  `resumeSessions` is on by default, so this is a latency win on the chronos client
  with no configuration change. A new interop check asserts the second connection to
  an origin reports a reused session, on all three native backends.
- **Cancellation is no longer swallowed by the chronos TLS pump (#430).** `feedIn`,
  the read that moves ciphertext off the transport into OpenSSL's read-BIO, caught
  `CatchableError` -- which in chronos includes `CancelledError` -- and reported it as
  a clean EOF. A structured cancel that landed while the pump was parked in `readOnce`
  therefore never reached the caller: with `timeouts.read` set, a stalled TLS response
  came back as an empty read instead of a `TimeoutError`, the engine saw an EOF before
  any response and raised `KeepAliveRaceError`, and the retry layer **replayed the
  request on a fresh connection** (any idempotent method, or any method carrying an
  `Idempotency-Key`) while the cancel that was meant to stop it sat in `cancelAndWait`;
  with a `timeouts.total` deadline or a `CancelToken`, the right error was still raised
  but only after that replay had run its course. The same blanket handler in the
  connect loop turned a cancelled handshake into "this address failed", so a cancelled
  connect went on to re-race the remaining addresses with no bound left. `feedIn` now
  re-raises `CancelledError` so it propagates out through `handshake`/`readSome`, the
  connect loop tears the attempt down and re-raises rather than moving to the next
  address, and `ChronosTls.close` shields its transport teardown with `noCancel`
  instead of catching the cancel (re-raising there would have leaked the SSL and its
  BIOs). The plaintext read path, which only ever caught `AsyncStreamError`, was
  already correct.
- **The chronos backend no longer discards establishment errors when a connect timeout
  is set, and never hands back an unverified Unix-socket TLS session (#420).** With
  `timeouts.connect` configured, `connect` called `withTimeout(establish(), ...)` and
  never read the establish future back. chronos completes `withTimeout` with `true`
  whenever the inner future has *finished*, a failure included, so every establishment
  error -- DNS, connection refused, handshake failure, chain or hostname verification,
  an SPKI pin mismatch, a rejecting verify callback -- was dropped and a half-built
  connection was returned. On the TCP path the caller then saw a generic "send on a
  closed connection" instead of the real reason, which the engine reclassifies as a
  keep-alive race and may replay; on the Unix-socket path, whose TLS branch had no
  failure teardown at all, `conn.tls` was still pointing at a live, fully handshaken
  SSL, so the request went out over a connection whose identity check had FAILED.
  `connect` now keeps the establish future and awaits it after the timeout check (the
  shape the asyncdispatch backend already used), and the Unix-socket branch runs the
  same teardown as the TCP one (close the SSL and the transport, clear `conn.tls`),
  which also stops it leaking an fd, an SSL and its BIOs on every failed handshake.
- **A PKCS#12 client credential now presents the intermediates the bundle carries
  (#425).** `usePkcs12` passed a nil CA out-param to `PKCS12_parse` and installed
  only the leaf and the key, so a client certificate issued by an intermediate CA
  and exported as `.p12` (the usual corporate shape: root -> issuing CA -> client,
  `openssl pkcs12 -export -certfile`) went on the wire bare. Servers that trust only
  the root -- `openssl s_server -CAfile root.pem`, nginx `ssl_client_certificate
  root.pem` -- could not build the path and rejected the handshake with "unable to
  get local issuer certificate", while the identical credential converted to PEM
  worked, because `useCertChainPem` does install the chain. Since `pkcs12File` has
  the highest precedence in `loadClientCert`, no other `TlsConfig` field could
  supply the missing intermediates. navi now asks `PKCS12_parse` for the CA stack
  and installs it with `SSL_CTX_set0_chain`, freeing it if the install fails.
- **An encrypted client key with no configured passphrase now fails instead of
  prompting on the terminal (#424).** `useKeyPem` passed a nil password callback to
  `PEM_read_bio_PrivateKey` whenever `tls.password` was empty, so OpenSSL fell back
  to `PEM_def_callback`, which prints `Enter PEM pass phrase:` on `/dev/tty` (or
  stdin) and reads synchronously. A service whose secret injection yielded an empty
  password therefore blocked inside `newTlsContext` -- and under asyncdispatch or
  chronos that is the whole event loop -- for as long as stdin stayed open, rather
  than raising the intended "could not read the private key (wrong password?)". navi
  now always installs its own `{.cdecl.}` callback, which hands OpenSSL the
  configured password or returns 0 (`PEM_R_BAD_PASSWORD_READ`) when there is none.
  A `keyFile` or `keyPem` configured without any certificate no longer reaches
  std/net's `newContext`, whose `SSL_CTX_use_PrivateKey_file` prompts the same way;
  it is now rejected up front as the misconfiguration it is (`keyPem` alone was
  previously ignored in silence).
- **The expected hostname is now bound into the TLS handshake, and a subject CN no
  longer rescues a certificate whose SANs all mismatch (#423).** On the sync,
  asyncdispatch and chronos backends nothing was written into the SSL's
  `X509_VERIFY_PARAM` before connecting: the chain was checked during the handshake
  but the identity only afterwards, by `verifyPeer`. A peer presenting a chain-valid
  certificate for some other name therefore passed OpenSSL's in-handshake
  verification, and an mTLS client sent it the client `Certificate` and
  `CertificateVerify` before the mismatch was noticed, disclosing its identity to a
  party hostname verification would have rejected. `newClientSsl` / `newClientSslMem`
  now call `SSL_set1_host` (DNS names) or `X509_VERIFY_PARAM_set1_ip_asc` (IP
  literals) before the handshake, so a mismatch aborts it before the client's second
  flight; `verifyPeer` stays as the redundant post-handshake check. The host-match
  flags also changed: `X509_CHECK_FLAG_ALWAYS_CHECK_SUBJECT` (inherited from std/net)
  is gone and `X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS` is set, matching the HTTP/3
  transport, so a certificate carrying dNSName SANs is judged on those SANs alone
  (RFC 9525) and partial wildcards such as `fo*.example.com` are rejected. A
  SAN-less certificate still matches on its CN, so private CAs that issue CN-only
  certificates keep working. Libraries too old to export the binding entry points
  (some LibreSSL builds) keep the previous post-handshake-only behaviour rather than
  failing to start.
- **The QUIC handshake is bounded by `connectMs`, and an h3 endpoint that will not
  connect is no longer retried on every request.** ngtcp2's `settings.handshake_timeout`
  was left at its `UINT64_MAX` default and neither the sync `drive_until` loop nor the
  asyncdispatch/chronos handshake loops carried a deadline, so the h3 leg was bounded
  only by the 30 s QUIC idle timer no matter what `connectMs` said. On the very common
  network that drops outbound UDP/443 to an origin advertising `Alt-Svc: h3`, that cost
  a ~30 s stall before the TCP fallback, and the Alt-Svc cache was only ever mutated on
  success, so the *next* request paid it again, and the one after that, until the
  advertisement's `ma` expired. `connectMs` (then `totalMs`, else a 30 s default) is now
  plumbed into the QUIC handshake as `settings.handshake_timeout` and bounds all three
  backends' handshake loops, and a handshake that fails before anything is submitted
  marks that origin's h3 alternative broken (RFC 7838 2.4) for a backoff window that
  doubles from 60 s up to 16 minutes, so later requests go straight to h2/h1. A
  successful h3 connection clears the backoff, and a re-advertisement of the same
  alt-authority deliberately does not (an origin repeats its `Alt-Svc` header on every
  TCP response, which would otherwise put the client straight back on the dead path);
  a *different* alt-authority starts clean. `openWsH3`'s docstring now matches what it
  does, since its `connectMs` bound previously started only after the QUIC handshake
  had already completed (#432).
- **HTTP/3 now honors the whole `TlsConfig`, not just `caFile` and `verify`.** The
  QUIC leg built its own `SSL_CTX` in `h3client.cpp` from those two fields alone, so
  `pinnedKeys`, `verifyCallback`, `caBundle`, the client credential
  (`pkcs12File`/`certPem`/`certFile`) and `ciphers`/`cipherSuites` were all silently
  dropped on h3 and `postHandshakeVerify` never ran there. In a `-d:naviHttp3` build
  H3 is in the default `http` set and the client upgrades to it after any `Alt-Svc`
  header, so the documented "replace verification entirely" mode (`verify = false`
  plus `pinnedKeys` or a `verifyCallback`) produced an *unauthenticated* QUIC
  connection to anything answering on UDP 443, and with `verify = true` a pin was
  bypassed on h3 exactly where a mis-issued certificate would have been caught. A
  `caBundle`-only private CA and mTLS instead failed the h3 handshake and fell back
  to h2/h1 with no signal, and an mTLS client was anonymous over QUIC. The FFI now
  carries navi's full TLS policy across as one struct (`NaviH3Tls`, in the new
  `backend/h3client.h`): the QUIC context adds the `caBundle` roots to its store,
  installs the client credential (PKCS#12 including its chain, or a PEM/DER chain and
  key, with an explicit password callback that fails rather than prompting on a tty),
  and applies the cipher selection. The peer's leaf is exported through two new FFI
  calls, so the SPKI pin comparison and the `verifyCallback` run from Nim right after
  the handshake and before the connection is bound into the pool or carries a
  request, on all three openers (`h3Open`, `openConnAsync`, `openConnChronos`) and
  `openWsH3`; a rejection raises the same `ValueError` with the same wording as the
  TCP backends, so the error surface does not depend on which leg was taken. QUIC is
  TLS 1.3 only (RFC 9001), so a `maxVersion` below TLS 1.3 now makes navi skip the
  advertised h3 endpoint and stay on h2/h1 rather than fail the request. With
  `verify` off and no pins or callback configured, behavior is unchanged (#419).
- **The sync backend's readiness waits use `poll(2)` (`WSAPoll` on Windows) instead of
  `select(2)`, so a connection on a descriptor above `FD_SETSIZE` no longer aborts the
  process or reports a bogus timeout (#429).** `waitReadable`, `waitWritable` and the
  Happy-Eyeballs race went through `std/nativesockets`' `selectRead`/`selectWrite`,
  which `FD_SET` the raw descriptor into a fixed 1024-bit `fd_set` with no range check.
  Inside a process already holding ~1024 descriptors (a server or worker with a raised
  `RLIMIT_NOFILE` that also makes outbound requests), navi's socket lands above that
  ceiling and the `FD_SET` either aborts the process on a fortified glibc build
  (`bit out of range 0 - FD_SETSIZE on fd_set`), corrupts the stack, or makes `select`
  fail with `EINVAL`. Since every call site only tested `> 0`, the `-1` was
  indistinguishable from an expiry, so a ready connection raised `TimeoutError`
  ("read timed out", or a connect timeout) on every request. It affected TLS and plain
  http alike, but only when a read, total or connect timeout was armed: with no
  timeout the wait is skipped entirely. The waits now poll a stack `pollfd` (the
  Happy-Eyeballs race reuses one buffer across rounds, so a wait still allocates
  nothing), `POLLHUP`/`POLLERR` count as ready so the following `recv`/`SO_ERROR`
  surfaces the real error exactly as before, `EINTR` retries with the time that is
  left, and a genuine poll failure now raises `IOError` with the errno text rather
  than passing for a timeout. Timeout semantics are unchanged. Windows was never
  affected (its `fd_set` is a counted array) and keeps an `SO_ERROR` fallback on the
  connect wait, because `WSAPoll` before Windows 10 2004 does not signal a failed
  connect.
- **A failed TLS handshake over a Unix socket no longer leaks a descriptor, an SSL
  and an SSL_CTX on the asyncdispatch client (#427).** The `pkUnix` branch of the
  backend's `connect` had no exception handler, so when `newClientSsl`,
  `driveHandshake`, `verifyPeer` or `postHandshakeVerify` raised (bad chain, hostname
  mismatch, SPKI pin failure, handshake error) the just-connected socket stayed
  registered on the dispatcher, the SSL was never freed, and with a bare `TlsConfig`
  (no client context store) the unshared SSL_CTX was never destroyed. Nothing
  downstream could reclaim them: the exception propagates out of `establish` before
  the connection is returned, a value-type `Conn` has no destructor, and the
  `connectMs` backstop only covers an establish that TIMED OUT, not one that failed.
  Every retry leaked another set. Both branches of `establish` now share one
  `tearDownAttempt` helper that frees the SSL, destroys an owned context and closes
  the socket, so the Unix path reclaims exactly what the TCP path always did (and,
  as a side effect, so does the `https` over a Unix socket without `-d:ssl` error).
  Measured over the new interop leg: 40 failing handshakes used to leak 40
  descriptors and now leave the count unchanged.
- **A send racing a connection close on the asyncdispatch client no longer writes
  through a freed TLS session (#421).** `sslRead` has long refused to read once the
  shared teardown flag reaches `csClosed`, because `freeConn` calls `SSL_free` before
  `closeSocket` and a parked read is woken by that `closeSocket`, i.e. after the free.
  The write side had no such guard. A `sslWrite` parked on `WANT_WRITE` (a body upload
  against a stalled peer, or the h2 mux's serialised writes when the reader hits a peer
  reset in the same tick) is not woken by the shutdown on Linux, where a reset reports
  readability and error but never `EPOLLOUT`: it stays parked right through
  `close`, and its continuation then called `SSL_write` on the dangling pointer. The
  same `csClosed` check now sits at the top of `sslWrite`'s loop, so it is re-run after
  every `WANT_READ`/`WANT_WRITE` retry as well as on entry, and the plaintext `sendAll`
  path consults the flag too (it only tested `fd == invalidFd`, which `freeConn` never
  sets on the Conn value copies the stream layers hold, so it could write to a
  descriptor number the process had already reused). Two neighbouring paths that could
  also run after `freeConn` were closed the same way: `shutdownConn` is now a no-op on
  a closed connection rather than shutting down a recycled descriptor, and an expired
  `recvWithin` no longer parks its abandoned read on a connection that was closed under
  it (which left the read unowned and its failure unobserved). Both parked writes and
  post-close sends now fail with a plain `IOError`, which the stream layers already
  treat as a connection drop.
- **A `TlsConfig` built with the object constructor no longer silently skips peer
  verification (#422).** `TlsConfig.verify` was a plain `bool`, so its zero value was
  `false` and the documented `TlsConfig(caFile: "corp-ca.pem")` hardening idiom (which
  the api.nim doc comment and the three wss examples recommended) produced a config
  with verification off: `newTlsContext` built a `CVerifyNone` context, `verifyPeer`
  returned before any chain, hostname or IP-SAN check, std/net never even loaded the
  supplied CA bundle, session resumption was off, and the same `wantsVerify = false`
  was forwarded to the HTTP/3 client. Any certificate from any peer was accepted with
  no error, while the field's own doc comment claimed the default was on. The field is
  now `insecureSkipVerify`, whose zero value (`false`) verifies, so a bare
  `TlsConfig()`, `TlsConfig(caFile: ...)`, `defaultTls()` and `initNaviConfig()` all
  authenticate the peer. `wantsVerify` is its inverse, the examples and the
  README/HARDENING/THREAT_MODEL prose were corrected, and `tls.verify` survives as a
  getter/setter pair so existing `cfg.tls.verify = false` opt-outs keep compiling.
- **The HTTP `CONNECT` proxy reply is now read to its blank line instead of with a
  single `recv`.** `proxyConnectDriver` took one read of at most 1024 bytes and only
  prefix-matched `HTTP/1.1 200` / `HTTP/1.0 200`. TCP does not guarantee the status
  line and the headers arrive together, so a proxy that flushes the status line first,
  or replies with more than 1024 bytes of `Via`/`X-Cache`/`Proxy-Agent` headers, left
  header bytes on the socket; OpenSSL then read them as the ServerHello and the
  request failed with a TLS handshake error rather than a tunnel error. The driver now
  loops on the backend read primitive until it sees `\r\n\r\n`, capped at 16 KiB
  (a longer head, or EOF before the terminator, raises a clear proxy error), parses the
  three-digit status code and accepts any 2xx per RFC 9110 9.3.6 instead of matching two
  literals, and puts the status line in the error text so a 407 or 403 is diagnosable.
  Bytes arriving after the blank line are now rejected with an explicit error rather
  than silently dropped: no backend can push them back into its TLS read path, and a
  conforming proxy never sends them because the TLS client speaks first. Covered by a
  new `tests/interop/http_connect.sh` (`nimble httpConnect`) that drives all three
  native clients through a split reply, an oversized reply and a 407 (#428).
- **`navi/js` request bodies now go on the wire as the Nim string's bytes, like every
  native backend.** `buildInit` handed the body to `fetch` as a `cstring`, and on the
  js backend that conversion decodes the string's bytes as UTF-8 into a JS (UTF-16)
  string, which `fetch` then re-encodes as UTF-8. The round trip is only the identity
  for a body that is already valid UTF-8: any other byte >= 0x80 was replaced with
  U+FFFD (`EF BF BD`), so binary uploads (gzip, images, `application/octet-stream`)
  arrived corrupted and longer than they were sent (a 256-byte body of every byte
  value arrived as 384 bytes, first mismatch at byte 128), and a latin1 text body was
  silently transcoded while its `Content-Type` charset still claimed the original
  encoding. The body is now passed as a `Uint8Array` of those bytes, so `fetch`
  transmits them verbatim. One knock-on: `fetch` no longer adds its default
  `Content-Type: text/plain;charset=UTF-8` to a raw string body that carries no
  Content-Type, which matches the native clients (a body whose type is implied, such
  as JSON or a form, still sets its own header). The streamed download side has been
  byte-exact since #412; this was the last lossy byte path in `navi/js`. Covered end
  to end under Node by `tests/interop/js_bytes.sh` (#417).
- **The sync WebSocket-over-h2 tunnel no longer buffers a peer flood without bound
  (#407).** Its Extended CONNECT stream now opens in sink mode and `receive` acks the
  bytes the application consumed, matching the async tunnel: while a large `send` waits
  for the peer's WINDOW_UPDATE, inbound DATA is bounded by the advertised receive
  window instead of being replenished eagerly frame by frame.
- **HTTP/1 header, chunk-size and WebSocket-handshake lines are now capped at
  `maxHeaderListBytes` (128 KiB) and scanned incrementally.** A peer that opened a
  status line, header field, chunk-size or trailer line and then streamed non-CRLF
  bytes forever grew the parse buffer without bound (`maxResponseBytes` caps only
  body bytes), and every 64 KiB read re-scanned the whole unterminated line, so CPU
  was quadratic in header size; the sync and async WebSocket `101` readers had the
  same unbounded accumulation. Both now stop at the shared cap h2 already applied to
  a CONTINUATION flood and raise the new `HeaderTooLargeError`, and the CRLF search
  resumes from the previously scanned offset instead of restarting at the read
  cursor. (#406)
- **A multiplexed HTTP/2 read no longer sizes every stream's body to the whole
  decoder buffer.** `handleData` sized each stream's per-batch body to
  `frames.remaining`, everything still buffered for the connection including other
  streams' frames, so on a mux every stream that got any DATA in a read allocated a
  read-sized string that then travelled through `takeBody` into the sink queue: one
  64 KiB read carrying a ~100-byte frame for each of 200 SSE subscriptions produced
  ~12.8 MB of live capacity for 20 KB of body, bounded in bytes by the connection
  window but not in capacity. The hint is now the run of contiguous DATA frames for
  that stream at the head of the buffer, so a stream is sized for what it actually
  receives; a single-stream download is still one run, keeping the #401 fast path
  (one sizing per read, no regrowth). (#408)
- **A decoded body is no longer cut short when the codec's output fills the 16 KiB
  decode scratch exactly.** The inflate loop stopped as soon as zlib had consumed
  every input byte, even when the scratch came back completely full; zlib pulls input
  into its own bit buffer, so that happens with a match still half written, and the
  pending bytes (plus the `Z_STREAM_END` behind them) were dropped. A headerless
  `deflate` body a few dozen bytes past a scratch boundary decoded to exactly 16384
  bytes and was then reported as truncated by the buffered path and delivered short by
  the streamed one. Both loops now drain the codec while it keeps filling the buffer,
  and the zstd loop does the same for a frame it has only partly flushed (#405).
- **An HTTP/3 connection now says goodbye: `close` sends a CONNECTION_CLOSE before
  freeing the connection.** The h3 driver used to drop its UDP socket and free the
  ngtcp2/nghttp3/OpenSSL state without telling the peer anything, so a server had no
  way to learn the connection was over and held its per-connection state until its own
  idle timer fired (~30 s for quic-go, which is what Caddy runs). Under connection
  churn that pins thousands of dead connections server side -- the sync h3 stress cell
  drove the Caddy front to 3.45 GB RSS against ~70 MB for the same load over the async
  clients. Teardown now writes a CONNECTION_CLOSE (H3_NO_ERROR once the handshake has
  completed, transport NO_ERROR before that) and sends that one datagram first, so the
  peer releases its state immediately; the write is best effort, and a connection
  already closing or draining is left alone. The client also advertises a 30 s
  `max_idle_timeout` now, which bounds peer retention even when the close datagram
  never arrives (a crash, a killed process, a lost packet); the existing 15 s
  keep-alive PING sits comfortably below it, so a pooled idle connection is unaffected.
- **The sync client pools HTTP/3 connections per origin instead of opening and
  closing one per request.** Only the async backends kept live h3 connections; the
  sync `h3Transport` did a full QUIC handshake for every request and tore it down in a
  `finally`, so a client talking h3 to one origin paid a handshake per request and
  left the server a dead connection behind each time. It now keeps one connection per
  origin (the QUIC twin of the h1/h2 pool), reusing it for the next request on a fresh
  stream, and drains the whole table in `close` and in the destructor leak-guard. A
  connection is dropped and closed on any transport failure, and, because the sync
  backend runs no background pump (nothing emits keep-alive PINGs between requests), a
  connection idle for more than 20 s is retired pre-emptively rather than probed -- a
  cold connect is provably pre-submit, so every method stays safe to send. If a pooled
  connection turns out to be dead anyway, the request is retried once on a fresh
  connection under the same replay rules the h1/h2 pool applies to a stale keep-alive
  socket. The sync streaming download/upload paths still use a connection per stream
  (one blocking drive loop owns the connection while the body is pulled).
- **The chronos client shuts a socket down before closing it, so the last write is
  delivered.** chronos's `closeWait` calls `closesocket` straight away, with no
  `shutdown` first; on Windows that could drop the bytes written just before the
  close, so a WebSocket close frame (or a TLS close_notify) sent right before
  `close` reached the peer as a bare EOF. `close` now sends FIN first (bounded to a
  second, best effort), matching what the asyncdispatch client already did.
- **A buffered compressed body that ends mid-member now raises instead of being
  returned part-decoded.** Buffered decoding no longer has its own copy of each
  codec: it feeds the body to the same decoder a streamed response uses, so the
  multi-member/multi-frame rules, the raw-deflate fallback and the size cap have one
  home and a buffered fetch cannot disagree with a streamed one about the same bytes.
  The visible change is truncation: a `br` or `zstd` body cut short used to come back
  silently partial, and now raises `IOError` with the same
  "compressed response body truncated" message the streamed path uses (a truncated
  gzip body already raised, as a generic malformed-body error) (#244).
- **A streamed `zstd` body made of several concatenated frames is decoded whole.**
  RFC 8878 allows a zstd body to be a run of frames back to back, the way RFC 1952
  allows a gzip body to be several members. The buffered path already decoded them
  all, but the incremental decoder marked itself done at the first frame end, so a
  streamed (or h2-muxed) multi-frame body was silently truncated to its first frame.
  A completed frame is now treated as a clean boundary and decoding continues while
  input remains, exactly as the gzip member path does. Trailing bytes after the last
  member/frame are still rejected rather than ignored (#244).
- **WebSocket over HTTP/3 now gates its Extended CONNECT on the server's
  `SETTINGS_ENABLE_CONNECT_PROTOCOL`.** The h3 path opened the CONNECT stream as soon
  as the QUIC handshake finished, without waiting for the peer's SETTINGS or checking
  that it allowed the extended CONNECT protocol, contrary to RFC 9220 / RFC 8441 3
  (navi advertised the setting to the server, which says nothing about the server).
  An origin that does not support WebSocket over h3 therefore failed late and
  obscurely, via a stream reset or an h3 error, where the h2 path fails immediately
  with a clear diagnostic. All three h3 clients (sync, asyncdispatch, chronos) now
  drive the connection until the peer's SETTINGS frame lands, bounded by the same
  connect/handshake deadline, and then raise the h2 path's `ProtocolError` -- "navi:
  server does not support WebSocket over HTTP/3 (no SETTINGS_ENABLE_CONNECT_PROTOCOL);
  use an h1 WebSocket" -- before anything is submitted (#393).
- **The h3 driver no longer drops stream data that arrives before its nghttp3 session
  is bound.** The session is created once the QUIC handshake completes, but the
  server's control stream (carrying its SETTINGS) can ride in the very datagram that
  completes the handshake, and ngtcp2 never re-delivers what the receive callback
  consumed, so those bytes were lost. They are now parked (bounded) and replayed into
  nghttp3 at bind time, with their flow-control offsets extended then. Found while
  adding the Extended CONNECT gate above, which otherwise waited forever for a
  SETTINGS frame that had already been thrown away (#393).
- **A misbehaving SSE server can no longer spin the reconnect loop.** The reconnect
  delay had no lower bound, so a server that sent `retry: 0` reduced it to
  `sleep(0)`, and a server that answered 200 and closed with no events reset the
  backoff on every connect (the reset was tied to a successful connect, not to a
  delivered event) -- either one reconnected as fast as the loop could run, hammering
  the server. Every client (sync, asyncdispatch, chronos, js) now floors every delay
  at the new `minRetryMs` (default 100 ms, itself capped by `maxRetryMs`), including
  a delay the server asked for with `retry:`, and doubles the delay after any connect
  that closed without delivering an event; only a connect that delivered at least one
  event resets it to the base (#291).
- **A redirect hop that drops a streamed upload no longer sends an empty chunked
  body.** `followRedirects` threaded the async body producer into every hop, so after
  a rewrite that drops the body (303 on any verb, 301/302 off a non-GET/HEAD method)
  the next hop went out as a GET with `Transfer-Encoding: chunked` and a producer that
  was already at EOF, i.e. a lone `0\r\n\r\n` body. Harmless on the wire for a
  conformant server, but a GET with a chunked body is something intermediaries log or
  reject, and it disagreed with the sync path, which sends nothing once `bodyStream`
  is nil. The producer is now dropped with the body, so such a hop is a plain bodiless
  request on every client and over h1 and h2 alike. A 307/308 hop is unaffected: a
  non-replayable body is still never followed there, the 3xx is surfaced (#395, #295).
- **`navi/proto/ws` compiles under `nim js` again.** The module's `js` branch was
  documented as a fallback that keeps a `nim js` build compiling, but the build
  failed: `checksums/sha1` reaches `std/endians`, which is native-only (`copyMem`),
  and the frame codec's word-wise masking and unmasked-payload copy call `copyMem`
  too. The js target now hashes the handshake accept with a small pure-Nim SHA-1
  (cross-checked against `checksums` in the unit suite), masks and copies byte-wise,
  and draws its masking keys and handshake nonces from the Web Crypto CSPRNG
  (`globalThis.crypto.getRandomValues`) instead of a fixed-seed `std/random`, raising
  rather than falling back to a predictable PRNG on a runtime without it. The 8-byte
  frame length is also emitted out of a `uint64` now, since a shift past 31 is
  undefined on a 32-bit `int`. navi/js still does not use this module at runtime (the
  runtime's `WebSocket` does the framing), but js is navi's only 32-bit-`int` target,
  so `tests/js_ws_codec.nim` now runs the RFC 6455 vectors and the #285 frame-length
  guard under Node in CI, where a truncating `int` is real (#394).
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
  A masked server frame (RFC 6455 5.1) is rejected on the streaming path too, as
  `receive` already did through `offer(rejectMasked = true)`, so a masked close can
  no longer be echoed back either; and a `WsReader` whose message was cut short by a
  rejected close reports `closeCode == closeProtocolError`, the code the connection
  was failed with. A synthetic EOF (no close frame at all) is still reported as
  1006, never validated or echoed (#283).
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
