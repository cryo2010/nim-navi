# Plan: HTTP/3 on the asyncdispatch backend

## Goal

Extend transparent HTTP/3 dispatch (sync path, shipped in phases 2a-2g) to the
`navi/asyncdispatch` backend, without blocking its event loop, and set up the
multiplexing that is h3's main advantage. As on sync, `api.get(url)` upgrades to
h3 after an Alt-Svc advertisement; the difference is that concurrent requests to
one origin should share a single QUIC connection.

Non-goals here: chronos (BearSSL, no QUIC) and `navi/js` (the runtime does h3).

## The problem

`backend/h3client.c` is a **blocking** driver: `navi_h3_open`/`navi_h3_request`
own a UDP socket and run their own `poll()` loop (`run_until`) until the handshake
or a request completes. That is fine for sync, but calling it from an
asyncdispatch proc would freeze the whole event loop for the duration of every
QUIC exchange. The async backend must drive the QUIC socket through
`std/asyncdispatch` (AsyncFD readiness + timers), never a blocking `poll`.

## Options considered

- **A. Off-thread blocking driver.** Keep h3client.c as-is; run each request on a
  worker thread and complete a Future from it (via an `AsyncEvent`/pipe). Least
  code, but spawns a thread per request, needs cross-thread marshalling of the
  GC'd result, and **cannot multiplex** (each request owns a thread + connection).
  Rejected: it throws away h3's reason for existing on the async backend.

- **B. Rewrite the whole client in Nim FFI.** Port every ngtcp2/nghttp3 call and
  the ~15 callbacks to Nim and drive I/O with asyncdispatch. Maximal control, but
  duplicates the large, already-verified C driver and doubles the surface to
  maintain. Rejected: cost without benefit over C.

- **C. Non-blocking step-function C core, driven by a Nim async I/O loop.**
  Refactor h3client.c so it never does I/O or sleeps: it exposes "advance the
  state machine" functions (feed a datagram, produce the next datagram, report the
  next timeout, submit a request, read response state). Nim owns the UDP socket
  (an AsyncFD) and the loop: drain outgoing datagrams to `send`, `await` readiness
  or the QUIC timeout, feed incoming datagrams or handle expiry. **Chosen.** It
  reuses the verified ngtcp2/nghttp3 logic, keeps navi's "navi drives I/O" model
  (as the design doc intends), and naturally supports multiplexing: one Nim loop
  can service many streams on one connection. The sync path is re-expressed as a
  trivial blocking loop over the same step functions, so both backends share one
  core.

## The step-function C API (h3client.c)

Replace the monolithic open/request with a non-blocking core (names indicative):

```c
navi_h3_conn *navi_h3_new(host, port, sni, ca_file, verify); /* no I/O, no block */
int      navi_h3_fd(navi_h3_conn *c);                 /* the UDP socket, for AsyncFD */
ngtcp2_ssize navi_h3_send(navi_h3_conn *c, uint8_t *buf, size_t buflen);
                                                       /* next datagram to send, 0 = none */
int      navi_h3_recv(navi_h3_conn *c, const uint8_t *pkt, size_t len); /* feed one datagram */
uint64_t navi_h3_timeout_ms(navi_h3_conn *c);          /* ms until next expiry, or a large cap */
int      navi_h3_handle_timeout(navi_h3_conn *c);      /* run loss-recovery on expiry */
int      navi_h3_handshake_done(navi_h3_conn *c);
int      navi_h3_bind(navi_h3_conn *c);                /* nghttp3 control/QPACK, after handshake */
int64_t  navi_h3_submit(navi_h3_conn *c, method, path, headers, body, body_len); /* -> stream id */
int      navi_h3_stream_done(navi_h3_conn *c, int64_t sid);
/* + per-stream getters: status, body, response headers */
```

Per-request response state moves from one-in-flight fields to a small per-stream
table (keyed by stream id), which is what unlocks multiplexing. The blocking
`run_until` becomes the sync wrapper's own loop over `navi_h3_send` /
`poll(fd, navi_h3_timeout_ms)` / `navi_h3_recv`.

## The Nim async I/O loop (asyncdispatch)

```nim
# register the UDP fd once; then, until the target stream(s) complete:
while true:
  var buf: array[1500, byte]
  while (let n = navi_h3_send(c, addr buf[0], buf.len); n > 0):
    await sendTo(fd, ...)                 # drain outgoing datagrams
  let t = navi_h3_timeout_ms(c)
  await (waitRead(fd) or sleepAsync(t.int))   # readiness OR the QUIC timer
  if fd readable: while (recvFrom -> pkt): navi_h3_recv(c, pkt)
  else:           navi_h3_handle_timeout(c)
```

`waitRead(fd) or sleepAsync(t)` (both are `Future[void]`) gives "whichever comes
first" without a blocking poll. UDP recv/send use the AsyncFD path
(`asyncdispatch` readiness + a non-blocking `recvfrom`/`sendto`).

## Multiplexing

A `QuicConn` gets a **background reader** future per connection (mirroring
`backend/h2mux.nim`): the loop above runs continuously while the connection is
live, and each in-flight request awaits a per-stream `Future[Response]` that the
reader completes on that stream's end. Concurrent `api.get`s to one origin then
share the connection. The async client keeps a `TableRef[origin, QuicConn]` next
to the existing `muxes` table, reusing a live h3 connection across requests.

## Integration

Mirror the sync work, in `navi/private/asyncdispatch_impl.nim`:

- Add a gated `altSvc: AltSvcCache` to the async `Navi`, init in `newNavi`.
- In the async `transport` (the `Future[Response]` proc that today branches
  h2-mux vs h1), add a gated branch: a buffered-body request to an origin with a
  cached h3 endpoint goes over h3 (via the QuicConn reader), falling back to
  h2/h1 on `QuicError`. Capture `alt-svc` from h2/h1 responses into `altSvc`.
- Verb/body/header forwarding, cert verification, and compression are unchanged:
  the C core is shared with sync, so those behaviors come for free.

All h3 code stays behind `-d:naviHttp3`; default async builds are unaffected.

## Implementation slices

1. **Non-blocking core + sync wrapper.** Refactor h3client.c to the step-function
   API; re-express `navi_h3_open`/`navi_h3_request`/`navi_h3_close` as a blocking
   loop over it. No behavior change; the existing sync interop tests must stay
   green. This is the prerequisite and is verifiable on its own.
2. **Single async request.** The Nim async loop + a one-request-in-flight async
   h3 path on asyncdispatch, with transparent dispatch and Alt-Svc capture. A new
   async interop test (h2 first, h3 second) mirrors the sync one.
3. **Multiplexing.** The per-connection background reader + per-stream futures +
   the `TableRef[origin, QuicConn]` reuse; a test issues concurrent GETs over one
   h3 connection.
4. **Leak/sanitizer coverage** for the async h3 path (the UDP socket + C library
   make this valuable), extending tests/leakcheck once the async path exists.

## Risks

- **Event-loop correctness.** The readiness-or-timer loop and the background
  reader are the subtle parts; getting the timer cadence wrong stalls ACKs/loss
  recovery. Mitigated by slicing (a working single-request loop before mux).
- **Lifetime/cancellation.** The reader future and per-stream futures must be torn
  down cleanly on `close`/cancel (h2mux's shutdown is the reference).
- **Scope.** This is larger than phases 2a-2g and is expected to land across
  several PRs (the slices above), not one.
