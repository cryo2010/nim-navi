# Plan: pull-based streaming (`stream()` handle + `each`)

## Goal

Add a headers-first, pull-based streaming download API alongside the current
push `stream(url, sink)`:

```nim
proc download() {.async.} =
  let res = await api.stream(GET, url)     # returns after headers; body pending
  if res.status != 200: return             # decide from headers, before the body
  res.each(chunk):                         # no outer await (the template bakes it in)
    await writeToFile(chunk)               # chunk: owned string, moved in (no copy)
```

The consumer sees the response status/headers before deciding to read the body,
draining is incremental with cooperative backpressure, and each chunk is handed
over by **move** (navi's read buffer transferred to the sink, no copy).

### Locked decision: plain move only

`chunk` is always an owned `string`, moved out of navi's buffer. We deliberately
do **not** implement the two zero-copy-but-restricted variants that were
prototyped and measured:

- an `openArray` borrow (`eachView`), which is (0 copy, 0 alloc) but cannot cross
  an `await` (synchronous bodies only);
- a raw `ptr`+`len` view (`eachUnsafe`), which is (0 copy, 0 alloc) across an
  await but trades compiler-enforced safety for a "do not retain the pointer"
  invariant.

Rationale (verified in prototypes): across an `await`, safe Nim forces either one
allocation or one copy per chunk. Plain move pays one same-size allocation that
ARC recycles off a free-list, which is dwarfed by the per-chunk TLS-decrypt and
syscall costs. The restricted variants can be added later behind their own names
if profiling ever justifies them; they are recorded here as explicitly deferred.

## API surface

Per backend, mirroring how `BodySink` is already defined per entry (the `Future`
type and `await` differ):

- `stream*(client, verb, target, headers, params, cancel): Future[StreamResponse]`
  (sync returns `StreamResponse` directly). A new overload **without** a `sink`.
- `StreamResponse` (per-backend object) exposes the header snapshot
  (`status`, `reason`, `httpVersion`, `headers`, `ok`) plus:
  - `each(res, chunk): body` — template. On async backends it expands to
    `await res.drain(proc(chunk: sink string): Future[void] {.async.} = body)`;
    on sync it drops the `await`. The `sink string` parameter is baked in so the
    coroutine-env capture is also a move.
  - `drain(res, sink)` — explicit sink form (the current push semantics, now on a
    handle). `each` is sugar over this.
  - `close(res)` — dispose a handle whose body will not be drained.

The free-standing `stream(url, sink)` is **removed** (breaking, fine pre-1.0). The
explicit sink form becomes `stream(url).drain(sink)`; `each` is sugar over that.
Removal is per-backend as each stage lands; the shared `performStream` template is
deleted once every backend has migrated (final stage).

### Why `each` bakes in `await`

`await res.each(chunk): body` mis-parses: `await` swallows the colon block, so
`each` never receives `body`. Putting the `await` inside the template fixes it and
callers write `res.each(chunk): body` inside an async proc. (Verified in the
prototype.)

## Design

### 1. Split the exchange at the header boundary

The core change is separating "send request + read headers" from "drain body" so
a handle can be returned in between. Today `h1Exchange` / `h2Stream` (in
`core/engine.nim`) and the mux's `drainDownload` (in `backend/h2mux.nim`) do both
in one loop.

Refactor into two reusable pieces per protocol, preserving current behavior:

- **h1**: `h1SendAndReadHeaders(transport, req) -> H1Parser` (loop `recvSome` +
  `feed` until `parser.headersReady()`, using the new predicate already added to
  `proto/h1.nim`), and `h1DrainBody(transport, parser, sink, decompress, cap)`
  (the existing post-header loop: `takeBody` -> decode -> cap -> `await sink`).
  The current `h1Exchange` becomes `readHeaders` then `drainBody`, so the push
  path is unchanged.
- **h2 (sync, pooled)**: same split of `h2Stream` into header-wait and
  body-drain over `proto/h2/conn` (`respHeader`, `takeBody`, `setSinkMode`,
  `ackRecv`).
- **h2 (async mux)**: `muxRequest` already yields once headers are in; expose that
  point, and reuse `drainDownload` as the body-drain (it already pulls `recvq`,
  decodes, moves to the sink, and `ackRecv`s the gated window).

### 2. `StreamResponse` holds what draining needs

A per-backend object carrying: the header snapshot (`Response`), the live
transport (or mux + stream id), the parser / h2 conn state, `decompress`, the
size `cap`, the `cancel` token, and the pool key + client (to return or close the
connection after drain). `drain` runs the appropriate `drainBody`, then applies
the same connection-reuse decision the push path uses today (`keepAliveAfter` /
`canReuse` -> `pushIdle`, else `close`).

### 3. Lifetime of an undrained handle

A handle whose body is never fully read pins a connection. Handle this with:

- `close(res)`: if not fully drained, **always close** the underlying connection
  (h1) or reset the stream (h2 `RST_STREAM`), and drop it from the pool
  bookkeeping. We do not attempt a bounded drain to keep it poolable: draining an
  arbitrarily large abandoned body to save one socket is a bad trade.
- `each` closes automatically on normal completion and on early `break` /
  exception (partial body -> not reusable -> close).
- A `=destroy` hook on `StreamResponse` as a backstop that closes a never-drained
  handle, so a forgotten handle cannot leak a connection.

### 4. Policy interactions (what applies to the pull API)

- **Redirects**: followed internally before returning the handle, so the snapshot
  is the final response. Reuse `followRedirects` up to the terminal response.
- **Retry**: transport/connection retries apply only up to header receipt (a
  streamed body cannot be replayed). Reuse the existing pre-header retry loop.
- **Digest 401**: handled before returning the handle (re-request), as today.
- **Cookies**: stored from the header snapshot (available at header time).
- **Decompress + size cap**: applied incrementally during `drain` (the decoder +
  running cap already live in the drain loop).
- **Cancel / total timeout**: honored during `drain` (thread the token/deadline
  into the handle and the drain loop, reusing the `guard` mechanism).
- **Throw-on-non-2xx**: **off** for the pull API. You get the handle and inspect
  `res.status` yourself (you may want to stream an error body). Documented.
- **Middleware**: **out of scope for v1.** Middleware wraps a buffered
  request/response and cannot meaningfully wrap a streamed body. Document that the
  pull API is not run through the middleware chain; `stream(url, sink)` and
  `request()` keep it. Revisit if a streaming-aware middleware shape is designed.

## Staging (each stage compiles all entries, `nimble test` green)

1. **Engine refactor + sync backend.** Split `h1Exchange`/`h2Stream` at the header
   boundary (push path behavior unchanged), add `StreamResponse` + `stream()`
   overload + `drain`/`each`/`close` for the sync entry, plus lifetime handling.
   Tests against the in-process server (`tests/support.nim`). This nails the API
   shape and the pooling/lifetime contract on the simplest backend. Get sign-off.
2. **asyncdispatch backend.** Header-first `muxRequest` + reuse `drainDownload`
   for h2, and the `h1OnConn` split for h1. This is the hard one (mux concurrency,
   `ackRecv` gating, `=destroy` timing under the event loop).
3. **chronos backend.** Mirror asyncdispatch; BearSSL/transport differences and
   `withTimeout`/deadline integration.
4. **js backend.** Naturally pull-based: `fetch` gives headers immediately and a
   `ReadableStream` reader; `each`/`drain` pull from the reader (reuse
   `drainToSink`). `chunk` stays `seq[byte]` here (bytes come from a JS
   `Uint8Array`), consistent with the existing js `BodySink` asymmetry.
5. **Docs, demos, consolidation.** README "Streaming" section (pull + push),
   `TESTING.md` coverage matrix, a `demos/streaming` pull example, and the
   `stream(url, sink)` = `stream(url).drain(sink)` reframe.

## Testing

- **End-to-end (per backend, in-process server)**: `stream()` exposes status +
  headers before the body; `each` drains the full body; `calls > 1` (incremental,
  not buffered into one); a slow sink applies backpressure; early `break` and an
  exception in the block both close the connection (assert it is not reused /
  no leak); an undrained-then-dropped handle is reclaimed by `=destroy`.
- **Policy**: redirect to a streamed body returns the final headers; size cap
  raises mid-drain; decompress decodes on the way to the sink; cancel aborts a
  drain in progress.
- **Interop**: add a pull-drain case to `tests/interop/nghttpd_async.nim` and the
  httpbin matrix (all four backends).
- **Copy-freedom**: inherited from the already-verified `sink string` path (the
  `each`-generated proc uses a `sink` parameter, and `drain` hands over by move).
  Note the prior `--expandArc` verification rather than re-asserting through the
  string API.

## Out of scope / deferred

- The `openArray` view (`eachView`) and `ptr`+`len` (`eachUnsafe`) variants.
- Streaming-aware middleware.
- A low-level `readChunk(res): Future[string]` pull primitive (an inlined
  `while` alternative to `each`). `each` is built on `drain(sink)` for v1; the
  primitive can be added later if a non-callback loop is wanted.

## Resolved decisions

- Name: `stream` (confirmed).
- `stream(url, sink)` is removed; use `stream(url).drain(sink)`.
- `close`/`=destroy` on an undrained handle always closes (no bounded drain).

## Known constraints

- `each`'s `body` runs as a proc, so `break`/`continue`/`return` cannot escape the
  loop from inside it. Early stop is via `close()` (don't call `each`) or by
  raising from `body` (which closes the connection and propagates). A future
  low-level `readChunk`/inlined-loop primitive could support `break`; deferred.

## Status

- **Stage 1 (sync backend): done.** Engine split (`h1SendAndReadHeaders`/
  `h1DrainBody`, `h2SendAndReadHeaders`/`h2DrainBody`; `h1Exchange`/`h2Stream`
  now compose them, push behavior unchanged), `h1.headersReady`,
  `h2.headersReady`/`respSnapshot`, sync `StreamResponse` + `stream`/`each`/
  `drain`/`close`/`=destroy`, sync `stream(url,sink)` removed, tests migrated +
  pool-lifecycle tests added. All four entries compile; `nimble test` green.
- Stages 2-5 (asyncdispatch, chronos, js, docs/consolidation): pending.
