# Plan: first-class Server-Sent Events (SSE)

## Goal

A first-class SSE client that parses the `text/event-stream` wire format, reconnects
transparently (Last-Event-ID + server `retry:` + backoff), and works uniformly
across all four backends, with arbitrary method/headers/body (which the browser
`EventSource` cannot do). Built as a thin layer over `stream()`.

```nim
let s = await api.sse(url, headers = {"authorization": "Bearer " & tok}.toHeaders)
while (let ev = await s.next(); ev.isSome):
  let e = ev.get
  if e.event == "done": break        # break works with the pull form
  await handle(e)
```

## Decisions (confirmed)

- **Consumption:** pull `next()` is primary (break-friendly, no raises-constraint on
  user code); `each(ev): body` is convenience sugar over it.
- **Reconnection:** full auto, on by default (Last-Event-ID resume, honor server
  `retry:` with exponential backoff to `maxRetryMs`, optional `idleTimeoutMs`).
  Configurable off.
- **js:** uniform fetch-based (navi/js `stream()` + the same parser/reconnect), so
  methods and custom headers work and behavior matches the native backends. The
  platform `EventSource` is not used.
- **Content-type: strict.** A `200` response whose `Content-Type` is not
  `text/event-stream` is an error (raise), matching `EventSource`.
- **`readChunk` first:** the pull chunk primitive that `next()` needs lands as its
  own PR before the SSE integration.

## API

```nim
type
  SseEvent* = object
    event*, data*, id*: string
    retry*: int                    # reconnect ms in effect, -1 if never set
  SseStream* = ref SseStreamObj    # one logical stream across reconnects

proc sse*(client: Navi, target: string,
          verb = GET, headers = initHeaders(), body = "",
          params: seq[(string,string)] = @[],
          lastEventId = "",
          reconnect = true, retryMs = 3000, maxRetryMs = 30_000,
          idleTimeoutMs = 0,
          cancel: CancelToken = nil): SseStream   # async: Future[SseStream]

proc next*(s: SseStream): Option[SseEvent]        # async: Future[Option[SseEvent]]
template each*(s: SseStream, ev, body): untyped    # sugar over next()
proc close*(s: SseStream)
proc lastEventId*(s: SseStream): string
```

## Design (three layers)

1. **Sans-io `SseParser`** (`src/navi/proto/sse.nim`, done): fed decoded UTF-8 text,
   emits `SseEvent`s. Owns the WHATWG parsing rules (fields, multi-line data,
   comments, `\r\n`/`\r`/`\n`, chunk-boundary splits, BOM, persistent last-id,
   NUL-id and non-integer-retry rejection). Unit-tested in `tests/test_sse.nim`. No
   sockets, no reconnection.
2. **`readChunk` pull primitive on `StreamResponse`** (separate PR): the deferred
   low-level `readChunk(s): Future[string]` (`""` at EOF), the drain-loop body
   factored to yield one decoded chunk (on the async h2 path it pops the mux
   `recvq`). Independently useful for break-friendly chunk loops.
3. **`SseStream`: the reconnect state machine over `stream()`.** `next()` returns a
   buffered parsed event, else `readChunk`s the current `StreamResponse`, feeds the
   parser, and buffers; on EOF/error it reconnects and continues, so callers never
   see the seam.

### Reconnect state machine

- **Open:** `stream()` with `Accept: text/event-stream` + `Cache-Control: no-cache`
  (+ `Last-Event-ID` when resuming), and internally force `maxResponseBytes = 0`
  and read/total timeouts off (a long feed must not be capped or idle-killed;
  liveness is `idleTimeoutMs`).
- **Status/type:** require `200` **and** `Content-Type: text/event-stream` (strict);
  otherwise raise, no reconnect. `204` means stop -> `next()` returns none. Redirects
  are already followed by `stream()`.
- **Track:** update the resume id on every event carrying `id`; update the reconnect
  delay on `retry:`.
- **On drop (EOF/error):** if `reconnect` and not closed, wait the current delay
  (server `retry:` as base, exponential to `maxRetryMs`, reset to base after a
  connection that delivered data), `reset()` the parser (keeps last id), reopen with
  `Last-Event-ID`, continue. Else return none (clean end) or raise (retries
  exhausted).
- **`idleTimeoutMs`:** if set and no bytes arrive for that long, treat the link as
  dead and reconnect.
- **`close()`:** dispose the current `StreamResponse`, mark closed; `next()` returns
  none.

### Lifetime

`SseStream` holds the current `StreamResponse` as a field, inheriting its
`StreamGuard`: dropping the stream without `close()` closes the connection via that
guard. So `SseStream` needs **no custom `=destroy`** (avoids the field-destruction
gotcha). On reconnect the old `StreamResponse` is closed before the new one opens.

### Per-backend

- sync: `next()` blocks; blocking retry sleep.
- asyncdispatch/chronos: `next()` returns a `Future`; async retry sleep; h2
  backpressure inherited.
- js: fetch-based `stream()`; chunks are `Uint8Array`, so decode via a streaming
  `TextDecoder` (handles a multi-byte char split across chunks) and feed text to the
  same parser. Reconnect is a re-`fetch`.

## Staging

1. **Sans-io `SseParser` + tests.** (done)
2. **`readChunk` pull primitive** on `StreamResponse`, all backends. (separate PR)
3. **`sse()` + `SseStream` + `next()`/`each`/`close` + reconnect** on native
   (sync, then async/chronos).
4. **js** (fetch-based + `TextDecoder`).
5. **Docs + interop:** an SSE server (Docker) that streams events and drops
   mid-stream, asserting events arrive, reconnection happens, and `Last-Event-ID`
   resumes correctly.

## Edge cases accounted for

Comments/keep-alives swallowed; comment-only or id/retry-only blocks do not
dispatch; `retry:` persists across reconnects; a clean server close with no `204`
still reconnects (EventSource treats any drop as reconnectable); UTF-8 boundary
splits are safe (line splitting is on ASCII newlines; js uses a streaming decoder).
