# navi stress workloads

Focused, Dockerized soak tests, split by **workload** (what the client does) with
protocol, backend, server count, compression, and runtime as configurable
dimensions. Each runs many navi clients against N TLS servers, prints a status +
memory report every interval (responses are tallied and discarded, so memory
stays flat over a long soak), and — for the streaming workloads — verifies a 1 GiB
checksum and fails hard on any mismatch.

## Tasks

| Task | Workload |
| --- | --- |
| `nimble stressRequests` | buffered GET/POST/PUT: bodies, compression, auth, middleware, pool/mux |
| `nimble stressWs` | persistent WebSocket, text + binary under load |
| `nimble stressSse` | SSE subscribe under load, reconnect + Last-Event-ID resume |
| `nimble stressStreamUpload` | stream 1 GiB up; server verifies checksum (hard-fail) |
| `nimble stressStreamDownload` | stream 1 GiB down; client verifies checksum (hard-fail) |
| `nimble stress` | short smoke of all five |

## Configuration (`NAVI_*` env)

| Var | Default | Meaning |
| --- | --- | --- |
| `PROTO` | `h2` | `h1` \| `h2` \| `h3` \| `all` (h3 uses the h3 image) |
| `BACKEND` | `all` | `sync` \| `asyncdispatch` \| `chronos` \| `js` \| `all` |
| `SERVERS` | `5` | server instances; requests round-robin across them |
| `SECONDS` | `60` | runtime per (backend × protocol) cell |
| `CLIENTS` | `3` | navi clients per backend |
| `CONCURRENCY` | `8` | in-flight requests per client (async fan-out) |
| `REQ_COMPRESSION` | `gzip` | request body: `none` \| `gzip` \| `deflate` (native; **octet/text only**) |
| `RESP_COMPRESSION` | `gzip` | response via `x-want-encoding`: `none` \| `gzip` \| `deflate` \| `br` \| `zstd` |
| `CONTENT_TYPES` | `octet,text,json,form` | csv restricting the `requests` /echo rotation; unknown tokens hard-fail at startup |
| `REPORT_SECONDS` | `60` | report cadence |
| `STREAM_BYTES` | `1073741824` | stream size (1 GiB); lower for a smoke |

The `requests` workload rotates four body kinds through `/echo`:

- **octet** (`application/octet-stream`) and **text** (`text/plain`): raw-string bodies, byte-verified. These are the only kinds `REQ_COMPRESSION` applies to -- the client zlib-compresses the body and sets `content-encoding`; the server decodes it first.
- **json** (`application/json`): sent as a navi `JsonNode` so the typed encoder is on the wire, **never request-compressed** (a `content-encoding` on a typed body would misdescribe the plain bytes and is the realistic shape for apps sending JSON through navi). The server parses and canonically re-serializes (sorted keys, compact separators); the client compares parsed trees, so a byte-echo cannot make it pass.
- **form** (`application/x-www-form-urlencoded`): sent as `form = @[(k, v)]`, also never request-compressed. The server parses with `parse_qsl` and re-serializes sorted; the client compares decoded pairs.

`RESP_COMPRESSION` (response via `x-want-encoding`) still applies to every native kind: the server compresses the canonical json/form echo and navi's response auto-decompression decodes it before the parse. `NAVI_CONTENT_TYPES=octet` reproduces the pre-catalog native behavior for bisecting.

Example:

```
NAVI_SECONDS=600 NAVI_PROTO=all NAVI_BACKEND=chronos \
  nimble stressRequests
```

## Chaos: the misbehaving-server sidecar (`NAVI_CHAOS`)

Opt-in hardening against hostile or broken servers. When `NAVI_CHAOS` is on,
`run.sh` launches a Python asyncio *chaos sidecar* per cell (offering only the
cell's pinned protocol) and the client attacks it with a seeded schedule of fault
modes **alongside** the normal verified soak, which keeps running as a canary. A
client bug that corrupts the shared event loop or allocator takes the canary down
too -- that is signal. Chaos traffic uses **separate `Navi` instances** (tight
timeouts, same proto pin + CA), so the canary's pools never contain a chaos
connection. Default `none` is byte-identical to a pre-chaos run: no sidecar, no
extra ports, no leak sampling, no chaos report lines.

The client asserts four invariants per cell: no crash/hang (every interaction is a
typed error or a valid `Response` within a watchdog), the protocol pin is never
violated, no FD leak, no memory leak. Violations use greppable prefixes
`CHAOS-FAIL(hang|pin|mode|fd|mem)` and route to the FAILURES banner.

**Modes** (h1 implemented; h2/h3 are follow-up phases). Strict modes assert an
exact outcome; tolerant modes accept any catchable typed error:

| Mode | Server behavior | Expected |
| --- | --- | --- |
| `stall` | reads the request, then silence | strict: clean timeout/abort |
| `slowbody` | 200 + big length, then drips slower than the read timeout | strict: `TimeoutError`/truncation |
| `truncate` | short body vs `Content-Length`, or a cut chunk (`?case=`) | tolerant; never a successful short body |
| `garbage` | malformed status line / colon-less header / `Content-Length: abc` / NULs / doubled (`?case=`) | tolerant; a later request still succeeds |
| `vanish` | `SO_LINGER=0` RST mid-body; `?prefix=slow`, `?at=pre-headers` | tolerant, within total incl. retries |
| `headerbomb` | thousands of 8 KiB header lines | tolerant; bounded memory (the heap assertion is the teeth) |
| `redirectloop` | valid `302` self-loop (`?n` increments) | strict: bounded 3xx once `maxRedirects` is spent |
| *(port-selected)* `vanish-on-accept` / `stall-on-accept` | RST / never-progress right after accept | tolerant / timeout |

The sidecar is a pure function of the request: the client's seeded schedule picks
the mode + coins and encodes them in the path/query, so both sides' logs name the
same mode and reruns with the same seed are identical. Ports derive from
`NAVI_BASE_PORT + NAVI_CHAOS_PORTBAND` (`+0` data, `+1` vanish-on-accept, `+2`
stall-on-accept, `+99` a plain-HTTP `/health` control port), loopback only.

**Backends:** asyncdispatch and chronos run the full async driver
(`NAVI_CHAOS_CONC` workers + an in-process hang watchdog); sync interleaves one
chaos request per `NAVI_CHAOS_CONC` verified requests (its hang backstop is navi's
timeouts plus a coreutils `timeout` wrapper). **js is excluded** (hostile-input
handling there is undici's, not navi's; js cells can't pin the protocol or measure
navi's FD/heap). The two pure never-respond modes (`stall`, `stall-on-accept`) are
skipped on **sync** only (a sync read-timeout gap on a zero-byte-response TLS
connection; async runs them).

**Leak checks** (Linux/Docker): FD is bracketed process-wide (`/proc/self/fd`
before any `Navi` vs after close+drain, `<= baseline + NAVI_CHAOS_FD_SLACK`). Nim
heap is checked against the pre-`Navi` baseline after a full GC (so only genuine
retention counts, not the soak's transient working set). RSS is baselined at the
pre-teardown peak and must not grow further through close+drain (pages are rarely
returned). Green runs print the margins.

| Var | Default | Meaning |
| --- | --- | --- |
| `CHAOS` | `none` | `none` \| `all` \| csv of mode names; unknown tokens hard-fail at startup |
| `CHAOS_CONC` | `8` | chaos workers per cell (async) / interleave basis (sync) |
| `CHAOS_SEED` | `1` | schedule PRNG seed, mixed with the cell id; printed as a digest |
| `CHAOS_PORTBAND` | `2000` | chaos ports = `NAVI_BASE_PORT + band {+0,+1,+2,+99}` |
| `CHAOS_WATCHDOG` | `60` | seconds a chaos worker may go without progress |
| `CHAOS_FD_SLACK` | `8` | FD leak bound |
| `CHAOS_HEAP_SLACK_MB` | `32` | Nim heap leak bound |
| `CHAOS_RSS_SLACK_MB` | `128` | RSS leak bound |
| `CHAOS_SELFTEST` | *(unset)* | `fd` \| `mem` \| `hang`: plant a deliberate leak/hang to prove the assertions fire (expects FAILURES) |

```
NAVI_CHAOS=all NAVI_PROTO=h1 NAVI_BACKEND=all nimble stressRequests
```

## Layout

- `common/` — shared native harness: `config` (env + gap policy), `reporter`
  (status counter + RSS from `/proc/self/statm`), `servers` (round-robin),
  `streamcontent` (fixed-block + incremental SHA-1), `httpset` (proto → version set),
  `chaos` (client-side chaos driver: seeded schedule, workers/watchdog, outcome
  classification; split into `chaos_async`/`chaos_sync` for the two backend models),
  `leakcheck` (FD/heap/RSS sampling + assertions).
- `chaos/` — the Python asyncio misbehaving-server sidecar: `chaos_server.py`
  (entrypoint + control port), `modes.py` (registry + wire helpers), `h1.py` (h1
  fault handlers), `h2.py`/`h3.py` (phase-2/3 stubs), `requirements.txt` (`h2`).
- `clients/` — one client per workload. The async source (`*.nim`) is built for
  both asyncdispatch and (`-d:useChronos`) chronos; `*_sync.nim` is the sync
  backend; `*_js.nim` runs under Node. run.sh skips any backend whose client
  source is absent, so partial backend coverage degrades gracefully.
- `server/app.py` — one FastAPI app (echo, ws, events, upload, download) served by
  hypercorn (h1/h2); Caddy fronts it for h3.
- `Dockerfile` (h1/h2) and `Dockerfile.h3` (adds the ngtcp2/nghttp3/OpenSSL-3.5
  client toolchain + Caddy). `run.sh` orchestrates: cert, N servers, the
  backend × protocol matrix, cleanup, and a final pass/fail banner.

## Notes

- `nimble` does not propagate a task's exit code (nim-lang/nimble#1802): read the
  final `== <workload>: all cells passed ==` banner, or run the `docker run`
  directly for an honest exit code.
- RSS is read on Linux (everything is Dockerized); the js backend reports
  `process.memoryUsage().rss`.
