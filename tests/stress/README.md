# navi stress workloads

Focused, Dockerized soak tests, split by **workload** (what the client does) with
protocol, client, server count, compression, and runtime as configurable
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
| `CLIENT` | `all` | `sync` \| `asyncdispatch` \| `chronos` \| `js` \| `all` |
| `SERVER_COUNT` | `5` | server instances; requests round-robin across them |
| `SECONDS` | `60` | runtime per (client × protocol) cell |
| `CLIENT_COUNT` | `3` | concurrent navi client instances per cell |
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
NAVI_SECONDS=600 NAVI_PROTO=all NAVI_CLIENT=chronos \
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

**Modes** (all three protocols implemented). Strict modes assert an exact outcome
class and hard-fail on a miss; tolerant modes accept any catchable typed navi
error (truncation-vs-reset classification legitimately differs across clients, so
the class is tallied, not enforced). Every mode is also subject to the four
universal invariants:

| Mode | Protos | Server behavior | Expected |
| --- | --- | --- | --- |
| `stall` | h1 h2 h3 | reads the request, then silence forever | strict: `TimeoutError` |
| `slowbody` | h1 h2 h3 | 200 + big length, then drips slower than the read timeout | strict: `TimeoutError`/truncation mid-body |
| `truncate` | h1 h2 h3 | h1: short body vs `Content-Length` / cut chunk; h2: partial DATA then RST_STREAM or close; h3: partial DATA then RESET_STREAM (`?case=`) | tolerant; never a successful short body |
| `garbage` | h1 h2 h3 | h1: malformed status line / colon-less header / `Content-Length: abc` / NULs / doubled; h2: corrupt HPACK, unknown frame types, DATA on stream 0; h3: raw bytes / bogus frame types / duplicate SETTINGS (`?case=`) | tolerant; a later request on a fresh connection still succeeds |
| `vanish` | h1 h2 h3 | mid-response abrupt death: `SO_LINGER=0` RST (h1/h2), `quic.close`/silent UDP drop (h3); `?prefix=slow`, `?at=pre-headers` | tolerant, within total incl. retries |
| `badframes` | h2 h3 | h2 (`?case=`): frame over the client's `SETTINGS_MAX_FRAME_SIZE`; `INITIAL_WINDOW_SIZE=2^31` (FLOW_CONTROL_ERROR); zero-increment WINDOW_UPDATE; HEADERS on stream 0. h3: control-stream garbage, second SETTINGS | tolerant; violation detected, connection closed, no hang |
| `zerowindow` | h2 | `INITIAL_WINDOW_SIZE=0` in handshake SETTINGS, never a WINDOW_UPDATE; a POST with a 64 KiB body that can never flush | strict: `TimeoutError`, stream RST by client, FD reclaimed |
| `headerbomb` | h1 h2 h3 | h1: thousands of 8 KiB header lines; h2: giant HPACK over HEADERS+CONTINUATION + a never-ending CONTINUATION (capped ~64 MiB); h3: one giant QPACK block | tolerant; bounded memory (the heap assertion is the teeth) |
| `redirectloop` | h1 h2 h3 | valid `302` self-loop (`?n` increments) | strict: bounded 3xx once `maxRedirects` (default 20) is spent |
| *(port-selected)* `vanish-on-accept` / `stall-on-accept` | h1 h2 h3 | RST / never-progress right after accept | tolerant / timeout |

**Established protocol deviations.** `zerowindow` is **h2-only**: aioquic grants
QUIC flow-control credit automatically inside `transmit()`, so an h3 server cannot
starve the client's window. The two pure never-respond modes (`stall`,
`stall-on-accept`) are **skipped on the sync client only** (a sync read-timeout
gap on a zero-byte-response TLS connection; a documented follow-up); async runs
them. **js is excluded from chaos entirely** (hostile-input handling there is
undici's, not navi's; js cells can't pin the protocol or measure navi's FD/heap),
and prints a skipReason-style notice.

**h3 reachability (redirectloop's strict pass).** navi has no direct-dial h3: like
a browser it reaches h3 only via Alt-Svc discovery, so a UDP-only chaos port is
unreachable by construction (every request dies with connection-refused). Each
QUIC band port is therefore paired with a **TCP+TLS Alt-Svc discovery leg** on the
same port number (ALPN `http/1.1`) that serves exactly one protocol-valid `302`
redirect to the same target carrying `Alt-Svc: h3=":<port>"`. navi follows the
redirect, caches the endpoint, and the next hop lands on QUIC where the real mode
runs. `redirectloop` is the mode that exercises this leg end-to-end; the tolerant
modes would otherwise launder the connection-refused into their error tallies and
hide the vacuity.

**close() stabilizing sweep.** Because asyncdispatch has no true cancellation, a
request whose per-attempt timeout fired leaves its connect frame *abandoned* but
still running; it finishes and caches a fresh connection *after* a single-pass
teardown drained the tables, orphaning that connection's reader and fds. So
`Navi.close()` runs a **bounded stabilizing sweep** (drain the shared-connection
tables, yield a tick for any straggler to land, repeat until a pass finds them
stable-empty, capped at 64 sweeps) instead of one pass. This is general navi
behavior; the h3 stall/slowbody chaos modes are what surfaced and now guard it
(one leaked UDP socket + wake-pipe per un-reaped abandoned h3 connect otherwise).

The sidecar is a pure function of the request: the client's seeded schedule picks
the mode + coins and encodes them in the path/query, so both sides' logs name the
same mode and reruns with the same seed are identical. Ports derive from
`NAVI_BASE_PORT + NAVI_CHAOS_PORTBAND` (`+0` data, `+1` vanish-on-accept, `+2`
stall-on-accept, `+99` a plain-HTTP `/health` control port), loopback only.

**Clients:** asyncdispatch and chronos run the full async driver
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

**Hard-fail prefixes.** Every violation routes through the existing hard-fail path
to the FAILURES banner with a greppable prefix: `CHAOS-FAIL(mode)` (a strict mode
missed its expected outcome class), `CHAOS-FAIL(pin)` (a chaos `Response` failed
the cell's `checkVersion`), `CHAOS-FAIL(hang)` (the watchdog found a worker stuck
past `NAVI_CHAOS_WATCHDOG`), `CHAOS-FAIL(fd)` and `CHAOS-FAIL(mem)` (a leak bound
was exceeded at final sampling). A true crash class (uncaught exception, defect,
signal, or the external `timeout` wrapper firing) fails the cell directly, not via
a `CHAOS-FAIL` line.

**Self-tests** (`NAVI_CHAOS_SELFTEST`, off by default) plant a deliberate
client-side leak or hang so each assertion can be proven to have teeth; a green run
under a self-test is itself the bug, so these **expect the FAILURES banner**:

- `fd`: every interaction dups and retains a never-closed fd, so the final
  `/proc/self/fd` count blows past the slack: `CHAOS-FAIL(fd): baseline=N final=M
  (slack=8, bound=...) <label>`.
- `mem`: retains a 4 MiB body in a global every other interaction until the
  retained heap clears the slack: `CHAOS-FAIL(mem): heap baseline=... final=...
  (slack=32MB) <label>`.
- `hang`: worker 0 sleeps forever, so the watchdog must catch it within
  `NAVI_CHAOS_WATCHDOG + ~15s`: `CHAOS-FAIL(hang): <label> worker 0 stuck Ns
  (watchdog=60s)`.

**Seeded schedule + digest.** The schedule is driven by an explicit seedable PRNG
(xoshiro256\*\*, not `std/random`'s shared state), seeded with `NAVI_CHAOS_SEED`
mixed with a stable hash of `workload|proto|client`, so cells differ but reruns
are byte-identical. At startup each cell prints a schedule line, e.g.
`[requests h1 chronos chaos] seed=1 modes=stall,slowbody,... digest=<16 hex>`. The
digest is a purely structural hash of `seed` + the resolved proto-filtered mode
sequence (it draws no PRNG), so two runs with the same seed and knobs print an
identical digest and a different seed changes it; reproducibility is verified by
comparing digests (attempt tallies can wobble slightly with timing).

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
NAVI_CHAOS=all NAVI_PROTO=h1 NAVI_CLIENT=all nimble stressRequests
```

## Layout

- `common/` — shared native harness: `config` (env + gap policy), `reporter`
  (status counter + RSS from `/proc/self/statm`), `servers` (round-robin),
  `streamcontent` (fixed-block + incremental SHA-1), `httpset` (proto → version set),
  `chaos` (client-side chaos driver: seeded schedule, workers/watchdog, outcome
  classification; split into `chaos_async`/`chaos_sync` for the two client models),
  `leakcheck` (FD/heap/RSS sampling + assertions).
- `chaos/` — the Python asyncio misbehaving-server sidecar: `chaos_server.py`
  (entrypoint + control port), `modes.py` (registry + wire helpers), `h1.py`,
  `h2.py` (hyper-h2 decoder + raw-frame writer), `h3.py` (aioquic modes + the
  TCP Alt-Svc discovery leg), `requirements.txt` (`h2`).
- `clients/` — one client per workload. The async source (`*.nim`) is built for
  both asyncdispatch and (`-d:useChronos`) chronos; `*_sync.nim` is the sync
  client; `*_js.nim` runs under Node. run.sh skips any client whose source
  is absent, so partial client coverage degrades gracefully.
- `server/app.py` — one FastAPI app (echo, ws, events, upload, download) served by
  hypercorn (h1/h2); Caddy fronts it for h3.
- `Dockerfile` (h1/h2) and `Dockerfile.h3` (adds the ngtcp2/nghttp3/OpenSSL-3.5
  client toolchain + Caddy). `run.sh` orchestrates: cert, N servers, the
  client × protocol matrix, cleanup, and a final pass/fail banner.

## Notes

- `nimble` does not propagate a task's exit code (nim-lang/nimble#1802): read the
  final `== <workload>: all cells passed ==` banner, or run the `docker run`
  directly for an honest exit code.
- RSS is read on Linux (everything is Dockerized); the js client reports
  `process.memoryUsage().rss`.
- CI runs a nightly chaos rotation (`.github/workflows/stress-chaos.yml`): the full
  `NAVI_CHAOS=all NAVI_PROTO=all NAVI_CLIENT=all` matrix (must end `all cells
  passed`) plus a `NAVI_CHAOS_SELFTEST=fd` job that inverts the exit code and passes
  only when the FD assertion fails the run. Both use `docker run` directly, not
  nimble, so the container exit code is honest.
