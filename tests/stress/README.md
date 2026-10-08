# navi stress workloads

Focused, Dockerized soak tests, split by **workload** (what the client does) with
protocol, client, **server**, server count, compression, and runtime as
configurable dimensions. Each runs many navi clients against N TLS servers, prints
a status + throughput + memory report every interval (responses are tallied and
discarded, so memory stays flat over a long soak), and, for the streaming
workloads, verifies a 1 GiB checksum and fails hard on any mismatch.

`NAVI_SERVER` chooses what those servers are: the FastAPI/hypercorn + Caddy +
aioquic stack (default) or a native Nim [vortex](https://github.com/cryo2010/nim-vortex)
origin. See [Choosing the server](#choosing-the-server).

## Report lines

Every cell prints one line per `REPORT_SECONDS` and a final `== ... passed (...) ==`
banner. The throughput field on the interval line is the rate over the window since
the previous line (so a mid-soak slowdown shows where it happened); the banner
carries the whole-run average.

- `requests`, `ws`, `sse`: status tallies, RSS, heap, then **ops/s** for the
  interval. The banner names the unit: `ops/s`, `round-trips/s`, `events/s`.

  ```
  [requests h2 chronos] 200x45123 503x12 err3 | RSS 42MB | heap 3MB | 1234.5 ops/s | t=120s
  == requests chronos h2 passed (45123 ops, 751.9 ops/s) ==
  ```

- `streamUpload`, `streamDownload`: measured in **megabytes** (MiB), not transfers.
  A 1 GiB transfer is minutes long, so the transfer count is only a secondary
  field. Cumulative MB first, then the interval's MB/s. A window at or below a
  millisecond prints `n/a` instead of a bogus rate.

  ```
  [streamDownload h2 chronos] 3072MB rx | 51.2 MB/s | 3 done | 0 retried | RSS 42MB | heap 12MB
  == streamDownload chronos passed (4096MB rx in 61s, 67.1 MB/s, 4 transfers, 0 retried) ==
  ```

- `mixed`: one line carrying every slice's own numbers side by side, in the same
  fields the single-workload cells print. The slices are **never summed**: a
  headline that added small `/echo` ops to 1 MiB stream chunks would mean nothing,
  and a dead slice would hide inside it.

  ```
  [mixed h3 chronos] req 200x7529 743.7 ops/s | ws 3990 rt 396.8 rt/s | sse 930005 ev 95708.6 ev/s | up 1981MB 193.9 MB/s 30 done 0 retried | down 844MB 86.7 MB/s 12 done 0 retried | RSS 78MB | heap 10MB | t=10s
  == mixed chronos h3 passed (req 7529 ops 731.7 ops/s, ws 3990 round-trips, sse 930005 events, up 2048MB tx 32 transfers, down 896MB rx 14 transfers) ==
  ```

  The interval line's cumulative MB and the banner's differ because a transfer
  in flight at the deadline runs to completion: the interval line was printed at
  the deadline, the banner after the last transfer finished.

## Tasks

| Task | Workload |
| --- | --- |
| `nimble stressRequests` | buffered GET/POST/PUT: bodies, compression, auth, middleware, pool/mux |
| `nimble stressWs` | persistent WebSocket, text + binary under load |
| `nimble stressSse` | SSE subscribe under load, reconnect + Last-Event-ID resume |
| `nimble stressStreamUpload` | stream 1 GiB up; server verifies checksum (hard-fail) |
| `nimble stressStreamDownload` | stream 1 GiB down; client verifies checksum (hard-fail) |
| `nimble stressMixed` | all five of the above at once, through the same clients and connections |
| `nimble stress` | short smoke of all six |

## Configuration (`NAVI_*` env)

| Var | Default | Meaning |
| --- | --- | --- |
| `PROTO` | `h2` | `h1` \| `h2` \| `h3` \| `all` (h3 uses the h3 image) |
| `CLIENT` | `all` | `sync` \| `asyncdispatch` \| `chronos` \| `js` \| `all` |
| `SERVER` | `hypercorn` | the origin: `hypercorn` (FastAPI + Caddy + aioquic) or `vortex` (native Nim h1/h2/h3). Unknown value: exit 2. See [Choosing the server](#choosing-the-server) |
| `VORTEX_RUNTIME` | `sync` | `vortex` only: vortex's handler runtime the server binary is built with (`sync` \| `async` \| `chronos`). A Docker **build-arg**, so it is fixed per image build, not per cell |
| `VORTEX_REF` | pinned sha | `vortex` only: the nim-vortex commit the image installs. Also a build-arg; the pin lives in `Dockerfile.h3` so a vortex change cannot silently move navi's numbers. Must be a **full 40-character sha** (`nimble` rejects anything else): a branch or tag name would be baked into the Docker layer cache and never move again, so the image would keep serving whatever that name meant on the first build |
| `VORTEX_THREADS` | `1` | `vortex` only: loop threads per instance. One, to match the one hypercorn worker per instance; vortex's own default is `countProcessors()` |
| `VORTEX_HEADER_TIMEOUT` | `60` | `vortex` only: seconds from accept to a complete request head, `headerTimeout` (it covers the TLS handshake and any protocol upgrade). vortex's own default is 10 s, which these deliberately oversubscribed cells trip on a descheduled loop thread or a slow WebSocket upgrade; 60 s is the value vortex's own soak uses |
| `SSE_DROP_EVERY` | `1000` | events the `/events` stream delivers before the server drops the connection, so reconnect + `Last-Event-ID` resume is exercised. Read by both servers |
| `SERVER_COUNT` | `5` | server instances; requests round-robin across them |
| `SECONDS` | `60` | runtime per (client × protocol) cell |
| `CLIENT_COUNT` | `3` | concurrent navi client instances per cell |
| `CONCURRENCY` | `8` | in-flight requests per client (async fan-out) |
| `REQ_COMPRESSION` | `gzip` | request body: `none` \| `gzip` \| `deflate` (native; **octet/text only**) |
| `RESP_COMPRESSION` | `gzip` | response via `x-want-encoding`: `none` \| `gzip` \| `deflate` \| `br` \| `zstd` |
| `CONTENT_TYPES` | `octet,text,json,form` | csv restricting the `requests` /echo rotation; unknown tokens hard-fail at startup |
| `REPORT_SECONDS` | `60` | report cadence |
| `STREAM_BYTES` | `1073741824` | stream size (1 GiB); lower for a smoke |
| `WS_H3_PORTBAND` | `3000` | `mixed` + h3 **under hypercorn** only: the aioquic ws origins are `NAVI_BASE_PORT + band + i`, beside the Caddy front. A vortex origin needs no band |

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

## Choosing the server

`NAVI_SERVER` is a server dimension alongside `NAVI_PROTO` and `NAVI_CLIENT`, not
a replacement for the default. Pick by the question you are asking.

| | `hypercorn` (default) | `vortex` |
| --- | --- | --- |
| stack | FastAPI on hypercorn (h1/h2), Caddy in front for h3, aioquic for an h3 WebSocket | one Nim process per instance: h1, h2 and h3 on the same port, WebSocket over h1 Upgrade **and** h2/h3 Extended CONNECT |
| what a green run proves | navi talks to widely deployed servers under load: **the interop evidence** | navi's own throughput, h3 behaviour and fairness, against an independent implementation |
| server code | `server/app.py` | `server/vortex_server.nim` (navi-owned, implementing `app.py`'s contract) |
| ports | base + i public; **h3 adds** a hypercorn backend at base + 1000 + i and, for `mixed`, aioquic on the ws band | base + i, TCP and (h3 cells) UDP. No backend band, no ws band |

Use **hypercorn** by default, and for anything that is really a question about
interop. A CI or nightly run must stay on it.

Use **vortex** when the server is in the way of the measurement:

- **hypercorn is the bottleneck on the hot cells.** It is a Python asyncio server
  behind the GIL, `run.sh` already carries workarounds for its connection-lifecycle
  timers, and on `requests` the harness is measuring hypercorn at least as much as
  navi.
- **h3 goes through a Caddy detour under hypercorn.** Caddy cannot bridge an h3
  Extended CONNECT, so the `ws` h3 cell replaces it with aioquic and the `mixed`
  h3 cell runs aioquic on a separate port band, which means the ws slice of
  `mixed` never shares a QUIC connection with the other four, the interaction the
  cell exists for. Caddy also means the h3 cells never churn a QUIC connect.
  Under vortex every h3 cell, ws included, is served by one process on one port.

### The shared-blind-spot caveat

vortex and navi share an author and conventions, and vortex is **not** interop
evidence for that reason: a cell that passes only against vortex proves less than
one that passes against hypercorn. That is why hypercorn stays the default and
remains what CI/nightly runs. vortex is an independent *implementation* (its only
dependency is nimcrypto; its h3 is ngtcp2 + nghttp3, not nim-quic), so it is still
a second opinion, just not a disinterested one.

### Reading a failure under vortex

Two suspects instead of one. Triage in this order:

1. Re-run the same cell with `NAVI_SERVER=hypercorn`. If it passes there, the
   suspect is vortex, not navi.
2. Reproduce against the vortex server with a **non-navi** client (`curl`,
   `h2load`, a browser) before touching navi. Then file it at nim-vortex and
   either bump `VORTEX_REF` past the fix or record the cell as blocked on it.

### Numbers do not compare across servers

Always say which server produced a figure. Two reasons the same cell reads
differently:

- **Flow-control windows.** vortex defaults to a 1 MiB stream window, a 1 MiB h2
  connection window and a 4 MiB h3 connection window; hypercorn advertises HTTP/2's
  64 KiB initial window. The harness leaves both at their defaults, so every
  upload number (`up` in `mixed`, the `streamUpload` MB/s) moves, relevant to
  #461.
- **No Caddy hop.** Under hypercorn an h3 cell's bytes cross Caddy and then a
  loopback h1 connection to the backend; under vortex they do not.

### `NAVI_RECYCLE` under vortex

`NAVI_RECYCLE=1` buys **much less** against vortex than against hypercorn, and
differently on each protocol. Two mappings:

- `NAVI_KEEPALIVE_TIMEOUT` -> `keepAliveTimeout`, which is a **true idle timer**
  on h1 and h2: it closes a connection that has been quiet, and a busy one is
  never touched. hypercorn's `keep_alive_timeout` is not that -- measured on
  hypercorn 0.18 h2 it fires at about the configured value even on a connection
  carrying 13k req/s, which is why the hypercorn default here is pushed past the
  whole soak. So the same knob churns busy connections there and only pooled,
  quiet ones here.
- `NAVI_KEEPALIVE_MAX` -> `maxRequestsPerSocket`, which vortex applies to
  **HTTP/1 keep-alive only**. It is vortex's only per-connection request counter,
  and nothing equivalent is reachable from handler code on h2 or h3.

On h3 the two combine into no coverage at all: vortex advertises the idle window
as QUIC's `max_idle_timeout` and then arms ngtcp2's keep-alive PING at a third of
it, so a live QUIC connection is never idle-closed, and there is no request cap
either.

The cell runs either way, and says which of the three it got, after its banner:

```
== stress: streamDownload | chronos | h1 | 5 servers | server=vortex@006b4de68835/sync ==
[streamDownload h1 server=vortex] notice: maxRequestsPerSocket=200 caps requests per connection; idle close at 2s is idle-only
== stress: streamDownload | chronos | h2 | 5 servers | server=vortex@006b4de68835/sync ==
[streamDownload h2 server=vortex] notice: idle-only recycle (keepAliveTimeout=2s); no per-connection request cap
== stress: streamDownload | chronos | h3 | 5 servers | server=vortex@006b4de68835/sync ==
[streamDownload h3 server=vortex] notice: no recycle coverage under vortex (QUIC keep-alive PING defeats the idle close; no request cap)
```

So a recycle soak that has to churn **busy** connections is a hypercorn run, on
every protocol, and h1 is the only vortex cell with a per-connection request cap
at all.

Example:

```
NAVI_SERVER=vortex NAVI_SECONDS=600 NAVI_PROTO=all NAVI_CLIENT=all \
  nimble stressRequests
```

## `mixed`: all five workloads at once (`nimble stressMixed`)

The five soaks above each drive one workload at one set of servers, so they can
never see an interaction *between* workloads. `mixed` is the sixth cell: all five
verified loops at once, against one set of servers, through the **same `Navi`
instances**. `NAVI_CLIENT_COUNT` instances are built once (with the `requests`
cell's `x-stress` middleware, harmless on the other routes) and shared
round-robin across every slice, so a bulk `/upload` or `/download` body and
dozens of small `/echo` streams really ride one pooled h2/h3 connection next to a
parked SSE read and a WebSocket. Same server is easy; same connection is the
point. #444 -- a buffered upload parking an h2 connection's only reader, delaying
every inbound frame for every other stream on it -- is the shape this cell exists
to catch.

One caveat: on h3 the ws slice shares the instances and the event loop but *not* a
connection, because it has to dial its own QUIC origin (see "h3 needs two origins"
below). On h1 and h2 every slice shares connections with every other.

**Worker split.** `NAVI_CLIENT_COUNT x NAVI_CONCURRENCY` workers are divided
requests 40% / ws 20% / sse 20% / streamUpload 10% / streamDownload 10%, each
share rounded to the nearest worker, every slice at least one, and the remainder
to requests. At the defaults (3 x 8 = 24) that is `req=10 ws=5 sse=5 up=2 down=2`.
The rule is one pure proc (`common/mixsplit.nim`), shared with the js client, and
the resolved split is printed at cell start:

```
[mixed h2 chronos] split req=10 ws=5 sse=5 up=2 down=2
```

**Verification is per slice, unchanged.** Each slice runs the same `parts/*` work
proc its single-workload client runs: per-response `checkVersion` for requests and
both streams, the per-kind echo checks, the SSE `VersionGate` plus Last-Event-ID
continuity, and the upload/download SHA-1 brackets. Each slice keeps its own
counter, and each has its own **zero-work check** -- `no request completed`, `no
WebSocket round-trip completed`, `no SSE event consumed`, `no upload bytes moved`,
`no download bytes moved` -- so a stalled SSE feed or an upload that moved nothing
cannot hide behind a healthy `/echo` rate. The ws and sse checks read
`ops - errors`, not `ops`, because a tallied failure counts as an op too, so an
all-failing slice would otherwise clear the check. All shared instances are passed to
`chaosFinish`, so the process-wide FD/heap bracket stays honest under chaos.

**h3 needs two origins under hypercorn.** Caddy's `reverse_proxy` does not bridge
an h3 Extended CONNECT to a backend WebSocket, so for a mixed h3 cell `run.sh`
keeps the Caddy front on `NAVI_BASE_PORT + i` (for `/echo`, `/events`, `/upload`,
`/download`) and *also* starts aioquic ws servers on a band at `NAVI_BASE_PORT +
NAVI_WS_H3_PORTBAND + i`; the ws slice dials those. navi direct-dials QUIC for an
h3 WebSocket, so the band needs no Alt-Svc discovery leg. On h1 and h2 the ws
slice shares the base origins with everything else.

The consequence is that under hypercorn the ws slice of a mixed h3 cell shares the
instances and the event loop but **not** a QUIC connection with the other four,
the one gap in the cell's premise. `NAVI_SERVER=vortex` closes it: one process
terminates the h3 Extended CONNECT on the same port as the other four routes, so
there is no band, the ws slice rides the shared connection, and the h3 mixed cell
finally tests what the h1/h2 ones do.

**Gaps.** `sync` has **no mixed client**: the sync client is blocking, so a real
concurrent mix needs either `--threads:on` with a thread per slice or an
interleaved step loop like `syncChaosStep`. That is deliberately not built yet;
`run.sh` prints the usual "no source for this client/workload" skip. `js` mixes
**four** of the five (requests, ws, sse, streamDownload): js cannot stream a
request body, so the upload share folds into the download slice and the split line
says so.

**Reading the numbers.** A slice's rate in a mixed cell is not comparable to its
own dedicated cell, and that is the whole point: the slices contend for one
connection and one event loop, so a measured h2 cell can show the download slice
at hundreds of MB/s while the upload slice crawls, or the request slice at a
fraction of its solo ops/s. Compare a mixed cell to *itself* across runs, not to
the single-workload cells. One practical consequence: a streaming transfer takes
far longer here than in its own cell, so `nimble stressMixed` defaults
`NAVI_STREAM_BYTES` to **256 MiB** rather than the 1 GiB the dedicated stream
tasks use (and the `nimble stress` smoke drops it to 64 MiB); set
`NAVI_STREAM_BYTES` to override either. The two stream zero-work checks are
byte-based for the same reason -- `no upload bytes moved` / `no download bytes
moved`, not "no transfer completed" -- since a healthy short cell can legitimately
end with a transfer still in flight and none finished.

**The deadline is soft for the stream slices.** A stream worker will not *start* a
transfer it has no time left to finish (it compares the time remaining against its
own last transfer's duration), but one already in flight at the deadline runs to
completion, so a mixed cell can overrun `NAVI_SECONDS` by roughly one transfer.
The interval lines keep printing through the overrun, and the final line divides
the req/ws/sse rates by the time the deadline was reached while the two stream
fields divide by the full elapsed, so the overrun is never charged to the other
slices.

```
NAVI_SECONDS=600 NAVI_PROTO=h2 NAVI_CLIENT=chronos nimble stressMixed
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

- `common/`: shared native harness, `config` (env + gap policy + the shared
  `failHard`), `reporter` (status counter + interval ops/s + RSS from
  `/proc/self/statm`, plus the `segment` renderer the mixed line composes),
  `servers` (round-robin), `streamcontent` (fixed-block + incremental SHA-1 + the
  MB/MB/s accounting shared with the js download client + the `StreamProgress` both
  stream slices report from), `mixsplit` (the mixed worker split, shared with js),
  `httpset` (proto → version set),
  `chaos` (client-side chaos driver: seeded schedule, workers/watchdog, outcome
  classification; split into `chaos_async`/`chaos_sync` for the two client models),
  `leakcheck` (FD/heap/RSS sampling + assertions).
- `chaos/` — the Python asyncio misbehaving-server sidecar: `chaos_server.py`
  (entrypoint + control port), `modes.py` (registry + wire helpers), `h1.py`,
  `h2.py` (hyper-h2 decoder + raw-frame writer), `h3.py` (aioquic modes + the
  TCP Alt-Svc discovery leg), `requirements.txt` (`h2`).
- `clients/`: the workload clients, plus `clients/parts/` holding the verified
  work procs they are composed from. One client per workload, and `mixed` /
  `mixed_js` compose all of them: `parts/*_part.nim` (native) and
  `parts/*_js_part.nim` (js) are `include`d, not imported, because every proc in
  them takes the backend's `Navi`/`Future` types -- the same reason `common/httpset`
  and `common/chaos` are includes. So a single-workload cell and the mixed cell run
  the same code, not two copies of it. The async source (`*.nim`) is built for both
  asyncdispatch and (`-d:useChronos`) chronos; `*_sync.nim` is the sync client;
  `*_js.nim` runs under Node. run.sh skips any client whose source is absent, so
  partial client coverage degrades gracefully (there is no `mixed_sync.nim`).
- `server/app.py` — one FastAPI app (echo, ws, events, upload, download) served by
  hypercorn (h1/h2); Caddy fronts it for h3. The **contract spec** for both servers.
- `server/vortex_server.nim`: the `NAVI_SERVER=vortex` origin, the same routes on
  [vortex](https://github.com/cryo2010/nim-vortex), h1 + h2 + h3 and WebSocket
  over h1 Upgrade and h2/h3 Extended CONNECT in one process. navi-owned rather
  than vortex's own `conformance/stress/stress_server.nim`, whose route contract is
  the vortex Python client's (different SSE numbering, no `x-sha1` on `/download`,
  no `/echo` canonicalisation, none of the coverage routes). It does its own
  gzip/deflate/br/zstd on both directions instead of using vortex's negotiation,
  because the catalogue expects exactly the codec `x-want-encoding` asked for --
  the same zlib/brotli/zstd libraries `app.py` reaches through Python, so both
  servers put the same bytes on the wire.
- `Dockerfile` (h1/h2, hypercorn only) and `Dockerfile.h3` (adds the
  ngtcp2/nghttp3/OpenSSL-3.5 toolchain, Caddy, aioquic **and** the vortex origin,
  compiled once at image build time). vortex serves TLS and HTTP/3 from one build,
  so it needs those trees and lives only in the h3 image; `navi.nimble` selects
  that image whenever `NAVI_PROTO` includes h3 **or** `NAVI_SERVER=vortex`, which
  keeps a plain hypercorn h1/h2 run as cheap as it was. It does **not** keep an
  h3 run cheap: a hypercorn h3 or `all` run builds this file too, so it now pays
  the vortex install and compile layer (about 25 to 30 s on an otherwise cached
  image) and a build-time clone of nim-vortex from GitHub, for a server that run
  never starts. The nightly `stress-chaos` workflow builds this image and pays it
  as well. `run.sh` orchestrates: cert, N servers, the client × protocol matrix,
  cleanup, and a final pass/fail banner.

## Notes

- `nimble` does not propagate a task's exit code (nim-lang/nimble#1802): read the
  final `== <workload>: all cells passed ==` banner, or run the `docker run`
  directly for an honest exit code.
- A matrix in which every cell was skipped prints `== <workload>: NO CELLS RAN ==`
  and exits 1, rather than reading as a pass on no work: `NAVI_CLIENT=sync` with
  `mixed`, or `NAVI_CLIENT=js` with `streamUpload`, has no client to run at all.
- RSS is read on Linux (everything is Dockerized); the js client reports
  `process.memoryUsage().rss`.
- CI runs a nightly chaos rotation (`.github/workflows/stress-chaos.yml`): the full
  `NAVI_CHAOS=all NAVI_PROTO=all NAVI_CLIENT=all` matrix (must end `all cells
  passed`) plus a `NAVI_CHAOS_SELFTEST=fd` job that inverts the exit code and passes
  only when the FD assertion fails the run. Both use `docker run` directly, not
  nimble, so the container exit code is honest.
