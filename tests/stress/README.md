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

## Layout

- `common/` — shared native harness: `config` (env + gap policy), `reporter`
  (status counter + RSS from `/proc/self/statm`), `servers` (round-robin),
  `streamcontent` (fixed-block + incremental SHA-1), `httpset` (proto → version set).
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
