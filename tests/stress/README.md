# Backend stress test

Exercises every navi backend under sustained mixed load and asserts correctness
throughout. For each backend (`sync`, `asyncdispatch`, `chronos`, `js`) it runs,
for a configurable duration:

- **every HTTP verb** (GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS) against a
  TLS server. The server echoes the request method and the middleware-stamped
  `x-stress` header (as `x-echo-*` response headers) and the request body
  verbatim; the client asserts the method, that the middleware ran, and that the
  echoed **body**, **Content-Type**, and **Content-Length** are exactly what it
  sent;
- **compression both ways** on the native backends: the client gzip/deflate-
  encodes the request body (alternating), the server decodes it, re-encodes the
  echoed response, and navi transparently **decompresses** it -- so the round
  trip is compressed on the wire and navi's decode path runs under load. The
  client also asserts navi consumed the `Content-Encoding` header. (js stays
  plain: its codec is the runtime's.)
- a **persistent WebSocket** round trip (text + binary echo) per client;
- **several navi clients**, run concurrently on the async backends
  (asyncdispatch, chronos, js) and sequentially on sync. On the async backends
  each client also fires its whole verb batch **concurrently** (verbs.len
  requests in flight at once), so the connection pool / h2 multiplexer is
  exercised under real contention, not one request at a time;
- all over **TLS**, with certificate verification on (the client trusts the
  server's self-signed cert), through **a middleware** that stamps `x-stress: 1`.

Any bad status, wrong echo, missing middleware effect, crash, or hang fails the
run.

## Run

```
nimble stress                          # 20s per backend, 3 clients each (Docker)
NAVI_STRESS_SECONDS=120 nimble stress
NAVI_STRESS_CLIENTS=8   nimble stress
```

It is Dockerized so `chronos` and Node (for `navi/js`, which needs Node 22+ for
the global WebSocket) are present on any host. `NAVI_STRESS_SECONDS` (default 20)
and `NAVI_STRESS_CLIENTS` (default 3) are the knobs.

As with the other Docker tasks, `nimble` does not propagate the exit code
(nim-lang/nimble#1802), so for a trustworthy pass/fail run the container directly
(its exit code is honest) or look for the final `== all backends passed ==` line:

```
docker build -f tests/stress/Dockerfile -t navi-stress .
docker run --rm -e NAVI_STRESS_SECONDS=60 navi-stress
```

## Pieces

| file | role |
| --- | --- |
| `server.mjs` | Node.js TLS server: echoes any method on `/echo`, WebSocket echo on `/ws`, keep-alive, gzip/deflate |
| `stress_sync.nim` | sync client (`import navi`) |
| `stress_async.nim` | async client; built twice (`-d:useChronos` -> `navi/chronos`, else `navi/asyncdispatch`) |
| `stress_js.nim` | `navi/js` client, run under Node |
| `reference.mjs` | raw Node `fetch`/`WebSocket` baseline (no navi), for a `[node-ref]` req/s comparison |
| `zlibcodec.nim` | zlib gzip/deflate encode+decode for the native clients (navi only decodes) |
| `run.sh` | generate cert, start the server, build+run each backend, tear down |
| `Dockerfile` | Nim + Node 22 + chronos; `ENTRYPOINT` is `run.sh` |

## Notes

- **The server is Node.js**, not Nim. A robust concurrent TLS+WebSocket server is
  what's needed here, and Node's stacks are production-grade; the earlier Nim
  servers hit ARC refcount races across threads (thread-per-connection) and
  asyncnet SSL write bugs (async). The thing under test is the navi *clients*.
- A final `[node-ref]` line runs the same workload on raw Node `fetch`/`WebSocket`
  (no navi) as a baseline. Compare it especially to `navi/js` -- both are `fetch`
  underneath, so the gap is navi's JS layer (~20% in a quick local run).
- The server is HTTP/1.1 over TLS; navi offers ALPN h2 and falls back to h1. h2
  multiplexing is covered by the nghttpd interop suite, not here.
- Compression uses gzip and zlib-wrapped deflate (navi's common decode path).
  brotli/zstd decoding is covered by `tests/test_stream_decompress.nim`; adding
  them here would need server-side br/zstd *encoders* (extra libs).
- `HEAD` replies headers only, with the `Content-Length` a GET would have sent,
  so it exercises the client's HEAD handling (the h1 parser is told the request
  verb and must not wait for a body). Regression coverage: `tests/test_h1.nim`.
