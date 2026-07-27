# Backend stress test

Exercises every navi backend under sustained mixed load and asserts correctness
throughout. For each backend (`sync`, `asyncdispatch`, `chronos`, `js`) it runs,
for a configurable duration:

- **every HTTP verb** (GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS) against a
  TLS server, asserting the echoed method and a middleware-stamped header;
- a **persistent WebSocket** round trip (text + binary echo) per client;
- **several navi clients**, run concurrently on the async backends
  (asyncdispatch, chronos, js) and sequentially on sync;
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
| `server.nim` | TLS HTTP/1.1 server: echoes any method on `/echo`, WebSocket echo on `/ws`, keep-alive |
| `stress_sync.nim` | sync client (`import navi`) |
| `stress_async.nim` | async client; built twice (`-d:useChronos` -> `navi/chronos`, else `navi/asyncdispatch`) |
| `stress_js.nim` | `navi/js` client, run under Node |
| `run.sh` | generate cert, build all, run each backend, tear down |
| `Dockerfile` | Nim + Node 22 + chronos; `ENTRYPOINT` is `run.sh` |

## Notes

- The server is HTTP/1.1 over TLS; navi offers ALPN h2 and falls back to h1. h2
  multiplexing is covered by the nghttpd interop suite, not here.
- `HEAD` replies with `Content-Length: 0`. navi's h1 response parser is not told
  the request verb, so a `HEAD` reply with a non-zero `Content-Length` and no body
  would make the client wait for a body it never gets. That is a real client bug
  (its own fix, separate from this harness); the server avoids it so the run does
  not deadlock.
