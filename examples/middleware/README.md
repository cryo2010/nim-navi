# Middleware examples

Runnable demonstrations of the batteries-included middleware under `navi/mw`
(imported to mirror your client import, e.g. `navi/asyncdispatch/mw`). Each script
starts a small local HTTP server on a thread (`server.nim`) so it is
self-contained and needs no network.

| Example | Middleware | Client | Proves |
| --- | --- | --- | --- |
| `caching.nim` | `mw.cache()` | sync | the second GET is served from cache (server sees 1 request) |
| `rateLimit.nim` | `mw.rateLimit()` | asyncdispatch | a token bucket paces concurrent requests |
| `concurrencyLimit.nim` | `mw.concurrencyLimit()` | asyncdispatch | in-flight requests are capped |
| `bearer.nim` | `mw.bearer()` | sync | `Authorization: Bearer <token>` is attached |
| `basicAuth.nim` | `mw.basic()` | sync | `Authorization: Basic <base64>` is attached |

Run any of them directly:

```
nim c -r examples/middleware/caching.nim
```

`server.nim` is the shared helper (counting HTTP server), not an example itself.
