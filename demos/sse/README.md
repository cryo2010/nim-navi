# navi SSE demo

A navi/asyncdispatch Server-Sent Events client against a FastAPI SSE server over
HTTP/2. The server streams eight `tick` events but **drops the connection once
midway**; navi reconnects transparently, resends `Last-Event-ID`, and resumes, so
the client receives every tick in order without noticing the drop.

Run it (needs Docker):

```sh
nimble demoSse
```

You'll see the client print each tick, and the server log two "connection opened"
lines -- the reconnection made visible. The client exits non-zero if any tick is
missing or out of order.

- [`server.py`](server.py) -- FastAPI SSE endpoint that drops once and resumes from
  `Last-Event-ID`.
- [`client.nim`](client.nim) -- `api.sse(url)` consumed with `each`, breaking on the
  final `end` event.
