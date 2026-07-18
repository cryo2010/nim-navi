# WebSocket demo (one command)

Runs the `examples/websocket` clients for **every backend** and serves the
navi/js page — all from one container, so you don't need a Nim toolchain or to
juggle several terminals.

```sh
nimble demoWs
# or: docker compose -f demos/websocket/docker-compose.yml up --build
```

What happens:

1. A WebSocket echo server starts inside the container.
2. The **sync**, **asyncdispatch**, and **chronos** clients each connect, send a
   message, and print the echoed reply (visible in the compose logs).
3. A static server serves the **navi/js** page at <http://localhost:8000/> —
   open it in a browser to watch navi compiled to JavaScript do the same round
   trip over the runtime's native `WebSocket`.

Press Ctrl-C to stop; `nimble demoWs` tears the container down afterwards.

The clients' source lives in `examples/websocket/` (readable, runnable standalone
with a local `nim c -r`); this folder only holds the Docker orchestration.
