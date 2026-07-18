# One-command WebSocket demo: builds the examples/websocket clients for every
# backend and serves the navi/js page. Build context is the repo root so the
# navi sources under src/ and the examples are available.
FROM nimlang/nim:2.2.10

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY src ./src
COPY examples/websocket ./examples/websocket

# navi's Nim deps: checksums (proto/ws SHA-1) and chronos (the chronos backend).
RUN nimble install -y checksums "chronos@>= 4.0.0"

# Build the native example clients and the echo server.
RUN for t in echo_server sync asyncdispatch chronos; do \
      nim c --path:src -d:release --hints:off -o:/app/$t examples/websocket/$t.nim; \
    done

# Compile the navi/js client and stage the web page next to it.
RUN mkdir -p /app/web && \
    nim js --path:src -d:release --hints:off -o:/app/web/navi_ws.js examples/websocket/js.nim && \
    cp examples/websocket/index.html /app/web/

COPY demos/websocket/entrypoint.sh /app/entrypoint.sh
CMD ["bash", "/app/entrypoint.sh"]
