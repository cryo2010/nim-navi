# Backend stress test: exercises every navi backend (sync, asyncdispatch,
# chronos, js) over TLS with all HTTP verbs, WebSocket, a middleware, and
# several clients, for a configurable duration. Dockerized so chronos and Node
# (for navi/js) are present on any host.
#
#   docker build -f tests/stress/Dockerfile -t navi-stress .
#   docker run --rm -e NAVI_STRESS_SECONDS=30 navi-stress
#
# Usually driven by `nimble stress`.
FROM nimlang/nim:2.2.10

RUN apt-get update && apt-get install -y --no-install-recommends \
      openssl ca-certificates curl && rm -rf /var/lib/apt/lists/*

# Node 22+ provides the global WebSocket the navi/js backend wraps.
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && rm -rf /var/lib/apt/lists/*

WORKDIR /navi
COPY . .
# checksums: proto/ws SHA-1 + Digest auth. chronos: the chronos backend.
RUN nimble install -y checksums "chronos@>= 4.0.0"

ENTRYPOINT ["bash", "tests/stress/run.sh"]
