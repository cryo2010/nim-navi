# Per-workload stress image (h1/h2). Nim + Node (navi/js) + chronos + a FastAPI/
# hypercorn server. Dockerized so every backend and the Python server are present
# on any host.
#
#   docker build -f tests/stress/Dockerfile -t navi-stress .
#   docker run --rm -e NAVI_WORKLOAD=requests -e NAVI_SECONDS=60 navi-stress
#
# Usually driven by `nimble stressRequests` (etc). For h3, use Dockerfile.h3.
FROM nimlang/nim:2.2.10

RUN apt-get update && apt-get install -y --no-install-recommends \
      openssl ca-certificates curl python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Node 22+ provides the global WebSocket / fetch the navi/js backend wraps.
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && rm -rf /var/lib/apt/lists/*

WORKDIR /navi
COPY . .
# checksums: proto/ws SHA-1 + Digest auth. chronos: the chronos backend.
RUN nimble install -y checksums "chronos@>= 4.0.0"
# FastAPI + hypercorn (h1/h2 TLS) + optional br/zstd codecs.
RUN pip3 install --no-cache-dir --break-system-packages \
      -r tests/stress/server/requirements.txt

ENTRYPOINT ["bash", "tests/stress/run.sh"]
