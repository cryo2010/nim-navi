# Per-workload stress image (h1/h2). Nim + Node (navi/js) + chronos + a FastAPI/
# hypercorn server. Dockerized so every backend and the Python server are present
# on any host.
#
#   docker build -f tests/stress/Dockerfile -t navi-stress .
#   docker run --rm -e NAVI_WORKLOAD=requests -e NAVI_SECONDS=60 navi-stress
#
# Usually driven by `nimble stressRequests` (etc). For h3, use Dockerfile.h3.
#
# Deliberately has NO vortex origin, so a plain hypercorn h1/h2 run stays exactly
# as cheap as it was: vortex serves TLS and HTTP/3 from one build, so there is no
# TLS-without-QUIC configuration of it to put here and carrying it would mean
# building OpenSSL 3.5 + nghttp3 + ngtcp2 in this image too. NAVI_SERVER=vortex
# therefore lives in Dockerfile.h3, which already has those trees; navi.nimble
# selects that image for NAVI_SERVER=vortex and run.sh exits 127 naming it if
# someone runs this one with NAVI_SERVER=vortex by hand.
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
# FastAPI + hypercorn (h1/h2 TLS) + optional br/zstd codecs, plus the chaos
# sidecar deps (hyper-h2 for the frame-level h2 chaos; a no-op for the h1 slice
# but installed now so the image is ready for the later phases).
RUN pip3 install --no-cache-dir --break-system-packages \
      -r tests/stress/server/requirements.txt \
      -r tests/stress/chaos/requirements.txt

ENTRYPOINT ["bash", "tests/stress/run.sh"]
