# Valgrind leak check for navi's TLS client path. Valgrind is Linux-only, so this
# image gives a reproducible environment to run it from any host (including macOS).
#
#   docker build -f tests/valgrind/Dockerfile -t navi-valgrind .
#   docker run --rm navi-valgrind
FROM nimlang/nim:2.2.10

RUN apt-get update && apt-get install -y --no-install-recommends \
      valgrind nghttp2-server openssl libbrotli1 libzstd1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /navi
COPY . .
RUN nimble install -y checksums   # navi's MD5/SHA-256 dependency (Digest auth)

ENTRYPOINT ["bash", "tests/valgrind/run.sh"]
