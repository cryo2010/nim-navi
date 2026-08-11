# Reproducible environment for navi's leak-check matrix (Valgrind + ASan/UBSan for
# the native backends; a Node heap/fd check for the js backend). Valgrind is
# Linux-only, so this image lets it run from any host, including macOS.
#
#   docker build -f tests/leakcheck/Dockerfile -t navi-leakcheck .
#   docker run --rm navi-leakcheck valgrind asyncdispatch streamdownc
#   docker run --rm navi-leakcheck sanitize sync http1s
#   docker run --rm navi-leakcheck js streamdownc
FROM nimlang/nim:2.2.10

RUN apt-get update && apt-get install -y --no-install-recommends \
      valgrind openssl curl ca-certificates \
      libbrotli1 libzstd1 \
      python3 python3-pip python3-venv \
      nodejs \
    && rm -rf /var/lib/apt/lists/*

# FastAPI + Hypercorn serve every scenario (plaintext h1 and TLS h1/h2-ALPN).
RUN pip3 install --no-cache-dir --break-system-packages \
      "fastapi>=0.110" "hypercorn>=0.16" "wsproto>=1.2"

WORKDIR /navi
COPY . .
RUN nimble install -y checksums   # navi's MD5/SHA-256 dependency (Digest auth)
RUN nimble install -y "chronos@>= 4.0.0"   # for the navi/chronos backend cells

# Dispatch: `js <scenario>` -> run-js.sh; otherwise `<mode> <target> <scenario>`.
ENTRYPOINT ["bash", "tests/leakcheck/entry.sh"]
