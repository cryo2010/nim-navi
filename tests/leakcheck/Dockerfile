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
      valgrind openssl curl ca-certificates gnupg \
      libbrotli1 libzstd1 \
      python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

# The js backend calls `new WebSocket(...)`; the global WebSocket client only
# exists in Node >= 22 (Debian's packaged nodejs is 20, where the js websocket
# cells fail with "WebSocket is not defined"). Install Node 22 from NodeSource.
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
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
