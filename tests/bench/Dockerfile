# Light cross-language benchmark image: Nim + Go + Rust + Node + Python for h1/h2
# cells (no HTTP/3 toolchain, no Caddy). Used when NAVI_PROTO is h1 or h2.
#
#   docker build -f tests/bench/Dockerfile -t navi-bench .
#   docker run --rm -e NAVI_WORKLOAD=requests -e NAVI_PROTO=h2 navi-bench
FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential cmake curl git ca-certificates xz-utils pkg-config \
      libssl-dev libpcre3-dev openssl \
    && rm -rf /var/lib/apt/lists/*

# Go
ARG GO_VERSION=1.22.5
RUN arch="$(dpkg --print-architecture)" && \
    curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${arch}.tar.gz" | tar -C /usr/local -xz
ENV PATH="/usr/local/go/bin:${PATH}" GOFLAGS=-buildvcs=false GOTOOLCHAIN=local

# Rust (minimal profile)
RUN curl -fsSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

# Nim from source
ARG NIM_VERSION=2.2.10
RUN curl -fsSL "https://nim-lang.org/download/nim-${NIM_VERSION}.tar.xz" -o /tmp/nim.tar.xz && \
    mkdir -p /opt && tar -xJf /tmp/nim.tar.xz -C /opt && rm /tmp/nim.tar.xz && \
    cd "/opt/nim-${NIM_VERSION}" && sh build.sh && bin/nim c --skipUserCfg koch && \
    ./koch boot -d:release && ./koch tools
ENV PATH="/opt/nim-2.2.10/bin:/root/.nimble/bin:${PATH}"
RUN nimble install -y checksums zippy "chronos@>= 4.0.0"

# Node.js + Python + netem (tc), after the toolchains so their layers stay cached.
RUN apt-get update && apt-get install -y --no-install-recommends \
      nodejs npm python3 python3-pip iproute2 \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --no-cache-dir --break-system-packages "httpx[http2]" websockets

WORKDIR /app
COPY src ./src
COPY tests/bench ./tests/bench
RUN if [ -f tests/bench/clients/rust_client/Cargo.toml ]; then \
      cd tests/bench/clients/rust_client && cargo build --release; fi

ENTRYPOINT ["bash", "/app/tests/bench/run.sh"]
