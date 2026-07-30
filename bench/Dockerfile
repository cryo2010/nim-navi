# Multi-language HTTP client benchmark for navi. A single image carrying Nim, Go,
# Rust, Node.js, and Python; it builds/runs each client against a local TLS server.
# Large by design (several toolchains), but self-contained and reproducible.
#
#   docker build -f bench/Dockerfile -t navi-bench .
#   docker run --rm navi-bench                     # default 3000 iterations
#   docker run --rm -e NAVI_BENCH_ITERS=8000 navi-bench
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

# Nim from source (choosenim has no linux_arm64 build). build.sh bootstraps the
# bundled C sources, then koch builds the optimized compiler + nimble.
ARG NIM_VERSION=2.2.10
RUN curl -fsSL "https://nim-lang.org/download/nim-${NIM_VERSION}.tar.xz" -o /tmp/nim.tar.xz && \
    mkdir -p /opt && tar -xJf /tmp/nim.tar.xz -C /opt && rm /tmp/nim.tar.xz && \
    cd "/opt/nim-${NIM_VERSION}" && sh build.sh && bin/nim c --skipUserCfg koch && \
    ./koch boot -d:release && ./koch tools
ENV PATH="/opt/nim-2.2.10/bin:/root/.nimble/bin:${PATH}"
RUN nimble install -y checksums zippy

# Node.js and Python (requests) for the interpreted-client comparison. Added
# after the toolchains above so their build layers stay cached.
RUN apt-get update && apt-get install -y --no-install-recommends \
      nodejs python3 python3-requests \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY src ./src
COPY bench ./bench
# Pre-build the Rust client (pulls the reqwest crate graph) so `docker run` is
# fast; Nim/Go clients build quickly at run time.
RUN cd bench/clients/rust_client && cargo build --release

ENTRYPOINT ["bash", "/app/bench/run.sh"]
