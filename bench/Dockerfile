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
      autoconf automake libtool \
    && rm -rf /var/lib/apt/lists/*

# HTTP/3 toolchain (OpenSSL 3.5 + nghttp3 + ngtcp2) for the navi protocol matrix,
# plus Caddy as an h1/h2/h3 origin. Built from source, mirroring
# tests/interop/http3/Dockerfile. Kept separate from the system OpenSSL the other
# clients use; navi's h3 build finds it via PKG_CONFIG_PATH.
ENV OSSL=/opt/ossl NGHTTP3=/opt/nghttp3 NGTCP2=/opt/ngtcp2
RUN git clone --depth 1 -b openssl-3.5 https://github.com/openssl/openssl /src/openssl \
 && cd /src/openssl && ./Configure --prefix=$OSSL --libdir=lib \
 && make -j"$(nproc)" && make install_sw && rm -rf /src/openssl
RUN git clone --depth 1 --recursive https://github.com/ngtcp2/nghttp3 /src/nghttp3 \
 && cd /src/nghttp3 && autoreconf -i && ./configure --prefix=$NGHTTP3 --enable-lib-only \
 && make -j"$(nproc)" && make install && rm -rf /src/nghttp3
RUN git clone --depth 1 --recursive https://github.com/ngtcp2/ngtcp2 /src/ngtcp2 \
 && cd /src/ngtcp2 && autoreconf -i \
 && PKG_CONFIG_PATH=$OSSL/lib/pkgconfig ./configure --prefix=$NGTCP2 --enable-lib-only --with-openssl \
 && make -j"$(nproc)" && make install && rm -rf /src/ngtcp2
COPY --from=caddy:2 /usr/bin/caddy /usr/bin/caddy
# PKG_CONFIG_PATH is build-time only (navi_proto's h3 link). Deliberately NO
# global LD_LIBRARY_PATH: it would make the system openssl and the other clients
# load the wrong OpenSSL. navi_proto gets an rpath at link time instead.
ENV PKG_CONFIG_PATH=$OSSL/lib/pkgconfig:$NGHTTP3/lib/pkgconfig:$NGTCP2/lib/pkgconfig

# tc (netem) for the optional lossy/high-latency regime. Its own layer, after the
# from-source h3 toolchain, so adding it does not invalidate those slow builds.
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY src ./src
COPY bench ./bench
# Pre-build the Rust client (pulls the reqwest crate graph) so `docker run` is
# fast; Nim/Go clients build quickly at run time.
RUN cd bench/clients/rust_client && cargo build --release

ENTRYPOINT ["bash", "/app/bench/run.sh"]
