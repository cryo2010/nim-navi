# HTTP/3 toolchain for navi's transport bring-up: OpenSSL 3.5 (client QUIC API),
# nghttp3 (HTTP/3 + QPACK), ngtcp2 (QUIC + the OpenSSL crypto binding), Nim, and
# Caddy as a zero-config live h3 origin. This image lets us write and verify the
# ngtcp2/nghttp3 FFI against real headers and a real server before it lands in
# src/navi/backend/quic.nim.
#
#   docker build -f tests/interop/http3/Dockerfile -t navi-h3 .
#   docker run --rm navi-h3            # starts Caddy (h3) and runs the probe
FROM nimlang/nim:2.2.10

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential git ca-certificates pkg-config curl \
      autoconf automake libtool \
    && rm -rf /var/lib/apt/lists/*

ENV OSSL=/opt/ossl NGHTTP3=/opt/nghttp3 NGTCP2=/opt/ngtcp2

# OpenSSL 3.5.x: the client QUIC API that ngtcp2's crypto_ossl binding requires.
# (QUIC is built by default in 3.5, so no extra Configure flag.)
RUN git clone --depth 1 -b openssl-3.5 https://github.com/openssl/openssl /src/openssl \
 && cd /src/openssl \
 && ./Configure --prefix=$OSSL --libdir=lib \
 && make -j"$(nproc)" && make install_sw \
 && rm -rf /src/openssl

# nghttp3: HTTP/3 framing + QPACK.
RUN git clone --depth 1 --recursive https://github.com/ngtcp2/nghttp3 /src/nghttp3 \
 && cd /src/nghttp3 && autoreconf -i \
 && ./configure --prefix=$NGHTTP3 --enable-lib-only \
 && make -j"$(nproc)" && make install \
 && rm -rf /src/nghttp3

# ngtcp2: QUIC transport + the libngtcp2_crypto_ossl binding (built when it finds
# an OpenSSL >= 3.5 via pkg-config).
RUN git clone --depth 1 --recursive https://github.com/ngtcp2/ngtcp2 /src/ngtcp2 \
 && cd /src/ngtcp2 && autoreconf -i \
 && PKG_CONFIG_PATH=$OSSL/lib/pkgconfig ./configure --prefix=$NGTCP2 \
      --enable-lib-only --with-openssl \
 && make -j"$(nproc)" && make install \
 && rm -rf /src/ngtcp2

# A zero-config HTTP/3 origin for the probe to dial.
COPY --from=caddy:2 /usr/bin/caddy /usr/bin/caddy

ENV PKG_CONFIG_PATH=$OSSL/lib/pkgconfig:$NGHTTP3/lib/pkgconfig:$NGTCP2/lib/pkgconfig
ENV LD_LIBRARY_PATH=$OSSL/lib:$NGHTTP3/lib:$NGTCP2/lib

WORKDIR /navi
COPY . .
RUN nimble install -y checksums chronos

ENTRYPOINT ["bash", "tests/interop/http3/run.sh"]
