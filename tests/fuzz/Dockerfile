# Coverage-guided libFuzzer runs of navi's sans-io fuzz targets. libFuzzer's
# runtime ships with clang on Linux but not with macOS's clang, so this image
# gives a reproducible run from any host (macOS included).
#
#   docker build -f tests/fuzz/Dockerfile -t navi-fuzz .
#   docker run --rm navi-fuzz h2conn 60       # 60s coverage-guided run
#   docker run --rm navi-fuzz h2conn replay   # portable ASan seed replay
#
# Usually driven by `nimble fuzz` (see navi.nimble).
FROM nimlang/nim:2.2.10

# libclang-rt-dev carries the compiler-rt runtimes, including libFuzzer's
# (-fsanitize=fuzzer). Debian's `clang` package does not pull it in the way
# Ubuntu's does, so install it explicitly or the fuzzer link fails.
RUN apt-get update && apt-get install -y --no-install-recommends \
      clang libclang-rt-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /navi
COPY . .
RUN nimble install -y checksums   # navi's MD5/SHA-256 dependency (Digest auth)

# Absolute path: `nimble fuzz` sets the working dir to the mounted corpus so crash
# reproducers persist, and run.sh derives its root from its own location anyway.
ENTRYPOINT ["bash", "/navi/tests/fuzz/run.sh"]
