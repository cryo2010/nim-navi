# Package

version       = "0.1.0"
author        = "Craig Younker"
description   = "An HTTP client for Nim: HTTP/1.1 and HTTP/2, sync and async, ky-inspired API"
license       = "MIT"
srcDir        = "src"
installExt    = @["nim"]


# Dependencies

requires "nim >= 2.2.10"
requires "checksums >= 0.2.2"   # MD5 + SHA-256 (sha2 API) for Digest auth; 0.2.2 is the tested floor

# Optional: only needed when you `import navi/chronos`.
# requires "chronos >= 4.0.0"

task test, "Run the test suite":
  # Memory manager is selectable via NAVI_MM (orc/arc) for the CI matrix.
  let mm = getEnv("NAVI_MM", "orc")
  var opts = "--hints:off --mm:" & mm
  # NAVI_SANITIZE=1 builds the suite under AddressSanitizer + UBSan (the CI
  # memory-safety job). -d:useMalloc routes Nim allocations through malloc so
  # ASan can see them; -g and frame pointers give symbolized reports.
  if getEnv("NAVI_SANITIZE").len > 0:
    let san = "-fsanitize=address,undefined -fno-omit-frame-pointer -g"
    opts.add " -d:useMalloc --passC:\"" & san &
             "\" --passL:\"-fsanitize=address,undefined\""
  let suites = ["test_h1", "test_h2_frame", "test_h2_hpack", "test_h2_hpack_corpus",
                "test_h2_huffman", "test_h2_conn", "test_cookies", "test_digest",
                "test_entries", "test_stream_decompress", "test_ws", "test_ws_async",
                "test_async", "test_chronos", "test_ws_chronos"]
  for s in suites:
    exec "nim c -r " & opts & " tests/" & s & ".nim"

task leak, "Memory-growth check: every verb + request in a 100,000x loop":
  # Not in the default `test` suites (800k requests); its own PR job. NAVI_MM
  # selects the memory manager, NAVI_LEAK_ITERS the loop count.
  let mm = getEnv("NAVI_MM", "orc")
  exec "nim c -r -d:release --hints:off --mm:" & mm & " tests/leak.nim"

task leakSanitize, "LeakSanitizer check of the codec FFI (needs clang + libbrotli/libzstd)":
  # Catches C-side leaks getOccupiedMem can't see (zlib/brotli/zstd contexts).
  # On Linux, ASan enables LeakSanitizer at exit by default (detect_leaks=1).
  exec "nim c -r --mm:orc -d:useMalloc --hints:off " &
       "--passC:\"-fsanitize=address\" --passL:\"-fsanitize=address\" " &
       "tests/leak_sanitize.nim"

task valgrind, "Valgrind leak check of the TLS client path (Docker; Linux valgrind)":
  # Valgrind is Linux-only; the Docker image gives a reproducible run from any
  # host (macOS included). Fails on any definite/indirect leak. NAVI_MM selects
  # the memory manager (default orc; arc also flags reference-cycle leaks).
  let mm = getEnv("NAVI_MM", "orc")
  exec "docker build -f tests/valgrind/Dockerfile -t navi-valgrind ."
  exec "docker run --rm -e NAVI_MM=" & mm & " navi-valgrind"

task badssl, "TLS client conformance against badssl.com (network; nightly)":
  exec "nim c -r --hints:off tests/interop/badssl.nim"

task interop, "Run the nghttpd HTTP/2 interop suite (needs nghttpd + openssl)":
  # Starts the nghttp2 reference server over TLS+h2 and runs navi against it.
  exec "bash tests/interop/run.sh"

task mtls, "Run the mutual-TLS (client certificate) interop test (needs openssl)":
  # Starts an OpenSSL server that requires a client certificate and runs navi's
  # mTLS test against it.
  exec "bash tests/interop/mtls.sh"

task demoHello, "Run the hello demo (navi/js client + FastAPI server via Docker)":
  # Builds and runs both containers, stops when the client finishes, and cleans
  # up afterwards. Requires Docker.
  let compose = "docker compose -f demos/hello/docker-compose.yml"
  try:
    exec compose & " up --build --abort-on-container-exit --exit-code-from client"
  finally:
    exec compose & " down"
