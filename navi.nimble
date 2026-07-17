# Package

version       = "0.1.0"
author        = "Craig Younker"
description   = "An HTTP client for Nim: HTTP/1.1 and HTTP/2, sync and async, ky-inspired API"
license       = "MIT"
srcDir        = "src"
installExt    = @["nim"]


# Dependencies

requires "nim >= 2.2.10"

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
                "test_h2_huffman", "test_h2_conn", "test_cookies", "test_entries",
                "test_stream_decompress", "test_async", "test_chronos"]
  for s in suites:
    exec "nim c -r " & opts & " tests/" & s & ".nim"

task leak, "Memory-growth check: every verb + request in a 100,000x loop":
  # Not in the default `test` suites (800k requests); its own PR job. NAVI_MM
  # selects the memory manager, NAVI_LEAK_ITERS the loop count.
  let mm = getEnv("NAVI_MM", "orc")
  exec "nim c -r -d:release --hints:off --mm:" & mm & " tests/leak.nim"

task badssl, "TLS client conformance against badssl.com (network; nightly)":
  exec "nim c -r --hints:off tests/interop/badssl.nim"

task interop, "Run the nghttpd HTTP/2 interop suite (needs nghttpd + openssl)":
  # Starts the nghttp2 reference server over TLS+h2 and runs navi against it.
  exec "bash tests/interop/run.sh"

task demoHello, "Run the hello demo (navi/js client + FastAPI server via Docker)":
  # Builds and runs both containers, stops when the client finishes, and cleans
  # up afterwards. Requires Docker.
  let compose = "docker compose -f demos/hello/docker-compose.yml"
  try:
    exec compose & " up --build --abort-on-container-exit --exit-code-from client"
  finally:
    exec compose & " down"
