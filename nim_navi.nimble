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
                "test_h2_huffman", "test_h2_conn", "test_entries", "test_async",
                "test_chronos"]
  for s in suites:
    exec "nim c -r " & opts & " tests/" & s & ".nim"

task demoHello, "Run the hello demo (navi/js client + FastAPI server via Docker)":
  # Builds and runs both containers, stops when the client finishes, and cleans
  # up afterwards. Requires Docker.
  let compose = "docker compose -f demos/hello/docker-compose.yml"
  try:
    exec compose & " up --build --abort-on-container-exit --exit-code-from client"
  finally:
    exec compose & " down"
