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
  let opts = "--hints:off --mm:" & mm
  let suites = ["test_h1", "test_h2_frame", "test_h2_hpack", "test_h2_huffman",
                "test_h2_conn", "test_entries", "test_async", "test_chronos"]
  for s in suites:
    exec "nim c -r " & opts & " tests/" & s & ".nim"
