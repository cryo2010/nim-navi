# Package

version       = "0.1.0"
author        = "Craig Younker"
description   = "A sugary HTTP client for Nim (HTTP/1.1 + HTTP/2, sync and async)"
license       = "MIT"
srcDir        = "src"
installExt    = @["nim"]


# Dependencies

requires "nim >= 2.2.10"

# Optional: only needed when you `import navi/chronos`.
# requires "chronos >= 4.0.0"

task test, "Run the test suite":
  exec "nim c -r --hints:off tests/test_h1.nim"
  exec "nim c -r --hints:off tests/test_entries.nim"
  exec "nim c -r --hints:off tests/test_async.nim"
  exec "nim c -r --hints:off tests/test_chronos.nim"
