# Package

version       = "0.7.0"
author        = "Craig Younker"
description   = "An HTTP client with HTTP/1.1, HTTP/2, TLS and WebSocket support"
license       = "MIT"
srcDir        = "src"
installExt    = @["nim", "cpp"]   # ship the HTTP/3 driver (h3client.cpp) so a
                                  # downstream -d:naviHttp3 build can compile it


# Dependencies

requires "nim >= 2.2.10"
requires "checksums >= 0.2.2"   # MD5 + SHA-256 (sha2 API) for Digest auth; 0.2.2 is the tested floor

# Optional: only needed when you `import navi/chronos`.
# requires "chronos >= 4.0.0"

task test, "Run the test suite (delegates to tests/run.sh)":
  # The suite list and build options live in tests/run.sh, so there is one source
  # of truth shared with CI. NAVI_MM (orc/arc) and NAVI_SANITIZE=1 are read from
  # the environment by the script.
  #
  # WARNING: `nimble` does not propagate a task's exit code (nim-lang/nimble#1802)
  # -- on this nimble a failing test still makes `nimble test` exit 0. A failure
  # is visible in the output, but CI runs `bash tests/run.sh` directly so a real
  # failure actually fails the job.
  exec "bash tests/run.sh"

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

task fuzz, "Coverage-guided libFuzzer run of a sans-io fuzz target (Docker; Linux libFuzzer)":
  # libFuzzer's runtime ships with clang on Linux but not macOS, so the Docker
  # image gives a reproducible run from any host. NAVI_FUZZ_TARGET picks the
  # target (hpack|h1|frame|huffman|h2conn, default h2conn); NAVI_FUZZ_TIME is a
  # duration in seconds or "replay" for the portable ASan seed replay (default
  # 60). The corpus is mounted, so coverage and any crash reproducer persist on
  # the host under tests/fuzz/corpus/<target>/.
  let target = getEnv("NAVI_FUZZ_TARGET", "h2conn")
  let mode = getEnv("NAVI_FUZZ_TIME", "60")
  mkDir "tests/fuzz/corpus/" & target
  # NB: nimble does not propagate a task's exit code (nim-lang/nimble#1802) -- not
  # even an explicit quit -- so `nimble fuzz` exits 0 even on a crash. To tell a
  # run apart programmatically, check for a written crash artifact (a finding):
  #   ls tests/fuzz/corpus/<target>/crash-*
  # or run the docker command directly (its exit code IS reliable). See the fuzz
  # README. Interactively, a crash prints a stack trace + "SUMMARY: libFuzzer".
  exec "docker build -f tests/fuzz/Dockerfile -t navi-fuzz ."
  exec "docker run --rm " &
       "-v \"$(pwd)/tests/fuzz/corpus:/navi/tests/fuzz/corpus\" " &
       "-w /navi/tests/fuzz/corpus/" & target & " " &
       "navi-fuzz " & target & " " & mode

task stress, "Dockerized backend stress test (all backends, TLS, WS, middleware, multi-client)":
  # Runs every backend (sync, asyncdispatch, chronos, js) against a TLS test
  # server for NAVI_STRESS_SECONDS (default 20), each with NAVI_STRESS_CLIENTS
  # navi clients (default 3; concurrent on the async backends), all HTTP verbs,
  # and a persistent WebSocket, through a middleware. Dockerized so chronos and
  # Node (for navi/js) are available anywhere.
  # NB: nimble does not propagate a task's exit code (nim-lang/nimble#1802), so
  # this exits 0 even on failure; for a reliable pass/fail, run the docker
  # command directly (its exit code is honest) or read the final "all backends
  # passed" line.
  let secs = getEnv("NAVI_STRESS_SECONDS", "20")
  let clients = getEnv("NAVI_STRESS_CLIENTS", "3")
  exec "docker build -f tests/stress/Dockerfile -t navi-stress ."
  exec "docker run --rm -e NAVI_STRESS_SECONDS=" & secs &
       " -e NAVI_STRESS_CLIENTS=" & clients & " navi-stress"

task badssl, "TLS client conformance against badssl.com (network; nightly)":
  exec "nim c -r --hints:off tests/interop/badssl.nim"

task interop, "Run the nghttpd HTTP/2 interop suite (needs nghttpd + openssl)":
  # Starts the nghttp2 reference server over TLS+h2 and runs navi against it.
  exec "bash tests/interop/run.sh"

task tlsFallback, "Handshake-aware address fallback test (needs openssl + python3)":
  # A dead endpoint + a good TLS server on the same port; navi must fall through.
  exec "bash tests/interop/tls_fallback.sh"

task tlsVersion, "TLS min/max version pinning enforcement (needs openssl with TLS 1.3)":
  # TLS 1.2-only and TLS 1.3-only servers; navi's min/max version pins must be honored.
  exec "bash tests/interop/tls_version.sh"

task happyEyeballs, "Happy Eyeballs address racing (needs openssl)":
  # A blackholed first address + a good server; navi must race past the blackhole.
  exec "bash tests/interop/happy_eyeballs.sh"

task cipherSuite, "Cipher-suite selection enforcement (needs openssl with TLS 1.3)":
  # Servers pinned to one TLS 1.2 cipher / one TLS 1.3 ciphersuite; pins must hold.
  exec "bash tests/interop/cipher_suite.sh"

task caVerify, "Private-CA (TlsConfig.caFile) verification, sync backend (needs openssl)":
  # A server cert signed by a throwaway CA: navi must trust it via caFile and
  # reject it without the CA (private root is not in the system trust store).
  exec "bash tests/interop/ca_verify.sh"

task streaming, "File-streaming interop: http1/http2 x upload/download (needs nghttpd + openssl)":
  # Streams a 3 MiB file each way over each protocol and asserts the transfer used
  # that protocol and hash-matches the original. Mirrors the four CI checks.
  for proto in ["http1", "http2"]:
    for dir in ["upload", "download"]:
      exec "bash tests/interop/streaming.sh " & proto & " " & dir

task streamConcurrent, "Concurrent streaming interop: 50 simultaneous streamed uploads + downloads over the h2 mux (Docker)":
  # Fires N (default 50, set NAVI_CONCURRENT_N) simultaneous streamed downloads,
  # uploads, and a mixed batch over one h2 connection against a FastAPI server,
  # verifies every transfer by SHA-1, and asserts they multiplexed onto a single
  # connection. Requires Docker; exits non-zero on any mismatch.
  let compose = "docker compose -f tests/interop/streaming_concurrent/docker-compose.yml"
  try:
    exec compose & " up --build --abort-on-container-exit --exit-code-from client"
  finally:
    exec compose & " down"

task sse, "SSE reconnect interop: server drops mid-stream, client resumes via Last-Event-ID over the h2 mux (Docker)":
  # A FastAPI SSE server drops the connection after 3 events per request; the
  # navi/asyncdispatch client must reconnect and resume from Last-Event-ID to
  # receive all 10 events in order. One command, exits non-zero on any gap.
  let compose = "docker compose -f tests/interop/sse/docker-compose.yml"
  try:
    exec compose & " up --build --abort-on-container-exit --exit-code-from client"
  finally:
    exec compose & " down"

task servers, "Multi-server HTTP/2 interop: nginx, Caddy, h2o (needs Docker + openssl)":
  # Runs navi's h2 client (and chronos h1) against three unrelated server stacks.
  exec "bash tests/interop/servers.sh"

task httpbin, "httpbin functionality interop behind Caddy TLS+h2 (needs Docker + openssl)":
  # Local httpbin (never the public service): methods, bodies, auth, redirects,
  # decompression, cookies, streaming, exercised through navi.
  exec "bash tests/interop/httpbin.sh"

task live, "Live interop against real public servers/CDNs (network; nightly)":
  # Tolerates network noise/server rejections; fails only on a navi protocol bug.
  exec "nim c -r --path:src -d:ssl --hints:off tests/interop/live.nim"

task bench, "Dockerized HTTP client benchmark: navi vs std/httpclient, Go, Rust (needs Docker)":
  # Builds a Nim+Go+Rust image and runs the TLS+gzip+all-methods comparison, then
  # a navi h1/h2/h3 x cold/pooled protocol matrix. Set NAVI_BENCH_ITERS to change
  # the load. `NAVI_BENCH_NETEM=1 nimble bench` adds a lossy/high-latency regime
  # (needs the NET_ADMIN cap, which is granted below) where h3 pulls ahead of h2.
  exec "docker build -f bench/Dockerfile -t navi-bench ."
  exec "docker run --rm --cap-add=NET_ADMIN " &
       "-e NAVI_BENCH_ITERS -e NAVI_BENCH_COLD_ITERS -e NAVI_BENCH_NETEM " &
       "-e NAVI_BENCH_CONC -e NAVI_BENCH_NETEM_DELAY -e NAVI_BENCH_NETEM_LOSS navi-bench"

task wsjs, "navi/js WebSocket runtime test (Node client vs a native server)":
  # Runs the navi/js WebSocket client under Node against a native echo server.
  exec "bash tests/interop/jsws.sh"

task jsCookieJar, "navi/js opt-in cookie jar runtime test (Node)":
  # Verifies the opt-in jar replays cookies across requests on Node/undici.
  exec "bash tests/interop/js_cookiejar.sh"

task demoWssBrowser, "Browser wss demo: mkcert cert + wss server + page (needs mkcert, python3)":
  # Generates a browser-trusted cert (mkcert), serves the navi/js page over a
  # wss echo server, and prints the URL to open.
  exec "bash examples/websocket/wss_browser.sh"

task demoWssSync, "wss echo round trip on the sync backend (navi)":
  # Builds and starts the wss echo server, runs the sync client, cleans up.
  exec "bash examples/websocket/wss_demo.sh sync"

task demoWssAsync, "wss echo round trip on the asyncdispatch backend (navi/asyncdispatch)":
  exec "bash examples/websocket/wss_demo.sh asyncdispatch"

task demoWssChronos, "wss echo round trip on the chronos backend (needs the chronos package)":
  exec "bash examples/websocket/wss_demo.sh chronos"

task demoWs, "Run the WebSocket demos for every backend + browser page (Docker)":
  # Builds and runs one container: the native clients print their round trip,
  # then a page for the navi/js client is served at http://localhost:8000/.
  let compose = "docker compose -f demos/websocket/docker-compose.yml"
  try:
    exec compose & " up --build"
  finally:
    exec compose & " down"

task mtls, "Run the mutual-TLS (client certificate) interop test (needs openssl)":
  # Starts an OpenSSL server that requires a client certificate and runs navi's
  # mTLS test against it.
  exec "bash tests/interop/mtls.sh"

task chronosCafile, "chronos custom-CA (caFile) interop test (needs openssl + chronos)":
  # Starts an OpenSSL HTTPS server with a cert signed by a throwaway CA and
  # checks that navi/chronos verifies it against TlsConfig.caFile.
  exec "bash tests/interop/chronos_cafile.sh"

task demoHello, "Run the hello demo (navi/js client + FastAPI server via Docker)":
  # Builds and runs both containers, stops when the client finishes, and cleans
  # up afterwards. Requires Docker.
  let compose = "docker compose -f demos/hello/docker-compose.yml"
  try:
    exec compose & " up --build --abort-on-container-exit --exit-code-from client"
  finally:
    exec compose & " down"

task exampleUpload, "Streaming file-upload example (self-contained; verifies the round-trip)":
  # Streams a file as a chunked request body to a local echo server and asserts the
  # echoed bytes hash-match the original. No network; generates a temp file.
  exec "nim c -r --hints:off examples/streaming/upload_file.nim"

task exampleDownload, "Streaming file-download example (self-contained; verifies the round-trip)":
  # Streams a response body to disk from a local server and asserts the downloaded
  # file hash-matches the original. No network.
  exec "nim c -r --hints:off examples/streaming/download_file.nim"

task demoStreaming, "Streaming upload+download demo: navi/asyncdispatch vs a FastAPI HTTP/2 server (Docker)":
  # Builds a FastAPI server (Hypercorn, TLS + h2) and a navi/asyncdispatch client
  # that streams an upload and a download and asserts each SHA-1 matches. Requires
  # Docker; exits non-zero if a transfer is corrupted.
  let compose = "docker compose -f demos/streaming/docker-compose.yml"
  try:
    exec compose & " up --build --abort-on-container-exit --exit-code-from client"
  finally:
    exec compose & " down"

task demoSse, "Server-Sent Events demo: navi/asyncdispatch reconnects through a mid-stream drop (Docker)":
  # A FastAPI SSE server streams tick events over h2 and drops once midway; the
  # navi client reconnects transparently (Last-Event-ID) and receives all ticks in
  # order. Requires Docker; exits non-zero if a tick is missing.
  let compose = "docker compose -f demos/sse/docker-compose.yml"
  try:
    exec compose & " up --build --abort-on-container-exit --exit-code-from client"
  finally:
    exec compose & " down"
