# Package

version       = "0.9.0"
author        = "Craig Younker"
description   = "A fast HTTP/1.1-3 client with TLS, streaming, SSE and WebSockets"
license       = "MIT"
srcDir        = "src"
installExt    = @["nim", "cpp"]   # ship the HTTP/3 driver (h3client.cpp) so a
                                  # downstream -d:naviHttp3 build can compile it


# Dependencies

requires "nim >= 2.2.10"
requires "checksums >= 0.2.2"   # MD5 + SHA-256 (sha2 API) for Digest auth; 0.2.2 is the tested floor

# Optional: only needed when you `import navi/chronos`. That client runs OpenSSL
# for TLS (like sync/asyncdispatch), so an https build needs `-d:ssl`.
# requires "chronos >= 4.0.0"

task test, "Run the unit test suite (via checkmate)":
  # checkmate discovers, compiles, and runs the tests/ suite (config in
  # .checkmate.toml: pattern t*.nim, excluding tests/interop). It is the same
  # runner CI uses. Install it with:
  #   nimble install "https://github.com/cryo2010/nim-checkmate"
  #
  # WARNING: `nimble` does not propagate a task's exit code (nim-lang/nimble#1802)
  # -- on this nimble a failing test still makes `nimble test` exit 0. A failure
  # is visible in the output, but CI runs `checkmate` directly so a real failure
  # actually fails the job.
  exec "checkmate"

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

proc runStress(workload: string) =
  # Build the stress image and run one workload, passing every NAVI_* knob
  # through. The h3 image (with the ngtcp2/nghttp3/OpenSSL-3.5 client toolchain +
  # Caddy) is only used when NAVI_PROTO is h3; h1/h2 use the light image.
  # Backend x protocol are iterated inside the container (run.sh). NB: nimble does
  # not propagate a task's exit code (nim-lang/nimble#1802), so a failure shows in
  # the output but this exits 0 -- run the docker command directly, or read the
  # final "== <workload>: all cells passed ==" banner, for CI-grade pass/fail.
  # `all` includes h3, so it needs the h3 image (ngtcp2/nghttp3/OpenSSL 3.5 + Caddy)
  # too -- that image is a superset and serves h1/h2 as well.
  let proto = getEnv("NAVI_PROTO", "h2")
  let h3 = proto == "h3" or proto == "all"
  let dockerfile = if h3: "tests/stress/Dockerfile.h3" else: "tests/stress/Dockerfile"
  let image = if h3: "navi-stress-h3" else: "navi-stress"
  exec "docker build -f " & dockerfile & " -t " & image & " ."
  exec "docker run --rm -e NAVI_WORKLOAD=" & workload &
       " -e NAVI_PROTO -e NAVI_BACKEND -e NAVI_SERVERS" &
       " -e NAVI_SECONDS -e NAVI_CLIENTS -e NAVI_CONCURRENCY" &
       " -e NAVI_REQ_COMPRESSION -e NAVI_RESP_COMPRESSION" &
       " -e NAVI_STREAM_BYTES -e NAVI_REPORT_SECONDS -e NAVI_LOG_ERRORS " & image

task stressRequests, "Stress: buffered request/response soak (verbs, compression, auth, mw, pool/mux)":
  runStress("requests")
task stressWs, "Stress: persistent WebSocket text+binary under load":
  runStress("ws")
task stressSse, "Stress: SSE subscribe under load with reconnect / Last-Event-ID resume":
  runStress("sse")
task stressStreamUpload, "Stress: stream 1 GiB up, server verifies checksum (hard-fail on mismatch)":
  runStress("streamUpload")
task stressStreamDownload, "Stress: stream 1 GiB down, client verifies checksum (hard-fail on mismatch)":
  runStress("streamDownload")

task stress, "Stress smoke: all five workloads, short + small (a quick everything-works check)":
  # Discoverability + a fast smoke of the whole set. Defaults to 20s cells and a
  # 64 MiB stream unless overridden; set the NAVI_* knobs for a real soak,
  # or run a single stress<Workload> task.
  if not existsEnv("NAVI_SECONDS"): putEnv("NAVI_SECONDS", "20")
  if not existsEnv("NAVI_STREAM_BYTES"): putEnv("NAVI_STREAM_BYTES", "67108864")
  for w in ["requests", "ws", "sse", "streamUpload", "streamDownload"]:
    runStress(w)

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

task tlsPinning, "In-memory CA bundle + SPKI pinning + verify callback, sync backend (needs openssl)":
  # A server signed by a throwaway CA: navi must trust it via an in-memory
  # caBundle, honor a matching SPKI pin (reject a wrong one), and run the verify
  # callback (accept/reject, and even with chain verification disabled).
  exec "bash tests/interop/tls_pin.sh"

task socks, "SOCKS5 proxy tunnelling + user/pass auth, all native backends (needs python3)":
  # A local HTTP origin behind two SOCKS5 proxies (no-auth and user/pass): navi
  # must tunnel through, authenticate, and reject wrong credentials, on the sync,
  # asyncdispatch and chronos backends.
  exec "bash tests/interop/socks5.sh"

task unixSocket, "Unix domain socket transport, all native backends (POSIX; needs python3)":
  # An AF_UNIX HTTP server that echoes the Host header: navi must dial the socket
  # path, send the URL host as Host, and reject an over-long path, on the sync,
  # asyncdispatch and chronos backends.
  exec "bash tests/interop/unixsocket.sh"

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

proc runBench(workload: string) =
  # Build the bench image and run one workload, passing every NAVI_* knob through.
  # Mirrors runStress: the h3 image (ngtcp2/nghttp3/OpenSSL-3.5 client toolchain +
  # Caddy) is used when NAVI_PROTO is h3/all; h1/h2 use the light image. Each cell
  # prints a ranked throughput+latency table across navi's backends + the reference
  # clients. NB: nimble does not propagate a task's exit code (nim-lang/nimble#1802),
  # so a failed cell shows in the output but this exits 0 -- run the docker command
  # directly, or read the final "== <workload>: all cells ran ==" banner, for
  # CI-grade pass/fail. `--cap-add=NET_ADMIN` is added only for the netem regime.
  let proto = getEnv("NAVI_PROTO", "h2")
  let h3 = proto == "h3" or proto == "all"
  let dockerfile = if h3: "tests/bench/Dockerfile.h3" else: "tests/bench/Dockerfile"
  let image = if h3: "navi-bench-h3" else: "navi-bench"
  let netem = if getEnv("NAVI_NETEM", "0") == "1": "--cap-add=NET_ADMIN " else: ""
  exec "docker build -f " & dockerfile & " -t " & image & " ."
  exec "docker run --rm " & netem & "-e NAVI_WORKLOAD=" & workload &
       " -e NAVI_PROTO -e NAVI_BACKEND -e NAVI_LANGS -e NAVI_SERVERS -e NAVI_PROCS" &
       " -e NAVI_SECONDS -e NAVI_WARMUP_SECONDS -e NAVI_MODE -e NAVI_CLIENTS" &
       " -e NAVI_CONCURRENCY -e NAVI_REQ_COMPRESSION -e NAVI_RESP_COMPRESSION" &
       " -e NAVI_STREAM_BYTES -e NAVI_REPORT_SECONDS" &
       " -e NAVI_NETEM -e NAVI_NETEM_DELAY -e NAVI_NETEM_LOSS " & image

task benchRequests, "Bench: buffered request/response throughput + latency, cross-language":
  runBench("requests")
task benchWs, "Bench: WebSocket echo throughput + latency, cross-language":
  runBench("ws")
task benchSse, "Bench: SSE consume throughput + latency, cross-language":
  runBench("sse")
task benchStreamUpload, "Bench: stream upload throughput (MB/s) + latency, cross-language":
  runBench("streamUpload")
task benchStreamDownload, "Bench: stream download throughput (MB/s) + latency, cross-language":
  runBench("streamDownload")

task bench, "Bench smoke: all five workloads, short + small (cross-language comparison tables)":
  # Discoverability + a fast smoke. Defaults to 10s cells and a 64 MiB stream unless
  # overridden; set the NAVI_* knobs for a real benchmark, or run a single bench<Workload>.
  if not existsEnv("NAVI_SECONDS"): putEnv("NAVI_SECONDS", "10")
  if not existsEnv("NAVI_STREAM_BYTES"): putEnv("NAVI_STREAM_BYTES", "67108864")
  for w in ["requests", "ws", "sse", "streamUpload", "streamDownload"]:
    runBench(w)

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
