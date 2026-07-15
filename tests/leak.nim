## Steady-state memory-growth check (run via `nimble leak`, not part of the
## default `nimble test` suites because of its size).
##
## Exercises every verb and `request` in a 1,000,000-iteration loop against an
## in-process keep-alive server, then asserts the Nim heap did not grow. This
## catches the leaks ARC/ORC track: connection-pool, cookie-jar, and response
## accumulation. C-side leaks (OpenSSL, zlib, fds) are invisible to
## getOccupiedMem and need LeakSanitizer or valgrind instead.
##
## Run under both memory managers; a growth gap between them means a reference
## cycle (arc leaks cycles, orc collects them):
##   NAVI_MM=orc nimble leak
##   NAVI_MM=arc nimble leak
## NAVI_LEAK_ITERS overrides the loop count.

import unittest
import std/[net, os, strutils]
import navi

const port = 9099
let url = "http://127.0.0.1:" & $port & "/"

var serverReady: bool

proc recvRequest(c: Socket): bool =
  ## Read one full HTTP/1.1 request (headers, then a Content-Length body if any).
  ## Returns false when the peer has closed the connection.
  var data = ""
  while not data.contains("\r\n\r\n"):
    let chunk = c.recv(4096)
    if chunk.len == 0: return false
    data.add chunk
  let headEnd = data.find("\r\n\r\n") + 4
  var clen = 0
  for line in data[0 ..< headEnd].splitLines:
    if line.toLowerAscii.startsWith("content-length:"):
      clen = parseInt(line.split(':', 1)[1].strip)
  var bodyHave = data.len - headEnd
  while bodyHave < clen:
    let chunk = c.recv(4096)
    if chunk.len == 0: return false
    bodyHave += chunk.len
  true

proc serve() {.thread.} =
  # Unbuffered so recv returns whatever is available rather than blocking until
  # the 4096-byte buffer fills (accept inherits the server's buffering).
  var server = newSocket(buffered = false)
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(port), "127.0.0.1")
  server.listen()
  serverReady = true
  const resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
  while true:
    var client: Socket
    try: server.accept(client)
    except CatchableError: break
    while recvRequest(client): client.send(resp)
    client.close()

proc exerciseAll(api: Navi) =
  ## Every HTTP method navi exposes, plus the explicit `request`.
  discard api.get(url)
  discard api.head(url)
  discard api.delete(url)
  discard api.options(url)
  discard api.post(url, body = "x")
  discard api.put(url, body = "x")
  discard api.patch(url, body = "x")
  discard api.request(GET, url)

suite "memory growth":
  test "every verb and request in a 1,000,000x loop does not grow the heap":
    var th: Thread[void]
    createThread(th, serve)
    while not serverReady: sleep(5)

    let iters = parseInt(getEnv("NAVI_LEAK_ITERS", "1000000"))
    let api = newNavi()

    for _ in 0 ..< 1000: exerciseAll(api)   # let pool/jar reach steady state
    GC_fullCollect()
    let base = getOccupiedMem()

    for _ in 0 ..< iters: exerciseAll(api)
    GC_fullCollect()
    let after = getOccupiedMem()
    let growth = after - base

    echo "iterations=", iters, " requests=", iters * 8,
         " baseline=", base, " after=", after, " growth=", growth
    # Steady-state jitter is a few KiB; a real per-request leak over 8M requests
    # would be hundreds of MB (or OOM), so a generous bound flags it unambiguously.
    check growth < 8 * 1024 * 1024
    # The server thread blocks on the pooled connection; the process exit reaps it.
