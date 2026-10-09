## SSE connection sharing over a real HTTP/2 origin (#466), shared by client.nim
## (asyncdispatch) and client_chronos.nim: each imports its backend, defines
## `backendName` and `napMs`, then `include`s this.
##
## What it pins, which only a TLS h2 origin can show (navi speaks h2 over TLS only,
## so none of this is reachable from the plain-TCP unit suite):
##
##  1. An `sse()` stream runs on the caller's client (its jar, its Alt-Svc cache, its
##     TLS session cache) but NOT on the h2 connection the client's requests use. An
##     h2 connection has ONE read bound for every stream on it, taken from the
##     `timeouts.read` of whoever opened it, and its expiry kills the connection and
##     all of its streams -- so the client's bounded requests and a stream that must
##     run unbounded cannot ride the same one. Here the owner's request to a route
##     that is never answered dies at its `timeouts.read` while the stream, whose
##     server goes silent for twice that long, delivers everything.
##  2. The streams of ONE client do share a connection with each other, so an h2
##     origin costs one extra handshake for the first stream and nothing after it.
##  3. `client.close()` reaps the stream connection as well as the request one.
##
## Connection accounting comes from the origin: /conns answers "<opened> <closed>"
## h2 sessions. The probe client is created and used FIRST so its own connection is
## counted before any delta is taken, and it keeps that one connection for the rest
## of the run.

proc conns(probe: Navi, base: string): Future[(int, int)] {.async.} =
  let body = (await probe.get(base & "/conns")).body
  let parts = body.split(' ')
  doAssert parts.len == 2, "bad /conns answer: [" & body & "]"
  return (parseInt(parts[0]), parseInt(parts[1]))

proc main() {.async.} =
  let base = "https://127.0.0.1:" & getEnv("SSE_PORT", "8443")
  var cfg = initNaviConfig()
  cfg.tls.verify = false             # self-signed test cert
  cfg.http = {H2}
  cfg.retry.limit = 0                # no replay above the mux: one attempt, one error
  cfg.timeouts.read = 1500           # the owner's bound; the server's gap is 3 s

  # --- 1. the owner's read bound must not reach the stream -------------------------
  let a = newNavi(cfg)
  let s = await a.sse(base & "/events", reconnect = false, idleTimeoutMs = 20_000)
  doAssert s.sharesConnections(a), "the stream should run on the caller's client"
  doAssert not s.sharesH2Connections(a),
    "the stream must not ride the h2 connection the caller's requests use"
  let ev1 = await s.next()
  doAssert ev1.isSome and ev1.get.data == "one", "first event: " & $ev1
  var stalled = ""
  let t0 = epochTime()
  try:
    discard await a.get(base & "/stall")
    doAssert false, "a route that is never answered should not succeed"
  except CatchableError as e:
    stalled = $e.name
  let stallMs = int((epochTime() - t0) * 1000.0)
  doAssert stalled.len > 0
  doAssert stallMs >= 1400, "it gave up before its read bound: " & $stallMs & " ms"
  doAssert stallMs < 12_000, "it did not give up: " & $stallMs & " ms"
  # The owner's next request works: its dead connection was retired and this one
  # reconnects (the replay path `timeouts.read` has always driven).
  doAssert (await a.get(base & "/plain")).status == 200, "the owner's next request"
  # ...and the stream is untouched by that expiry: its second event comes after a
  # silence twice the owner's bound.
  let ev2 = await s.next()
  doAssert ev2.isSome and ev2.get.data == "two", "second event: " & $ev2
  await s.close()
  await a.close()
  echo "[", backendName, "] owner request died at ", stallMs,
       " ms (", stalled, ") and the stream survived"

  # --- 2/3. one connection for all of a client's streams, and close() reaps it -----
  let probe = newNavi(cfg)
  discard await probe.conns(base)     # the probe's own connection, counted here
  let b = newNavi(cfg)
  let (o0, _) = await probe.conns(base)
  doAssert (await b.get(base & "/plain")).status == 200
  let s1 = await b.sse(base & "/events", reconnect = false, idleTimeoutMs = 20_000)
  let s2 = await b.sse(base & "/events", reconnect = false, idleTimeoutMs = 20_000)
  doAssert s1.sharesConnections(b) and s2.sharesConnections(b)
  let (o1, c0) = await probe.conns(base)
  doAssert o1 - o0 == 2,
    "expected 2 connections (requests + one shared by both streams), got " & $(o1 - o0)
  await s1.close()
  await s2.close()
  await b.close()
  var c1 = c0
  for _ in 0 ..< 50:                  # the origin notices a close asynchronously
    let (_, c) = await probe.conns(base)
    c1 = c
    if c1 - c0 >= 2: break
    await napMs(100)
  doAssert c1 - c0 == 2,
    "close() should reap the stream connection too, closed delta: " & $(c1 - c0)
  await probe.close()
  echo "[", backendName, "] 2 streams shared 1 connection and close() reaped both"
  echo "H2_SSE_SHARE_OK"

waitFor main()
