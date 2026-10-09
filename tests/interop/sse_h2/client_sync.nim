## SSE-over-HTTP/2 connection sharing, SYNC backend (#466). The sync client's claims
## are the opposite of the async ones, so they get their own scenario rather than a
## place in `share_spec.nim`:
##
##  1. `sharesH2Connections` is TRUE here. A sync h2 connection lives in the ordinary
##     pool and is checked out exclusively for one request at a time, so a stream and
##     the client's requests take turns on it instead of needing separate ones. The
##     origin's session count proves it: the stream opens no new connection.
##  2. `maxResponseBytes` is applied PER STREAM on that pooled connection, not per
##     connection. The client's cap is 8 bytes, far under the event stream, yet the
##     stream (whose view runs with the cap off) reads it all while the client's own
##     request for a 1000-byte body still raises `ResponseTooLargeError`.
##  3. A stream that ENDS on its own leaves its connection pooled and reusable, while
##     `close()` MID-STREAM -- the usual case for an open-ended event stream -- closes
##     that connection outright. Unlike the async backends, which RST the stream and
##     keep the connection, this backend has no background reader that could drain
##     the rest of a half-read response off a connection worth keeping.
##
## Needs a TLS h2 origin, so none of it is reachable from the plain-TCP unit suite.
## Driven by run.sh.
import navi
import std/[os, strutils]

proc conns(probe: Navi, base: string): (int, int) =
  let body = probe.get(base & "/conns").body
  let parts = body.split(' ')
  doAssert parts.len == 2, "bad /conns answer: [" & body & "]"
  (parseInt(parts[0]), parseInt(parts[1]))

let base = "https://127.0.0.1:" & getEnv("SSE_PORT", "8443")
var cfg = initNaviConfig()
cfg.tls.verify = false             # self-signed test cert
cfg.http = {H2}
cfg.retry.limit = 0                # no replay: one attempt, one error

# The probe reads /conns, so it stays uncapped and keeps one connection for the whole
# run: it is created and used FIRST, so its own session is counted before any delta.
let probe = newNavi(cfg)
discard probe.conns(base)

var capped = cfg
capped.maxResponseBytes = 8        # /events sends 34 body bytes, /big sends 1000
let api = newNavi(capped)

let (o0, _) = probe.conns(base)
doAssert api.get(base & "/plain").status == 200, "the owner's first request"

# --- 1. the stream rides the very connection that request opened --------------------
let s = api.sse(base & "/events", reconnect = false, idleTimeoutMs = 20_000)
doAssert s.sharesConnections(api), "the stream should run on the caller's client"
doAssert s.sharesH2Connections(api),
  "a sync stream shares the pooled h2 connection with the client's requests"
doAssert s.httpVersion == "HTTP/2", "the stream should be on h2: " & s.httpVersion
# 17 bytes of event framing, already over the owner's 8-byte cap: reading it at all
# is the per-stream cap working.
let ev1 = s.next()
doAssert ev1.isSome and ev1.get.data == "one", "first event: " & $ev1
let (o1, _) = probe.conns(base)
doAssert o1 - o0 == 1,
  "the stream should have reused the request connection, opened delta " & $(o1 - o0)

# --- 2. the owner's own cap still bites, on the same pool, while the stream runs ----
var raised = ""
try:
  discard api.get(base & "/big")
  doAssert false, "a 1000-byte body under an 8-byte cap should raise"
except ResponseTooLargeError as e:
  raised = $e.name
doAssert raised == "ResponseTooLargeError", "the owner's get over its cap: " & raised

# --- 3a. a stream that ends on its own leaves its connection reusable ---------------
let ev2 = s.next()                 # after the server's silent gap
doAssert ev2.isSome and ev2.get.data == "two", "second event: " & $ev2
doAssert s.next().isNone, "the stream should end with reconnect off"
let (o2, _) = probe.conns(base)
s.close()                          # a no-op: that last read already pooled it
doAssert api.get(base & "/plain").status == 200, "the owner's request after the stream"
let (o3, _) = probe.conns(base)
doAssert o3 - o2 == 0,
  "a drained stream's connection should be reusable, opened delta " & $(o3 - o2)

# --- 3b. close() mid-stream costs the connection ------------------------------------
let s2 = api.sse(base & "/events", reconnect = false, idleTimeoutMs = 20_000)
doAssert s2.next().isSome, "the second stream's first event"
let (_, c0) = probe.conns(base)
s2.close()                         # mid-body: the connection cannot be pooled
var c1 = c0
for _ in 0 ..< 50:                 # the origin notices a close asynchronously
  let (_, c) = probe.conns(base)
  c1 = c
  if c1 - c0 >= 1: break
  sleep(100)
doAssert c1 - c0 >= 1,
  "close() mid-stream should close its h2 connection, closed delta " & $(c1 - c0)
api.close()
probe.close()

echo "[sync] the stream shared the pooled h2 connection, read past the owner's ",
     "8-byte cap, and close() released the connection"
echo "H2_SSE_SYNC_OK"
