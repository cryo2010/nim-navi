## SSE over HTTP/3 on the asyncdispatch backend (#466). `sse()` runs on the CALLER's
## client, so two things the private-client design could not do must now hold against
## a live h3 origin:
##   1. a client whose Alt-Svc cache is already warm opens the stream straight on h3,
##      with `reconnect = false` -- there is no reconnect to upgrade on;
##   2. a COLD client's SSE response teaches that same client the advertisement, so
##      the caller's next request rides h3 too.
## Plus: a second `sse()` must reuse the client's h3 connection, not open its own,
## and the owner's `maxResponseBytes` must not cap the stream riding that shared
## connection while it still bounds the owner's own buffered and streamed reads.
## Built with -d:ssl -d:naviHttp3.
import std/[os, asyncdispatch]
import navi/asyncdispatch

const
  origin = "https://localhost:4433"
  want = @["one", "two"]        # the /sse route's two events (SSE_BODY in run.sh)

proc main() {.async.} =
  let ca = getEnv("NAVI_H3_CA")
  doAssert ca.len > 0, "NAVI_H3_CA must point at the origin cert"
  var cfg = initNaviConfig()
  cfg.tls.caFile = ca
  cfg.http = {H1, H2, H3}

  # 1. Warm the Alt-Svc cache with one h2 round trip, then open a NON-reconnecting
  #    stream: its first and only connection has to be h3.
  let api = newNavi(cfg)
  let warm = await api.get(origin & "/")
  doAssert warm.httpVersion == "HTTP/2", "warm-up should be h2, got " & warm.httpVersion
  let s = await api.sse(origin & "/sse", reconnect = false)
  doAssert s.sharesConnections(api), "the stream should run on the caller's client"
  let firstVersion = s.httpVersion    # capture it: `close` drops the handle, after
                                      # which `httpVersion` is "" by design
  doAssert firstVersion == "HTTP/3",
    "sse(reconnect = false) on a warm client should ride h3, got '" & firstVersion & "'"
  var got: seq[string]
  s.each(ev): got.add ev.data
  doAssert got == want, "SSE events over h3: " & $got
  await s.close()
  doAssert api.h3ConnCount() == 1,
    "the stream should use the client's h3 connection, not one of its own"
  echo "sse(reconnect = false) opened on ", firstVersion, " and delivered ", $got

  # 2. A second stream on the same client reuses that one h3 connection.
  let s2 = await api.sse(origin & "/sse", reconnect = false)
  doAssert s2.httpVersion == "HTTP/3", "second stream: " & s2.httpVersion
  var got2: seq[string]
  s2.each(ev): got2.add ev.data
  doAssert got2 == want, "second stream events: " & $got2
  await s2.close()
  doAssert api.h3ConnCount() == 1, "a second sse() must not open a second h3 connection"
  await api.close()
  echo "a second sse() reused the same h3 connection"

  # 3. A cold client: its first stream goes out on h2 (nothing is known about the
  #    origin yet) and the Alt-Svc that response carries lands in the CALLER's cache.
  let cold = newNavi(cfg)
  let c = await cold.sse(origin & "/sse", reconnect = false)
  doAssert c.httpVersion == "HTTP/2",
    "a cold client's first stream is h2, got '" & c.httpVersion & "'"
  var cgot: seq[string]
  c.each(ev): cgot.add ev.data
  doAssert cgot == want, "cold stream events: " & $cgot
  await c.close()
  let after = await cold.get(origin & "/")
  doAssert after.httpVersion == "HTTP/3",
    "an Alt-Svc learned BY the SSE response should reach the caller, got " &
    after.httpVersion
  await cold.close()
  echo "an Alt-Svc learned by the SSE response moved the caller to ", after.httpVersion
  # 4. The OWNER's `maxResponseBytes` does not reach the stream. A client and its
  #    `sse()` view share one QUIC connection, and the driver holds the cap as that
  #    connection's `max_body`, so a streaming submit deliberately asks for NO
  #    connection-wide enforcement (`cap_body = 0`) and the cap applied to a streamed
  #    read is the REQUESTING client's, navi-side per chunk (#466). With a cap far
  #    under the SSE body, the stream must still deliver every event while the
  #    owner's own reads over that same connection still stop at the cap.
  var capped = cfg
  capped.maxResponseBytes = 8          # the /sse body is 43 bytes, /big is 1000
  let owner = newNavi(capped)
  discard await owner.head(origin & "/")   # warm Alt-Svc with a body-less response
  let cs = await owner.sse(origin & "/sse", reconnect = false)
  doAssert cs.httpVersion == "HTTP/3", "capped-owner stream: " & cs.httpVersion
  var cgot4: seq[string]
  cs.each(ev): cgot4.add ev.data
  doAssert cgot4 == want,
    "an 8-byte owner cap must not truncate the stream, got " & $cgot4
  await cs.close()
  doAssert owner.h3ConnCount() == 1, "the capped owner should have one h3 connection"
  # ... and the owner's buffered read of a body over its cap still raises, so the
  # cap was relaxed for the stream only.
  var raised = ""
  try:
    discard await owner.get(origin & "/big")
    doAssert false, "a 1000-byte body under an 8-byte cap should raise"
  except ResponseTooLargeError as e:
    raised = $e.name
  doAssert raised == "ResponseTooLargeError", "owner get over its cap: " & raised
  # ... as does the owner's own STREAMED read, which is where the navi-side cap now
  # does all the work.
  var sraised = ""
  try:
    let big = await owner.stream.get(origin & "/big")
    big.each(chunk): discard chunk
    doAssert false, "a streamed body over the cap should raise too"
  except ResponseTooLargeError as e:
    sraised = $e.name
  doAssert sraised == "ResponseTooLargeError", "owner stream over its cap: " & sraised
  await owner.close()
  echo "an 8-byte owner cap left the h3 stream uncapped and still bounded the owner"
  echo "NAVI HTTP/3 SSE OK"

waitFor main()
