## SSE over HTTP/3 on the sync backend (#466). `sse()` runs on the CALLER's client,
## so a client whose Alt-Svc cache is already warm opens the stream straight on h3
## with `reconnect = false` (there is no reconnect to upgrade on), and a COLD client's
## SSE response teaches that same client the advertisement, so the caller's next
## request rides h3 too. Also: the owner's `maxResponseBytes` must not cap the
## stream riding the shared h3 connection, while still bounding the owner's own
## buffered and streamed reads on it. Built with -d:ssl -d:naviHttp3.
import std/os
import navi

const
  origin = "https://localhost:4433"
  want = @["one", "two"]        # the /sse route's two events (SSE_BODY in run.sh)

proc main() =
  let ca = getEnv("NAVI_H3_CA")
  doAssert ca.len > 0, "NAVI_H3_CA must point at the origin cert"
  var cfg = initNaviConfig()
  cfg.tls.caFile = ca
  cfg.http = {H1, H2, H3}

  # 1. Warm the Alt-Svc cache with one round trip, then open a NON-reconnecting
  #    stream: its first and only connection has to be h3.
  let api = newNavi(cfg)
  discard api.get(origin & "/")
  let s = api.sse(origin & "/sse", reconnect = false)
  doAssert s.sharesConnections(api), "the stream should run on the caller's client"
  let firstVersion = s.httpVersion    # capture it: `close` drops the handle, after
                                      # which `httpVersion` is "" by design
  doAssert firstVersion == "HTTP/3",
    "sse(reconnect = false) on a warm client should ride h3, got '" & firstVersion & "'"
  var got: seq[string]
  s.each(ev): got.add ev.data
  doAssert got == want, "SSE events over h3: " & $got
  s.close()
  echo "sse(reconnect = false) opened on ", firstVersion, " and delivered ", $got

  # 2. A second stream on the same client is h3 from the first connection too.
  let s2 = api.sse(origin & "/sse", reconnect = false)
  doAssert s2.httpVersion == "HTTP/3", "second stream: " & s2.httpVersion
  var got2: seq[string]
  s2.each(ev): got2.add ev.data
  doAssert got2 == want, "second stream events: " & $got2
  s2.close()
  api.close()

  # 3. A cold client: its first stream goes out on h1/h2 (nothing is known about the
  #    origin yet) and the Alt-Svc that response carries lands in the CALLER's cache.
  let cold = newNavi(cfg)
  let c = cold.sse(origin & "/sse", reconnect = false)
  doAssert c.httpVersion != "HTTP/3",
    "a cold client's first stream cannot be h3, got " & c.httpVersion
  var cgot: seq[string]
  c.each(ev): cgot.add ev.data
  doAssert cgot == want, "cold stream events: " & $cgot
  c.close()
  let after = cold.get(origin & "/")
  doAssert after.httpVersion == "HTTP/3",
    "an Alt-Svc learned BY the SSE response should reach the caller, got " &
    after.httpVersion
  cold.close()
  echo "an Alt-Svc learned by the SSE response moved the caller to ", after.httpVersion
  # 4. The OWNER's `maxResponseBytes` does not reach the stream. On the sync client a
  #    streamed h3 read runs on a dedicated QUIC connection, opened with the owner's
  #    cap as its `max_body` (`sharedConnCap`, like every other h3 opener), so a
  #    streaming submit deliberately asks for NO connection-wide enforcement
  #    (`cap_body = 0`) and the cap applied to a streamed read is the REQUESTING
  #    client's, navi-side per chunk (#466). With a cap far under the SSE body the
  #    stream must still deliver every event, while the owner's own reads keep
  #    stopping at the cap.
  var capped = cfg
  capped.maxResponseBytes = 8          # the /sse body is 43 bytes, /big is 1000
  let owner = newNavi(capped)
  discard owner.head(origin & "/")     # warm Alt-Svc with a body-less response
  let cs = owner.sse(origin & "/sse", reconnect = false)
  doAssert cs.httpVersion == "HTTP/3", "capped-owner stream: " & cs.httpVersion
  var cgot4: seq[string]
  cs.each(ev): cgot4.add ev.data
  doAssert cgot4 == want,
    "an 8-byte owner cap must not truncate the stream, got " & $cgot4
  cs.close()
  # ... and the owner's buffered read of a body over its cap still raises, so the
  # cap was relaxed for the stream only.
  var raised = ""
  try:
    discard owner.get(origin & "/big")
    doAssert false, "a 1000-byte body under an 8-byte cap should raise"
  except ResponseTooLargeError as e:
    raised = $e.name
  doAssert raised == "ResponseTooLargeError", "owner get over its cap: " & raised
  # ... as does the owner's own STREAMED read, which is where the navi-side cap now
  # does all the work.
  var sraised = ""
  try:
    let big = owner.stream.get(origin & "/big")
    big.each(chunk): discard chunk
    doAssert false, "a streamed body over the cap should raise too"
  except ResponseTooLargeError as e:
    sraised = $e.name
  doAssert sraised == "ResponseTooLargeError", "owner stream over its cap: " & sraised
  owner.close()
  echo "an 8-byte owner cap left the h3 stream uncapped and still bounded the owner"
  echo "NAVI HTTP/3 SYNC SSE OK"

main()
