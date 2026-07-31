## Happy Eyeballs (RFC 8305), sync backend. Driven by happy_eyeballs.sh, which
## stands up a good TLS server on 127.0.0.1 and passes a blackholed first address
## (SYN dropped). navi must *race* the addresses and reach the good one in about
## the attempt delay (~250ms) -- far under the per-address connect budget a
## sequential try would first burn on the blackhole.
import std/[os, strutils, monotimes, times]
import navi/backend/sync
import navi/backend/openssl_ctx

let cert = getEnv("NAVI_HE_CERT")
let port = parseInt(getEnv("NAVI_HE_PORT"))
let blackhole = getEnv("NAVI_HE_BLACKHOLE")   # an address whose SYN is dropped

var tls = defaultTls()
tls.caFile = cert                              # trust the self-signed test cert
let ctx = newTlsContext(tls, @["http/1.1"])

proc timeConnect(ips: seq[string], connectMs: int): int =
  ## Milliseconds to connect + read the status line (200), or -1 on failure.
  let t0 = getMonoTime()
  try:
    var c = connectAcross(ctx, ips, "127.0.0.1", port, false, nil, connectMs)
    sendAll(c, "GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
    let line = recvSome(c).splitLines()[0]
    c.close()
    doAssert "200" in line, "expected 200 from the good server, got: " & line
    result = (getMonoTime() - t0).inMilliseconds.int
  except CatchableError as e:
    echo "  connect failed: ", e.msg
    result = -1

# Sanity: the good address on its own connects.
doAssert timeConnect(@["127.0.0.1"], 0) >= 0, "the good address should connect"

# Happy Eyeballs: blackhole first, good second, with a 2s per-address budget. The
# racer reaches the good address in ~the 250ms attempt delay; a sequential try
# would first spend the full 2s failing the blackhole.
let ms = timeConnect(@[blackhole, "127.0.0.1"], 2000)
doAssert ms >= 0, "must race past the blackhole to the good address"
echo "OK  raced past blackhole ", blackhole, " to 127.0.0.1 in ", ms, "ms"
doAssert ms < 1500,
  "Happy Eyeballs should beat a sequential connect (blackhole budget 2000ms), got " &
  $ms & "ms"

echo "== happy eyeballs passed =="
