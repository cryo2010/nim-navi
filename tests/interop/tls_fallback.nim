## Handshake-aware address fallback (sync backend). Driven by tls_fallback.sh,
## which stands up a good TLS server on 127.0.0.1 and a dead endpoint (accepts
## TCP, then drops the connection so the TLS handshake fails) on a second
## loopback address, both on the same port. navi's `connectAcross` must fall
## through the dead address to the good one -- something std/net's `dial`, which
## stops at the first address that merely TCP-connects, cannot do.

import std/[os, strutils]
import navi/backend/sync
import navi/backend/openssl_ctx

let cert = getEnv("NAVI_FB_CERT")
let port = parseInt(getEnv("NAVI_FB_PORT"))
let badIp = getEnv("NAVI_FB_BADIP")

var tls = defaultTls()
tls.caFile = cert                       # trust the self-signed test cert
let ctx = newTlsContext(tls, @["http/1.1"])

# Dead address first, good address second: the handshake must fall through.
block:
  var c = connectAcross(ctx, @[badIp, "127.0.0.1"], "127.0.0.1", port, true)
  sendAll(c, "GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
  let line = recvSome(c).splitLines()[0]  # first read holds the status line
  doAssert "200" in line, "expected 200 from the good server, got: " & line
  c.close()
  echo "OK  fell through ", badIp, " (dead) -> 127.0.0.1 (good): ", line

# Only the dead address: must raise (nothing to fall through to).
block:
  var raised = false
  try: discard connectAcross(ctx, @[badIp], "127.0.0.1", port, true)
  except CatchableError: raised = true
  doAssert raised, "an all-dead address list should raise"
  echo "OK  all-dead address list raised"

echo "== tls fallback passed =="
