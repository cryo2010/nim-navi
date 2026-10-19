import navi
import std/[os, strutils]

var cfg = initNaviConfig()
cfg.tls.verify = false          # self-signed test cert
cfg.http = {H2}                  # WebSocket over h2 Extended CONNECT (RFC 8441)
cfg.retry.limit = 0
# A response-body cap far below what the tunnel carries below (#470): it bounds a
# response body, never a WebSocket, so the tunnel must outlive it. Step 2's ~312 KB
# echo alone is 300x the cap.
cfg.maxResponseBytes = 1024
let api = newNavi(cfg)
let port = getEnv("WS_PORT", "8443")
let ws = api.websocket("wss://127.0.0.1:" & port & "/chat")

# 1. A small text frame round-trips.
ws.send("hello over h2 sync")
let msg = ws.receive()
doAssert msg.kind == wmText, "expected text, got " & $msg.kind
doAssert msg.data == "hello over h2 sync", "echo mismatch: [" & msg.data & "]"

# 2. A binary frame larger than the peer's initial 64 KiB send window: the send
#    must pump the socket for WINDOW_UPDATEs (buffering the concurrent echo) and
#    the receive must reassemble one WS frame split across many h2 DATA frames.
let big = "navi-h2-sync-".repeat(24000)     # ~312 KB
ws.send(big, binary = true)
let echoed = ws.receive()
doAssert echoed.kind == wmBinary, "expected binary, got " & $echoed.kind
doAssert echoed.data.len == big.len, "big echo length " & $echoed.data.len &
  " != " & $big.len
doAssert echoed.data == big, "big echo payload mismatch"

# 3. The tunnel stays healthy for more traffic after the large transfer.
for i in 1 .. 3:
  let p = "ping-" & $i
  ws.send(p)
  let r = ws.receive()
  doAssert r.kind == wmText and r.data == p, "round-trip " & $i & " mismatch"

ws.close()

# 4. maxMessageBytes is still the WebSocket's size bound.
let small = api.websocket("wss://127.0.0.1:" & port & "/chat", maxMessageBytes = 256)
small.send("x".repeat(512))
var tooBig = false
try: discard small.receive()
except WsMessageTooLarge: tooBig = true
doAssert tooBig, "maxMessageBytes did not reject a 512-byte message"
small.close()

# 5. A plain h2 response on the same client still honours maxResponseBytes.
var capped = false
try: discard api.get("https://127.0.0.1:" & port & "/big")
except ResponseTooLargeError: capped = true
doAssert capped, "a 4 KiB h2 response passed maxResponseBytes = 1024"

echo "H2_WS_SYNC_OK payload=", msg.data, " big=", $big.len, " cap=", cfg.maxResponseBytes
api.close()
