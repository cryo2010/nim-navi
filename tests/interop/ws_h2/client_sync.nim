import navi
import std/[os, strutils]

var cfg = initNaviConfig()
cfg.tls.verify = false          # self-signed test cert
cfg.http = {H2}                  # WebSocket over h2 Extended CONNECT (RFC 8441)
cfg.retry.limit = 0
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

echo "H2_WS_SYNC_OK payload=", msg.data, " big=", $big.len
ws.close()
api.close()
