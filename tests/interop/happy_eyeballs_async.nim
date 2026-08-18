## Happy Eyeballs (RFC 8305), asyncdispatch backend. Driven by happy_eyeballs.sh,
## which passes a blackholed first address (SYN dropped) and a good 127.0.0.1 TCP
## target. navi must *race* the addresses and reach the good one in about the
## attempt delay (~250ms), far under the per-address budget a sequential try would
## first burn on the blackhole. This exercises the TCP race directly (no TLS).
import std/[asyncdispatch, os, strutils, monotimes, times]
import navi/backend/asyncdispatch

let port = parseInt(getEnv("NAVI_HE_PORT"))
let blackhole = getEnv("NAVI_HE_BLACKHOLE")   # an address whose SYN is dropped

proc timeRace(ips: seq[string]): Future[int] {.async.} =
  ## Milliseconds to win the connect race, closing the winning socket.
  let t0 = getMonoTime()
  let (fd, idx) = await happyConnect(ips, port)
  closeSocket(fd)
  doAssert idx >= 0
  return (getMonoTime() - t0).inMilliseconds.int

# Sanity: the good address on its own connects.
doAssert (waitFor timeRace(@["127.0.0.1"])) >= 0, "the good address should connect"

# Happy Eyeballs: blackhole first, good second. The racer reaches the good address
# in ~the attempt delay; a sequential try would stall on the blackhole first.
let ms = waitFor timeRace(@[blackhole, "127.0.0.1"])
echo "OK  raced past blackhole ", blackhole, " to 127.0.0.1 in ", ms, "ms"
doAssert ms < 1500,
  "Happy Eyeballs should beat a sequential connect, got " & $ms & "ms"

echo "== happy eyeballs (asyncdispatch) passed =="
