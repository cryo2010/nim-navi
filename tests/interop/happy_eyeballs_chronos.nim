## Happy Eyeballs (RFC 8305), chronos backend. Driven by happy_eyeballs.sh, which
## passes a blackholed first address (SYN dropped) and a good 127.0.0.1 TCP target.
## navi must *race* the addresses and reach the good one in about the attempt delay
## (~250ms). This exercises the TCP race directly (no TLS).
import std/[os, strutils, monotimes, times]
import pkg/chronos
import navi/backend/chronos

let port = parseInt(getEnv("NAVI_HE_PORT"))
let blackhole = getEnv("NAVI_HE_BLACKHOLE")   # an address whose SYN is dropped

proc timeRace(addrs: seq[TransportAddress]): Future[int] {.async.} =
  ## Milliseconds to win the connect race, closing the winning transport.
  let t0 = getMonoTime()
  let (transport, idx) = await happyConnect(addrs)
  await transport.closeWait()
  doAssert idx >= 0
  return (getMonoTime() - t0).inMilliseconds.int

let good = initTAddress("127.0.0.1", Port(port))
let bh = initTAddress(blackhole, Port(port))

# Sanity: the good address on its own connects.
doAssert (waitFor timeRace(@[good])) >= 0, "the good address should connect"

# Happy Eyeballs: blackhole first, good second.
let ms = waitFor timeRace(@[bh, good])
echo "OK  raced past blackhole ", blackhole, " to 127.0.0.1 in ", ms, "ms"
doAssert ms < 1500,
  "Happy Eyeballs should beat a sequential connect, got " & $ms & "ms"

echo "== happy eyeballs (chronos) passed =="
