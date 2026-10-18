## Event-loop lag for the mixed cells (#462): how late a 10 ms timer actually
## fires while every slice runs on one loop. Pure (std/strutils only) so the
## native clients and `nim js` share it, like mixsplit.
##
## Why the mixed line carries it: a slice whose rate collapses next to the stream
## slices can be starved on the CLIENT's event loop rather than slowed by the
## server or the protocol. Before #462 a saturated h1 TLS transfer completed
## read after read without ever handing the loop back, so this probe measured an
## average lag of 165-245 ms in the mixed h1 cell (a 10 ms sleep took ~175 ms)
## while the requests slice sat at ~1/50 of its solo rate; with the fix it is a
## few ms. A large number here points at the client loop, not the origin.

import std/strutils

const lagTickMs* = 10
  ## The probe's sleep: short enough to see a starved turn, long enough to cost
  ## nothing next to the slices it shares the loop with.

type LoopLag* = ref object
  samples*: int
  totalMs*: float
  worstMs*: float

proc newLoopLag*(): LoopLag = LoopLag()

proc add*(l: LoopLag, sleptMs: float) =
  ## Record one tick: `sleptMs` is how long the `lagTickMs` sleep really took.
  let lag = max(0.0, sleptMs - lagTickMs.float)
  inc l.samples
  l.totalMs += lag
  l.worstMs = max(l.worstMs, lag)

proc render*(l: LoopLag): string =
  ## "lag avg 6.8ms max 56.6ms", or "lag n/a" before the first tick.
  if l.samples == 0: return "lag n/a"
  "lag avg " & formatFloat(l.totalMs / l.samples.float, ffDecimal, 1) &
    "ms max " & formatFloat(l.worstMs, ffDecimal, 1) & "ms"
