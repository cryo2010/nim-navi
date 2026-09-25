## navi/js SSE reconnect-delay client for js_sse_retry.sh (#291): checks that the
## floor under the reconnect delay and the empty-connect backoff are in force on
## the js client too. Run under Node by the shell driver, not by the unit runner.
import std/[options, times]
import navi/js

const Base = "http://127.0.0.1:9533"

proc nowMs(): float = epochTime() * 1000    # float: ms since the epoch overflows a js int

proc sinceMs(t0: float): int = int(nowMs() - t0)

proc main() {.async.} =
  let api = newNavi()

  # /floor answers `retry: 0` plus one event on every connection, then closes.
  # Three events means two reconnects, each of which must wait out the 120 ms
  # floor rather than reconnecting instantly.
  let s = await api.sse(Base & "/floor", retryMs = 20, maxRetryMs = 5000,
                        minRetryMs = 120)
  let t0 = nowMs()
  var got: seq[string]
  for _ in 0 ..< 3:
    let ev = await s.next()
    if ev.isSome: got.add ev.get.data
  let floored = sinceMs(t0)
  s.close()
  doAssert got == @["tick", "tick", "tick"], "floor: got " & $got
  doAssert floored >= 200, "floor: retry: 0 reconnected in " & $floored & " ms"
  echo "floor ok (", floored, " ms for two reconnects)"

  # /flap closes the first four connections with no events at all, so the delay
  # doubles per attempt (40+80+160+320 ms) before the fifth delivers; delivery then
  # resets it to the 20 ms base.
  let f = await api.sse(Base & "/flap", retryMs = 20, maxRetryMs = 1000,
                        minRetryMs = 20)
  let t1 = nowMs()
  let first = await f.next()
  let backedOff = sinceMs(t1)
  let t2 = nowMs()
  let second = await f.next()
  let afterReset = sinceMs(t2)
  f.close()
  doAssert first.isSome and first.get.data == "done", "flap: no first event"
  doAssert second.isSome and second.get.data == "done", "flap: no second event"
  doAssert backedOff >= 450, "flap: empty closes did not back off (" &
    $backedOff & " ms)"
  doAssert afterReset < 400, "flap: a delivered event did not reset the backoff (" &
    $afterReset & " ms)"
  echo "backoff ok (", backedOff, " ms backing off, ", afterReset, " ms after reset)"
  echo "js sse retry: all checks passed"

discard main()
