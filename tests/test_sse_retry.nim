## Sync SSE reconnect-delay floor and empty-connect backoff (#291). A server that
## sends `retry: 0`, or that answers 200 and closes with no events, used to drive
## the reconnect loop with no delay at all: `sleep(0)` in a tight loop hammering
## the server. Plain HTTP over a loopback socket (no TLS), so it runs on this host.
import unittest
import std/[options, monotimes, os, times]
import navi
import ./support_sse

proc elapsedMs(t0: MonoTime): int = (getMonoTime() - t0).inMilliseconds.int

suite "sync SSE reconnect floor (#291)":
  test "a server's retry: 0 should be floored, not reconnected instantly":
    # Every connection carries `retry: 0` plus one event, then closes. The stream
    # must still wait out the floor before each reconnect: three events means two
    # reconnects, so at least 2 x 120 ms of sleeping.
    var th: Thread[SseFlapSrv]
    var port, conns: int
    var cfg = SseFlapSrv(conns: addr conns, emptyConns: 0, total: 3,
                         preamble: "retry: 0\ndata: tick\n\n")
    startSseFlap(th, port, cfg)
    let api = newNavi()
    let s = api.sse("http://127.0.0.1:" & $port & "/events",
                    retryMs = 20, maxRetryMs = 5000, minRetryMs = 120)
    let t0 = getMonoTime()
    var got: seq[string]
    for _ in 0 ..< 3:
      let ev = s.next()
      check ev.isSome
      if ev.isSome: got.add ev.get.data
    let took = elapsedMs(t0)
    s.close()
    check got == @["tick", "tick", "tick"]
    check conns == 3                       # one connection per event
    check took >= 200                      # two floored reconnects, not sleep(0)
    drainSseFlap(port, 3, addr conns)
    joinThread(th)

  test "repeated zero-event closes should back off, and an event should reset it":
    # The first four connections answer 200 and close with nothing in them, so the
    # delay doubles per attempt: 40, 80, 160, 320 ms before the fifth connection
    # finally delivers. Delivery then resets the delay to the 20 ms base, so the
    # next event arrives promptly instead of after another 640 ms.
    var th: Thread[SseFlapSrv]
    var port, conns: int
    var cfg = SseFlapSrv(conns: addr conns, emptyConns: 4, total: 6,
                         event: "data: done\n\n")
    startSseFlap(th, port, cfg)
    let api = newNavi()
    let s = api.sse("http://127.0.0.1:" & $port & "/events",
                    retryMs = 20, maxRetryMs = 1000, minRetryMs = 20)
    let t0 = getMonoTime()
    let first = s.next()
    let backedOff = elapsedMs(t0)
    let t1 = getMonoTime()
    let second = s.next()
    let afterReset = elapsedMs(t1)
    s.close()
    check first.isSome and first.get.data == "done"
    check second.isSome and second.get.data == "done"
    check conns == 6
    check backedOff >= 450                 # 40+80+160+320, not 4 x 20 ms
    check afterReset < 400                 # reset to the base, not still backing off
    drainSseFlap(port, 6, addr conns)
    joinThread(th)
