## HTTP/1.1 keep-alive race classification (sync entry, in-process plain-TCP peers).
## Mirrors the h2 race suite for the two h1-specific gaps this branch closes:
##   * a 1xx interim (100/103) that arrives before a drop must NOT be a keep-alive race
##     -- the peer began responding, so it stays a plain IOError, never auto-replayed
##     (finding 5: the parser discarded the interim and lost the "response began" signal).
##   * a reused pooled connection that fails BEFORE any response -- at WRITE time (the
##     server already closed it) or at read time -- is the keep-alive race, so an
##     idempotent method is replayed on a fresh connection (finding 6: a write-time
##     failure used to surface a raw transport error the replay layer declined).
##
## Plain 127.0.0.1 TCP, no TLS: macOS cannot dlopen libcrypto in this harness.
import unittest
import navi
import navi/core/response as naviresp   # KeepAliveRaceError qualifier
import ./support_h1race

var interimThread, reusedThread: Thread[H1RaceCtx]

suite "http/1.1 keep-alive race":
  test "a 1xx interim before the drop is a plain IOError, NOT a race":
    var port = 0
    var accepts = 0
    startInterimThenClose(interimThread, port, addr accepts)
    let api = newNavi()
    let key = "http://127.0.0.1:" & $port
    var raised: ref Exception
    try:
      discard api.request(POST, key & "/submit", body = "data")
    except CatchableError as e:
      raised = e
    check raised != nil
    check raised of IOError                   # the peer began responding (103)
    check not (raised of naviresp.KeepAliveRaceError)   # so NOT the ambiguous race
    joinThread(interimThread)
    check accepts == 1                         # sent once, never replayed

  test "an idempotent request is replayed when a reused connection drops pre-response":
    # The reused pooled connection fails before any response (write- or read-time). PUT is
    # idempotent, so the ambiguous race is safe to replay: it lands on a fresh connection.
    var port = 0
    var accepts = 0
    var closed1 = false
    startReusedDrop(reusedThread, port, addr closed1, addr accepts)
    let api = newNavi()
    let key = "http://127.0.0.1:" & $port
    check (api.get(key & "/")).status == 200   # connection 1, then pooled
    waitFlag(addr closed1)                      # server closed the pooled connection
    let r = api.put(key & "/submit", body = "data")
    check r.status == 200
    check r.body == "replayed:data"             # served on the fresh connection
    joinThread(reusedThread)
    check accepts == 2                          # first (pooled) dropped, retry served
