## Sans-io SOCKS5 handshake frame building and reply parsing, plus the sync
## backend's wall-clock bound on the live handshake (issue #452).
import unittest
import std/[strutils, monotimes, times]
import navi
import navi/core/socks
import navi/core/response  # for the `response.TimeoutError` qualifier
import ./support

suite "SOCKS5 greeting and method selection":
  test "the greeting should offer only no-auth without credentials":
    check greeting(false) == "\x05\x01\x00"

  test "the greeting should offer no-auth and user/pass with credentials":
    check greeting(true) == "\x05\x02\x00\x02"

  test "selectedMethod should return the chosen method byte":
    check selectedMethod("\x05\x00") == methodNoAuth
    check selectedMethod("\x05\x02") == methodUserPass
    check selectedMethod("\x05\xFF") == methodNone

  test "selectedMethod should reject a non-SOCKS5 reply":
    expect SocksError: discard selectedMethod("\x04\x00")

suite "SOCKS5 username/password auth (RFC 1929)":
  test "authRequest should length-prefix the username and password":
    check authRequest("me", "pw") == "\x01\x02me\x02pw"

  test "checkAuthReply should accept a zero status":
    var accepted = true
    try: checkAuthReply("\x01\x00")
    except SocksError: accepted = false
    check accepted

  test "checkAuthReply should reject a non-zero status":
    expect SocksError: checkAuthReply("\x01\x01")

  test "authRequest should reject an over-long credential":
    expect SocksError: discard authRequest("u", "p".repeat(256))

suite "SOCKS5 connect request and reply":
  test "connectRequest should use the domain address type and network-order port":
    check connectRequest("ex.com", 80) == "\x05\x01\x00\x03\x06ex.com\x00\x50"

  test "connectRequest should reject an empty or over-long host":
    expect SocksError: discard connectRequest("", 80)
    expect SocksError: discard connectRequest("h".repeat(256), 80)

  test "replyStatus should return REP for a valid header":
    check replyStatus("\x05\x00\x00\x01") == 0
    check replyStatus("\x05\x05\x00\x01") == 5

  test "replyStatus should reject a malformed header":
    expect SocksError: discard replyStatus("\x04\x00\x00\x01")

  test "boundTailLen should size the bound address by type":
    check boundTailLen(0x01) == 6      # IPv4 + port
    check boundTailLen(0x04) == 18     # IPv6 + port
    check boundTailLen(0x03) == -1     # domain: length byte follows

  test "boundTailLen should reject an unknown address type":
    expect SocksError: discard boundTailLen(0x09)

suite "the sync SOCKS5 handshake is bounded by the connect budget (#452)":
  test "a trickling connect reply trips the connect timeout":
    # The proxy selects no-auth promptly, then dribbles the connect reply one byte
    # every 100 ms: a domain-type bound address of 4 + 1 + 60 + 2 bytes, ~6.7 s in
    # all. `sockReadExactly` recv'd each byte under a fresh 300 ms SO_RCVTIMEO, so
    # nothing ever expired; the budget has to be wall-clock to stop it.
    var port = 0
    var th: Thread[TrickleCtx]
    let reply = "\x05\x00\x00\x03" & char(60) & repeat('x', 60) & "\x00\x50"
    startTrickleProxy(th, port, reply, 100, socks = true)
    var cfg = initNaviConfig()
    cfg.proxy = "socks5://127.0.0.1:" & $port
    cfg.timeouts.connect = 300
    cfg.retry.limit = 0                      # one attempt: measure one budget
    let api = newNavi(cfg)
    let t0 = getMonoTime()
    var raised = "none"
    try:
      discard api.get("http://tunnel.test/")  # SOCKS5 tunnels plain http too
    except response.TimeoutError:
      raised = "timeout"
    except CatchableError as e:
      raised = "other:" & $e.name
    let elapsed = (getMonoTime() - t0).inMilliseconds.int
    check raised == "timeout"
    check elapsed < 1500
    joinThread(th)
