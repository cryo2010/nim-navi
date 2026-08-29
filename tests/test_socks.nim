## Sans-io SOCKS5 handshake frame building and reply parsing.
import unittest
import std/strutils
import navi/core/socks

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
