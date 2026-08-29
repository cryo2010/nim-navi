## buildRequest identity-header defaults (User-Agent, Accept, Accept-Encoding).
import unittest, std/strutils
import navi/core/[headers, request, version]

proc built(headers: Headers = initHeaders(), decompress = false): Request =
  var cfg = NaviConfigBase(decompress: decompress)
  buildRequest(cfg, GET, "http://x.test/", headers)

suite "default identity headers":
  test "a default User-Agent should be added when the caller sets none":
    check built().headers.get("user-agent") == "navi/" & naviVersion

  test "a caller User-Agent should not be overridden":
    let h = initHeaders({"user-agent": "mine/1.0"})
    check built(h).headers.get("user-agent") == "mine/1.0"

  test "a default Accept of */* should be added when the caller sets none":
    check built().headers.get("accept") == "*/*"

  test "a caller Accept should not be overridden (e.g. SSE)":
    let h = initHeaders({"accept": "text/event-stream"})
    check built(h).headers.get("accept") == "text/event-stream"

  test "User-Agent and Accept should each appear exactly once":
    let r = built()
    check r.headers.getAll("user-agent").len == 1
    check r.headers.getAll("accept").len == 1

  test "Accept-Encoding should be added only when decompression is on":
    check not built(decompress = false).headers.contains("accept-encoding")
    check "zstd" in built(decompress = true).headers.get("accept-encoding")
