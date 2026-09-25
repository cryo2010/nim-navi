## HTTP/2 request header mapping (core/h2glue): which caller headers cross to h2.

import unittest
import navi/core/[h2glue, headers, request, url]
import navi/proto/h2/hpack

proc value(list: seq[HeaderPair], name: string): string =
  for (k, v) in list:
    if k == name: return v
  ""

proc has(list: seq[HeaderPair], name: string): bool =
  for (k, _) in list:
    if k == name: return true
  false

proc post(): Request =
  result = Request(verb: POST, url: parseUrl("https://example.com/upload"))
  result.headers = initHeaders()

suite "h2 request header mapping":
  test "pseudo-headers should come first and Host should become :authority":
    var req = post()
    req.headers["Host"] = "ignored.test"
    req.headers["X-Trace"] = "abc"
    let list = h2HeaderList(req)
    check list[0][0] == ":method"
    check list.value(":authority") == "example.com"
    check not list.has("host")
    check list.value("x-trace") == "abc"

  test "a caller-supplied Content-Length should be dropped on a streamed upload (#294)":
    # The DATA frames are produced chunk by chunk, so the caller's length cannot be
    # trusted; forwarding it makes the message malformed when it disagrees with the
    # bytes sent (RFC 9113 8.1.2.6). h3 and h1 strip it on their streamed paths too.
    var req = post()
    req.headers["Content-Length"] = "5"
    req.bodyStream = proc(): string = ""
    req.hasStreamedBody = true
    check not h2HeaderList(req).has("content-length")

  test "an async producer (hasStreamedBody, no bodyStream) should also drop it (#294)":
    var req = post()
    req.headers["Content-Length"] = "5"
    req.hasStreamedBody = true
    check not h2HeaderList(req).has("content-length")

  test "an expect-gated request must never carry Expect over h2 (#392)":
    # `expectContinueMs` is an HTTP/1.1 knob: the header is added to a local copy of
    # the request inside the h1 send path, so `req` itself never carries it and the
    # h2 (and h3) field lists, built from `req`, cannot pick it up. h2 has flow
    # control, which makes the expectation pointless there.
    var req = post()
    req.expectContinueMs = 5000
    req.bodyStream = proc(): string = ""
    req.hasStreamedBody = true
    check not h2HeaderList(req).has("expect")

  test "a buffered body should keep the caller's Content-Length":
    var req = post()
    req.body = "hello"
    req.headers["Content-Length"] = "5"
    check h2HeaderList(req).value("content-length") == "5"
