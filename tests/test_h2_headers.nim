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

  test "a buffered body should keep the caller's Content-Length":
    var req = post()
    req.body = "hello"
    req.headers["Content-Length"] = "5"
    check h2HeaderList(req).value("content-length") == "5"
