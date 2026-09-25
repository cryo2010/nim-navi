## buildRequest identity-header defaults (User-Agent, Accept, Accept-Encoding)
## and the type-dispatched request body (`toBody` arm selection + precedence).
import unittest, std/[strutils, json]
import navi/core/[headers, request, version, multipart]

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

# --- toBody arm selection ---------------------------------------------------

type Point = object
  x, y: int

type PointRef = ref object
  x, y: int

suite "toBody arm selection":
  test "a string body should be untyped raw content":
    let b = toBody("raw")
    check not b.typed
    check b.content == "raw"
    check b.contentType == ""
    check b.stream == nil

  test "a JsonNode body should be typed JSON":
    let b = toBody(%*{"a": 1})
    check b.typed
    check b.content == """{"a":1}"""
    check b.contentType == "application/json"

  test "a nil JsonNode should be a no-op (default ResolvedBody)":
    check toBody(JsonNode(nil)) == ResolvedBody()

  test "a Multipart body should be typed multipart/form-data":
    let b = toBody(@[field("k", "v")])
    check b.typed
    check b.contentType.startsWith("multipart/form-data; boundary=")
    check "k" in b.content

  test "an empty Multipart should be a no-op (default ResolvedBody)":
    check toBody(Multipart(@[])) == ResolvedBody()

  test "a BodyProducer body should be typed and streamed":
    let p: BodyProducer = proc(): string = ""
    let b = toBody(p)
    check b.typed
    check b.stream != nil
    check b.content == ""

  test "a nil BodyProducer should be a no-op (default ResolvedBody)":
    check toBody(BodyProducer(nil)) == ResolvedBody()

  test "a nil BodyIterator should be a no-op (default ResolvedBody)":
    check toBody(BodyIterator(nil)) == ResolvedBody()

  test "a bare nil body should be rejected at compile time":
    check not compiles(toBody(nil))

  test "a ResolvedBody should pass through unchanged":
    let r = ResolvedBody(typed: true, content: "x", contentType: "text/plain")
    check toBody(r) == r

  test "an object body should be catch-all JSON":
    let b = toBody(Point(x: 1, y: 2))
    check b.typed
    check b.content == """{"x":1,"y":2}"""
    check b.contentType == "application/json"

  test "a seq[int] body should be catch-all JSON":
    let b = toBody(@[1, 2, 3])
    check b.typed
    check b.content == "[1,2,3]"
    check b.contentType == "application/json"

  test "a ref object body should be catch-all JSON":
    let b = toBody(PointRef(x: 3, y: 4))
    check b.typed
    check b.content == """{"x":3,"y":4}"""
    check b.contentType == "application/json"

  test "a nil ref body should serialize to JSON null":
    check toBody(PointRef(nil)).content == "null"

suite "toBody through buildRequest":
  proc build(body: ResolvedBody, headers = initHeaders(),
             form: seq[(string, string)] = @[]): Request =
    var cfg = NaviConfigBase()
    buildRequest(cfg, POST, "http://x.test/", headers, body, form)

  test "a catch-all body should set content-type and body on the request":
    let r = build(toBody(Point(x: 1, y: 2)))
    check r.body == """{"x":1,"y":2}"""
    check r.headers.get("content-type") == "application/json"

  test "a caller Content-Type should not be clobbered by a typed body":
    let h = initHeaders({"content-type": "application/vnd.custom+json"})
    let r = build(toBody(@[1, 2, 3]), h)
    check r.body == "[1,2,3]"
    check r.headers.get("content-type") == "application/vnd.custom+json"

  test "a typed body should win over form":
    let r = build(toBody(%*{"a": 1}), form = @[("b", "2")])
    check r.body == """{"a":1}"""
    check r.headers.get("content-type") == "application/json"
    check r.bodyStream == nil

  test "form should win over a plain-string body":
    let r = build(toBody("raw"), form = @[("b", "2 words")])
    check r.body == "b=2+words"
    check r.headers.get("content-type") == "application/x-www-form-urlencoded"

  test "a streamed body should set bodyStream, not body":
    let p: BodyProducer = proc(): string = ""
    let r = build(toBody(p))
    check r.body == ""
    check r.bodyStream != nil

  test "expectContinueMs should be copied from the config onto the request (#392)":
    # The h1 send path reads it off the Request, so every body arm (buffered,
    # `bodyStream`, and the async producer threaded outside the request) sees the
    # same client-level setting.
    var cfg = NaviConfigBase()
    cfg.expectContinueMs = 1500
    check cfg.expectContinueMs == 1500      # the accessor reads the field
    let r = buildRequest(cfg, POST, "http://x.test/", body = toBody("hi"))
    check r.expectContinueMs == 1500
    check build(toBody("hi")).expectContinueMs == 0   # default config: gate off

  test "carriesBody should be true only for a request that puts content on the wire":
    # What gates the Expect header: RFC 9110 10.1.1 only defines the expectation for
    # a request with content, and a server told to expect one it never gets stalls.
    check not build(ResolvedBody()).carriesBody()
    check build(toBody("hi")).carriesBody()
    let p: BodyProducer = proc(): string = ""
    check build(toBody(p)).carriesBody()
    var asyncish = build(ResolvedBody())
    asyncish.hasStreamedBody = true          # an async producer, threaded outside req
    check asyncish.carriesBody()

suite "BodyIterator wrapping producer":
  test "the wrapper should skip empty mid-stream yields and end at finished":
    let parts = @["a", "", "b", "", "c"]
    let it = iterator (): string {.closure.} =
      for p in parts: yield p
    let stream = toBody(it).stream
    check stream != nil
    check stream() == "a"
    check stream() == "b"       # empty yield between a and b was skipped
    check stream() == "c"
    check stream() == ""        # finished
    check stream() == ""        # stays "" after finish
