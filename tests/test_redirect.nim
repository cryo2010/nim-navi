## Redirect request rewriting: method changes and credential stripping.
import unittest
import navi/core/[headers, url, request, redirect, sinkgate]

proc req(verb: HttpVerb, target: string): Request =
  var h = initHeaders()
  h.add("authorization", "Bearer secret")
  h.add("proxy-authorization", "Basic proxy")
  h.add("cookie", "sid=1")
  Request(verb: verb, url: parseUrl(target), headers: h, body: "payload")

suite "redirect credential stripping":
  test "Authorization and Proxy-Authorization should be stripped on origin change":
    let r = redirectRequest(req(GET, "https://a.test/x"), 302, "https://b.test/y")
    check not r.headers.contains("authorization")
    check not r.headers.contains("proxy-authorization")

  test "Authorization and Proxy-Authorization should be kept on same-origin redirect":
    let r = redirectRequest(req(GET, "https://a.test/x"), 302, "https://a.test/y")
    check r.headers.get("authorization") == "Bearer secret"
    check r.headers.get("proxy-authorization") == "Basic proxy"

  test "the Cookie header should always be dropped (recomputed from the jar)":
    let r = redirectRequest(req(GET, "https://a.test/x"), 302, "https://a.test/y")
    check not r.headers.contains("cookie")

  test "a different port should count as a cross-origin change":
    let r = redirectRequest(req(GET, "https://a.test/x"), 307, "https://a.test:8443/y")
    check not r.headers.contains("authorization")
    check not r.headers.contains("proxy-authorization")

suite "redirect method rewriting":
  test "303 should switch any method to GET and drop the body":
    let r = redirectRequest(req(POST, "https://a.test/x"), 303, "https://a.test/y")
    check r.verb == GET
    check r.body == ""

  test "302 on a POST should degrade to GET":
    let r = redirectRequest(req(POST, "https://a.test/x"), 302, "https://a.test/y")
    check r.verb == GET
    check r.body == ""

  test "307 should preserve the method and body":
    let r = redirectRequest(req(POST, "https://a.test/x"), 307, "https://a.test/y")
    check r.verb == POST
    check r.body == "payload"

suite "streamed bodies across a redirect (#295)":
  # `preservesBody` is the guard `followRedirects` applies before rewriting: a
  # non-replayable (streamed) body may only follow a hop that drops the body, since
  # its producer was drained by the first attempt and cannot rewind.
  test "307/308 preserve the body for every method":
    for verb in [GET, HEAD, POST, PUT, PATCH]:
      check preservesBody(307, verb)
      check preservesBody(308, verb)

  test "303 always drops the body, so the hop is safe to follow":
    for verb in [GET, HEAD, POST, PUT, PATCH]:
      check not preservesBody(303, verb)

  test "301/302 preserve the body of a GET/HEAD, which is never rewritten":
    check preservesBody(301, GET)
    check preservesBody(302, GET)
    check preservesBody(301, HEAD)
    check preservesBody(302, HEAD)

  test "301/302 drop the body of a method that degrades to GET":
    check not preservesBody(301, POST)
    check not preservesBody(302, POST)
    check not preservesBody(302, PUT)
    check not preservesBody(302, PATCH)

  test "a 301/302 on a GET keeps the stream, which is why it must not be followed":
    # The rewrite itself is a no-op for GET: bodyStream survives it, so following the
    # hop would re-pull a spent producer. followRedirects breaks on this combination.
    var r = req(GET, "https://a.test/x")
    r.bodyStream = proc(): string = ""
    r.hasStreamedBody = true
    let hop = redirectRequest(r, 302, "https://a.test/y")
    check hop.verb == GET
    check hop.bodyStream != nil          # unchanged: the body would be replayed
    check preservesBody(302, r.verb)     # so the caller must surface the 302

  test "a 302 on a POST rewrites to a bodyless GET and clears the stream":
    var r = req(POST, "https://a.test/x")
    r.bodyStream = proc(): string = ""
    r.hasStreamedBody = true
    let hop = redirectRequest(r, 302, "https://a.test/y")
    check hop.verb == GET
    check hop.bodyStream == nil
    check not hop.hasStreamedBody
    check not preservesBody(302, r.verb)

suite "a body-dropping hop sends no body at all (#395)":
  # `preservesBody` is false exactly when the rewrite drops the body, and that is the
  # signal `followRedirects` uses to stop threading the async producer into the next
  # hop. Nothing may survive the rewrite that would frame a body on the wire: a
  # leftover producer would make the rewritten GET a `Transfer-Encoding: chunked`
  # request with a lone `0\r\n\r\n` body.
  test "a 303 leaves nothing on the hop that would frame a body":
    for verb in [POST, PUT, PATCH, GET]:
      var r = req(verb, "https://a.test/x")
      r.trailers.add("x-checksum", "abc")
      r.bodyStream = proc(): string = ""
      r.hasStreamedBody = true
      let hop = redirectRequest(r, 303, "https://a.test/y")
      check not preservesBody(303, verb)   # so the producer is dropped too
      check hop.verb == GET
      check hop.body == ""
      check hop.bodyStream == nil
      check not hop.hasStreamedBody
      check hop.trailers.len == 0

  test "a 301/302 off a non-GET/HEAD method leaves nothing that would frame a body":
    for status in [301, 302]:
      var r = req(POST, "https://a.test/x")
      r.trailers.add("x-checksum", "abc")
      r.bodyStream = proc(): string = ""
      r.hasStreamedBody = true
      let hop = redirectRequest(r, status, "https://a.test/y")
      check not preservesBody(status, POST)
      check hop.verb == GET
      check hop.body == ""
      check hop.bodyStream == nil
      check not hop.hasStreamedBody
      check hop.trailers.len == 0

  test "a hop that keeps the body keeps its framing, so it is never auto-followed":
    # The mirror image: 307 preserves everything, which is why a non-replayable body
    # makes `followRedirects` surface the 3xx instead of re-sending it (#295).
    var r = req(POST, "https://a.test/x")
    r.bodyStream = proc(): string = ""
    r.hasStreamedBody = true
    let hop = redirectRequest(r, 307, "https://a.test/y")
    check preservesBody(307, POST)
    check hop.verb == POST
    check hop.bodyStream != nil
    check hop.hasStreamedBody

suite "sink gate redirect delivery":
  # The gate must mirror followRedirects exactly: a hop is surfaced (so its body IS
  # delivered to the caller's sink) only when the hop would carry a non-replayable
  # body forward, which is `preservesBody` -- not 307/308 alone.
  proc gateFor(verb: HttpVerb, replayable: bool): SinkGate =
    result = newSinkGate()
    result.hops = 0
    result.redirectLimit = 5
    result.hopReplayable = replayable
    result.hopVerb = verb
    result.http = {H1, H2}

  proc redirectHeaders(): Headers =
    result = initHeaders()
    result.add("location", "https://b.test/y")

  test "a non-replayable GET taking a 301 is surfaced, so its body is delivered":
    # followRedirects breaks here (301 keeps a GET's body), so the 301 IS the final
    # response and its body belongs to the sink.
    let g = gateFor(GET, replayable = false)
    check g.wantsDelivery("HTTP/1.1", 301, redirectHeaders())
    check g.wantsDelivery("HTTP/1.1", 302, redirectHeaders())

  test "a non-replayable POST taking a 301 is followed, so nothing is delivered":
    # 301 rewrites the POST to a bodyless GET: no stream to replay, the hop is
    # followed, and the redirect body must never reach the sink.
    let g = gateFor(POST, replayable = false)
    check not g.wantsDelivery("HTTP/1.1", 301, redirectHeaders())
    check not g.wantsDelivery("HTTP/1.1", 302, redirectHeaders())
    check not g.wantsDelivery("HTTP/1.1", 303, redirectHeaders())

  test "a non-replayable POST taking a 307 is surfaced, so its body is delivered":
    let g = gateFor(POST, replayable = false)
    check g.wantsDelivery("HTTP/1.1", 307, redirectHeaders())
    check g.wantsDelivery("HTTP/1.1", 308, redirectHeaders())

  test "a replayable GET taking a 301 is followed, so nothing is delivered":
    let g = gateFor(GET, replayable = true)
    check not g.wantsDelivery("HTTP/1.1", 301, redirectHeaders())
    check not g.wantsDelivery("HTTP/1.1", 307, redirectHeaders())

  test "a redirect past the limit is surfaced whatever the hop looks like":
    let g = gateFor(POST, replayable = true)
    g.hops = 5                     # the limit is spent: nothing more is followed
    check g.wantsDelivery("HTTP/1.1", 302, redirectHeaders())

  test "a 3xx without a Location is not a followable redirect":
    let g = gateFor(GET, replayable = true)
    check g.wantsDelivery("HTTP/1.1", 302, initHeaders())
