## Redirect request rewriting: method changes and credential stripping.
import unittest
import navi/core/[headers, url, request, redirect]

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
