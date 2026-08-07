## Cookie jar expiry unit tests (Max-Age and Expires).
import unittest
import navi/core/[headers, url, request, response, cookies]

proc setCookieResp(setCookie: string): Response =
  var h = initHeaders()
  h.add("set-cookie", setCookie)
  initResponse(200, "OK", "HTTP/1.1", h, "")

proc replayed(jar: CookieJar, target: string): string =
  var req = Request(url: parseUrl(target), headers: initHeaders())
  applyCookies(jar, req)
  req.headers.get("cookie")

suite "cookie expiry":
  test "a cookie should be stored and replayed when Max-Age is positive":
    let jar = newCookieJar()
    storeCookies(jar, parseUrl("http://x.test/"), setCookieResp("a=1; Max-Age=3600"))
    check jar.replayed("http://x.test/") == "a=1"

  test "a cookie should not be stored when Max-Age is 0":
    let jar = newCookieJar()
    storeCookies(jar, parseUrl("http://x.test/"), setCookieResp("a=1; Max-Age=0"))
    check jar.replayed("http://x.test/") == ""

  test "a cookie should not be stored when Expires is in the past":
    let jar = newCookieJar()
    storeCookies(jar, parseUrl("http://x.test/"),
                 setCookieResp("a=1; Expires=Wed, 09 Jun 2021 10:18:14 GMT"))
    check jar.replayed("http://x.test/") == ""

  test "a cookie should be kept when Expires is in the future":
    let jar = newCookieJar()
    storeCookies(jar, parseUrl("http://x.test/"),
                 setCookieResp("a=1; Expires=Tue, 19 Jan 2038 03:14:07 GMT"))
    check jar.replayed("http://x.test/") == "a=1"

  test "a cookie should honor Max-Age over a past Expires":
    let jar = newCookieJar()
    storeCookies(jar, parseUrl("http://x.test/"),
      setCookieResp("a=1; Expires=Wed, 09 Jun 2021 10:18:14 GMT; Max-Age=3600"))
    check jar.replayed("http://x.test/") == "a=1"

  test "a session cookie should be replayed when it has no expiry":
    let jar = newCookieJar()
    storeCookies(jar, parseUrl("http://x.test/"), setCookieResp("a=1"))
    check jar.replayed("http://x.test/") == "a=1"

proc stored(setCookie, fromUrl: string): CookieJar =
  result = newCookieJar()
  storeCookies(result, parseUrl(fromUrl), setCookieResp(setCookie))

suite "cookie domain and path matching (RFC 6265)":
  test "a host-only cookie should not be sent to a subdomain (RFC 6265)":
    let jar = stored("a=1", "http://x.test/")
    check jar.replayed("http://x.test/") == "a=1"
    check jar.replayed("http://sub.x.test/") == ""

  test "a Domain cookie should be sent to subdomains (RFC 6265)":
    let jar = stored("a=1; Domain=x.test", "http://x.test/")
    check jar.replayed("http://x.test/") == "a=1"
    check jar.replayed("http://sub.x.test/") == "a=1"

  test "a Set-Cookie should be rejected when its Domain is unrelated (RFC 6265)":
    let jar = stored("a=1; Domain=evil.test", "http://x.test/")
    check jar.replayed("http://x.test/") == ""
    check jar.replayed("http://evil.test/") == ""

  test "path-matching should respect '/' boundaries (RFC 6265)":
    let jar = stored("a=1; Path=/foo", "http://x.test/foo")
    check jar.replayed("http://x.test/foo") == "a=1"
    check jar.replayed("http://x.test/foo/bar") == "a=1"
    check jar.replayed("http://x.test/foobar") == ""   # not a boundary match
    check jar.replayed("http://x.test/") == ""

  test "the default-path should be derived from the request path (RFC 6265)":
    let jar = stored("a=1", "http://x.test/dir/page")
    check jar.replayed("http://x.test/dir/other") == "a=1"  # default-path is /dir
    check jar.replayed("http://x.test/") == ""

  test "the cookie jar should parse an asctime Expires date (RFC 6265)":
    # 1994 is in the past, so the cookie is dropped (proves the date parsed;
    # an unparseable date would be kept as a session cookie).
    let jar = stored("a=1; Expires=Sun Nov  6 08:49:37 1994", "http://x.test/")
    check jar.replayed("http://x.test/") == ""
