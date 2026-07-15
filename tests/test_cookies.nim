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
  test "a positive Max-Age cookie is stored and replayed":
    let jar = newCookieJar()
    storeCookies(jar, parseUrl("http://x.test/"), setCookieResp("a=1; Max-Age=3600"))
    check jar.replayed("http://x.test/") == "a=1"

  test "Max-Age=0 is not stored":
    let jar = newCookieJar()
    storeCookies(jar, parseUrl("http://x.test/"), setCookieResp("a=1; Max-Age=0"))
    check jar.replayed("http://x.test/") == ""

  test "an Expires in the past is not stored":
    let jar = newCookieJar()
    storeCookies(jar, parseUrl("http://x.test/"),
                 setCookieResp("a=1; Expires=Wed, 09 Jun 2021 10:18:14 GMT"))
    check jar.replayed("http://x.test/") == ""

  test "an Expires in the future is kept":
    let jar = newCookieJar()
    storeCookies(jar, parseUrl("http://x.test/"),
                 setCookieResp("a=1; Expires=Tue, 19 Jan 2038 03:14:07 GMT"))
    check jar.replayed("http://x.test/") == "a=1"

  test "Max-Age takes precedence over a past Expires":
    let jar = newCookieJar()
    storeCookies(jar, parseUrl("http://x.test/"),
      setCookieResp("a=1; Expires=Wed, 09 Jun 2021 10:18:14 GMT; Max-Age=3600"))
    check jar.replayed("http://x.test/") == "a=1"

  test "a session cookie (no expiry) is replayed":
    let jar = newCookieJar()
    storeCookies(jar, parseUrl("http://x.test/"), setCookieResp("a=1"))
    check jar.replayed("http://x.test/") == "a=1"

proc stored(setCookie, fromUrl: string): CookieJar =
  result = newCookieJar()
  storeCookies(result, parseUrl(fromUrl), setCookieResp(setCookie))

suite "cookie domain and path matching (RFC 6265)":
  test "a host-only cookie is not sent to a subdomain":
    let jar = stored("a=1", "http://x.test/")
    check jar.replayed("http://x.test/") == "a=1"
    check jar.replayed("http://sub.x.test/") == ""

  test "a Domain cookie is sent to subdomains":
    let jar = stored("a=1; Domain=x.test", "http://x.test/")
    check jar.replayed("http://x.test/") == "a=1"
    check jar.replayed("http://sub.x.test/") == "a=1"

  test "a Set-Cookie for an unrelated domain is rejected":
    let jar = stored("a=1; Domain=evil.test", "http://x.test/")
    check jar.replayed("http://x.test/") == ""
    check jar.replayed("http://evil.test/") == ""

  test "path-match respects '/' boundaries":
    let jar = stored("a=1; Path=/foo", "http://x.test/foo")
    check jar.replayed("http://x.test/foo") == "a=1"
    check jar.replayed("http://x.test/foo/bar") == "a=1"
    check jar.replayed("http://x.test/foobar") == ""   # not a boundary match
    check jar.replayed("http://x.test/") == ""

  test "default-path is derived from the request path":
    let jar = stored("a=1", "http://x.test/dir/page")
    check jar.replayed("http://x.test/dir/other") == "a=1"  # default-path is /dir
    check jar.replayed("http://x.test/") == ""

  test "an asctime Expires date is parsed":
    # 1994 is in the past, so the cookie is dropped (proves the date parsed;
    # an unparseable date would be kept as a session cookie).
    let jar = stored("a=1; Expires=Sun Nov  6 08:49:37 1994", "http://x.test/")
    check jar.replayed("http://x.test/") == ""
