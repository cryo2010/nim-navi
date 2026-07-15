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
