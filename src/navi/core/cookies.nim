## A minimal per-client cookie jar.
##
## Stores cookies from Set-Cookie response headers and replays matching ones on
## later requests to the same client. Matching covers domain, path, and the
## Secure attribute. Expiry honors Max-Age and Expires (Max-Age takes
## precedence, per RFC 6265): an already-expired cookie deletes any match, and
## live cookies are pruned once their expiry passes. Cookies with no expiry are
## session cookies (kept for the client's lifetime).

import std/[strutils, sequtils, times, options]
import ./headers, ./url, ./request, ./response

type
  Cookie = object
    name, value, domain, path: string
    secure: bool
    expires: Option[Time]   ## absolute expiry; none means a session cookie
  CookieJar* = ref object
    cookies: seq[Cookie]

proc newCookieJar*(): CookieJar = CookieJar()

proc parseHttpDate(s: string): Option[Time] =
  ## Parse an Expires value. Accepts the RFC 1123 form Set-Cookie uses in
  ## practice ("Wed, 09 Jun 2021 10:18:14 GMT"); anything else is ignored.
  try:
    some(parse(s.strip, "ddd, dd MMM yyyy HH:mm:ss 'GMT'", utc()).toTime)
  except TimeParseError, ValueError:
    none(Time)

proc parseSetCookie(line, host: string): (Cookie, bool) =
  ## Parse one Set-Cookie value. Returns (cookie, delete?) where delete is set
  ## when the cookie is already expired (Max-Age <= 0 or Expires in the past).
  var c = Cookie(path: "/", domain: host)
  var maxAge = none(int)
  var expiresAt = none(Time)
  var i = 0
  for part in line.split(';'):
    let p = part.strip
    let eq = p.find('=')
    let key = (if eq < 0: p else: p[0 ..< eq])
    let val = (if eq < 0: "" else: p[eq + 1 .. ^1])
    if i == 0:
      c.name = key
      c.value = val
    else:
      case key.toLowerAscii
      of "domain": (if val.len > 0: c.domain = val.strip(chars = {'.'}).toLowerAscii)
      of "path": (if val.len > 0: c.path = val)
      of "secure": c.secure = true
      of "max-age":
        try: maxAge = some(parseInt(val.strip))
        except ValueError: discard
      of "expires":
        expiresAt = parseHttpDate(val)
      else: discard
    inc i
  # Max-Age wins over Expires when both are present.
  let now = getTime()
  var expired = false
  if maxAge.isSome:
    if maxAge.get <= 0: expired = true
    else: c.expires = some(now + initDuration(seconds = maxAge.get))
  elif expiresAt.isSome:
    if expiresAt.get <= now: expired = true
    else: c.expires = expiresAt
  (c, expired)

proc domainMatches(host, domain: string): bool =
  let h = host.toLowerAscii
  h == domain or h.endsWith("." & domain)

proc live(c: Cookie, now: Time): bool =
  c.expires.isNone or c.expires.get > now

proc storeCookies*(jar: CookieJar, url: Url, resp: Response) =
  for (name, value) in resp.headers.pairs:
    if cmpIgnoreCase(name, "set-cookie") != 0: continue
    let (c, expired) = parseSetCookie(value, url.host.toLowerAscii)
    jar.cookies.keepItIf(not (it.name == c.name and it.domain == c.domain and
                              it.path == c.path))
    if not expired:
      jar.cookies.add c

proc applyCookies*(jar: CookieJar, req: var Request) =
  if req.headers.contains("cookie"): return # caller set cookies explicitly
  let now = getTime()
  jar.cookies.keepItIf(it.live(now))   # drop cookies whose expiry has passed
  var pairs: seq[string]
  for c in jar.cookies:
    if domainMatches(req.url.host, c.domain) and
       req.url.path.startsWith(c.path) and
       (not c.secure or req.url.isTls):
      pairs.add(c.name & "=" & c.value)
  if pairs.len > 0:
    req.headers.add("cookie", pairs.join("; "))
