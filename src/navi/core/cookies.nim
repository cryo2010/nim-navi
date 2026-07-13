## A minimal per-client cookie jar.
##
## Stores cookies from Set-Cookie response headers and replays matching ones on
## later requests to the same client. Matching covers domain, path, and the
## Secure attribute; full expiry handling beyond immediate deletion (Max-Age
## <= 0) is left for later.

import std/[strutils, sequtils]
import ./headers, ./url, ./request, ./response

type
  Cookie = object
    name, value, domain, path: string
    secure: bool
  CookieJar* = ref object
    cookies: seq[Cookie]

proc newCookieJar*(): CookieJar = CookieJar()

proc parseSetCookie(line, host: string): (Cookie, bool) =
  ## Parse one Set-Cookie value. Returns (cookie, delete?) where delete is set
  ## for an immediate expiry (Max-Age <= 0).
  var c = Cookie(path: "/", domain: host)
  var expired = false
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
        try: (if parseInt(val) <= 0: expired = true)
        except ValueError: discard
      else: discard
    inc i
  (c, expired)

proc domainMatches(host, domain: string): bool =
  let h = host.toLowerAscii
  h == domain or h.endsWith("." & domain)

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
  var pairs: seq[string]
  for c in jar.cookies:
    if domainMatches(req.url.host, c.domain) and
       req.url.path.startsWith(c.path) and
       (not c.secure or req.url.isTls):
      pairs.add(c.name & "=" & c.value)
  if pairs.len > 0:
    req.headers.add("cookie", pairs.join("; "))
