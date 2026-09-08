## A per-client cookie jar implementing the RFC 6265 subset a client needs.
##
## Stores cookies from Set-Cookie and replays matching ones. Honors Max-Age and
## Expires (Max-Age wins), host-only vs Domain matching, path matching with
## default-path derivation, and the Secure attribute; it rejects a Set-Cookie
## whose Domain the request host is not within. Replayed cookies are ordered per
## RFC 6265 5.4 (longer paths first). The jar is in-memory and
## per-client -- persistence across process restarts is a separate feature,
## intentionally not implemented here.

import std/[strutils, sequtils, algorithm, times, options]
import ./headers, ./url, ./request, ./response

type
  Cookie = object
    name, value, domain, path: string
    secure: bool
    hostOnly: bool           ## no Domain attribute: match the exact host only
    expires: Option[Time]    ## absolute expiry; none means a session cookie
  SetCookieVerdict = enum
    ## What to do with a parsed Set-Cookie. `scStore` replaces/adds the cookie;
    ## `scDelete` evicts a matching stored cookie (Max-Age<=0 / past Expires);
    ## `scReject` leaves the store untouched (RFC 6265 5.3: a Domain the request
    ## host is not within, or a `__Host-`/`__Secure-` prefix violation).
    scStore, scDelete, scReject
  CookieJar* = ref object
    cookies: seq[Cookie]

proc newCookieJar*(): CookieJar = CookieJar()

const dateFormats = [
  "ddd, dd MMM yyyy HH:mm:ss 'GMT'",   # RFC 1123 (the common form)
  "ddd, dd-MMM-yyyy HH:mm:ss 'GMT'",   # RFC 1123 with dashes
  "dddd, dd-MMM-yy HH:mm:ss 'GMT'",    # RFC 850
  "ddd MMM d HH:mm:ss yyyy",           # asctime
]

proc parseHttpDate(s: string): Option[Time] =
  ## Parse an Expires value, trying the formats seen in Set-Cookie in practice.
  let t = s.strip.replace("  ", " ")   # asctime pads single-digit days with two
  for fmt in dateFormats:
    try: return some(parse(t, fmt, utc()).toTime)
    except CatchableError: discard
  none(Time)

proc domainMatches(host, domain: string): bool =
  ## RFC 6265 5.1.3: host equals domain, or is a subdomain of it.
  let h = host.toLowerAscii
  h == domain or h.endsWith("." & domain)

proc defaultPath(reqPath: string): string =
  ## RFC 6265 5.1.4: the request path up to (excluding) its last '/'.
  if not reqPath.startsWith("/"): return "/"
  let i = reqPath.rfind('/')
  if i <= 0: "/" else: reqPath[0 ..< i]

proc pathMatches(reqPath, cookiePath: string): bool =
  ## RFC 6265 5.1.4 path-match (respects '/' boundaries).
  if reqPath == cookiePath: return true
  if reqPath.startsWith(cookiePath):
    return cookiePath.endsWith("/") or
           (reqPath.len > cookiePath.len and reqPath[cookiePath.len] == '/')
  false

proc prefixAllows(c: Cookie, secureContext: bool): bool =
  ## RFC 6265bis 5.5 cookie name prefixes (case-insensitive). `__Secure-` demands
  ## the Secure attribute over a secure origin; `__Host-` additionally demands a
  ## host-only cookie (no Domain) scoped to Path=/. A cookie that claims a prefix
  ## it doesn't satisfy is rejected rather than silently downgraded.
  let name = c.name.toLowerAscii
  if name.startsWith("__host-"):
    return secureContext and c.secure and c.hostOnly and c.path == "/"
  if name.startsWith("__secure-"):
    return secureContext and c.secure
  true

proc parseSetCookie(line, host, reqPath: string, secureContext: bool): (Cookie, SetCookieVerdict) =
  ## Parse one Set-Cookie. Returns (cookie, verdict): `scReject` when the cookie
  ## must be wholly ignored (its Domain attribute is not one the request host is
  ## within, or a `__Host-`/`__Secure-` prefix violation), `scDelete` when it is
  ## already expired (so a matching stored cookie is evicted), else `scStore`.
  var c = Cookie(hostOnly: true, domain: host.toLowerAscii,
                 path: defaultPath(reqPath))
  var maxAge = none(int)
  var expiresAt = none(Time)
  var i = 0
  for part in line.split(';'):
    let p = part.strip
    let eq = p.find('=')
    let key = (if eq < 0: p else: p[0 ..< eq])
    let val = (if eq < 0: "" else: p[eq + 1 .. ^1])
    if i == 0:
      c.name = key.strip
      c.value = val.strip
    else:
      case key.toLowerAscii
      of "domain":
        let d = val.strip(chars = {'.'}).toLowerAscii
        if d.len > 0:
          # RFC 6265 5.3.5-6: a server may only scope a cookie to a domain the
          # request host is within; reject otherwise. (No public-suffix check.)
          # Rejected, not deleted: the store must be left untouched.
          if not domainMatches(host, d): return (c, scReject)
          c.domain = d
          c.hostOnly = false
      of "path": (if val.startsWith("/"): c.path = val)
      of "secure": c.secure = true
      of "max-age":
        try: maxAge = some(parseInt(val.strip))
        except ValueError: discard
      of "expires": expiresAt = parseHttpDate(val)
      else: discard
    inc i
  # Max-Age takes precedence over Expires when both are present.
  let now = getTime()
  var verdict = scStore
  if maxAge.isSome:
    if maxAge.get <= 0: verdict = scDelete
    else: c.expires = some(now + initDuration(seconds = maxAge.get))
  elif expiresAt.isSome:
    if expiresAt.get <= now: verdict = scDelete
    else: c.expires = expiresAt
  # A prefix violation is a rejection (ignore entirely), which takes precedence
  # over a delete: such a Set-Cookie must never touch the store.
  if not prefixAllows(c, secureContext): verdict = scReject
  (c, verdict)

proc live(c: Cookie, now: Time): bool =
  c.expires.isNone or c.expires.get > now

proc matchesHost(c: Cookie, host: string): bool =
  if c.hostOnly: host.toLowerAscii == c.domain
  else: domainMatches(host, c.domain)

proc storeCookies*(jar: CookieJar, url: Url, resp: Response) =
  for (name, value) in resp.headers.pairs:
    if cmpIgnoreCase(name, "set-cookie") != 0: continue
    let (c, verdict) = parseSetCookie(value, url.host, url.path, url.isTls)
    if c.name.len == 0: continue   # a nameless cookie is ignored
    if verdict == scReject: continue   # RFC 6265 5.3: ignore, leave the store as is
    # Evict any matching stored cookie for both the store (replace) and delete
    # (expired) cases; a rejection above never reaches here.
    jar.cookies.keepItIf(not (it.name == c.name and it.domain == c.domain and
                              it.path == c.path))
    if verdict == scStore:
      jar.cookies.add c

proc applyCookies*(jar: CookieJar, req: var Request) =
  if req.headers.contains("cookie"): return # caller set cookies explicitly
  let now = getTime()
  jar.cookies.keepItIf(it.live(now))   # drop cookies whose expiry has passed
  var matched: seq[Cookie]
  for c in jar.cookies:
    if c.matchesHost(req.url.host) and
       pathMatches(req.url.path, c.path) and
       (not c.secure or req.url.isTls):
      matched.add c
  # RFC 6265 5.4: longer paths (more specific) first; equal paths keep their
  # existing (creation) order. Nim's sort is stable, so the seq order already in
  # place is the creation-time tie-break the RFC calls for.
  matched.sort(proc (a, b: Cookie): int = cmp(b.path.len, a.path.len))
  var pairs: seq[string]
  for c in matched:
    pairs.add(c.name & "=" & c.value)
  if pairs.len > 0:
    req.headers.add("cookie", pairs.join("; "))
