## URL handling built on std/uri, plus prefix joining and a query builder.

import std/[uri, strutils, sequtils]

type
  Url* = object
    raw*: Uri

proc parseUrl*(s: string): Url =
  result.raw = parseUri(s)

proc `$`*(u: Url): string = $u.raw

proc scheme*(u: Url): string = u.raw.scheme
proc host*(u: Url): string = u.raw.hostname
proc isTls*(u: Url): bool = cmpIgnoreCase(u.raw.scheme, "https") == 0

proc hostLiteral*(u: Url): string =
  ## The host as it must appear in a Host header or request authority. std/uri
  ## strips the brackets from an IPv6 literal (`[2001:db8::1]` -> `2001:db8::1`),
  ## but an address with colons is ambiguous unbracketed (RFC 3986 3.2.2), so
  ## re-wrap it. A regular hostname or IPv4 literal passes through unchanged.
  let h = u.host
  if ':' in h and not h.startsWith("["): "[" & h & "]" else: h

proc port*(u: Url): int =
  ## Explicit port, or the scheme default (443 for https, else 80). A malformed or
  ## out-of-range port (e.g. from a crafted redirect Location) raises a clear
  ## `ValueError` rather than the cryptic overflow/parse error `parseInt` would.
  if u.raw.port.len > 0:
    var p: int
    try:
      p = parseInt(u.raw.port)
    except ValueError:
      raise newException(ValueError, "navi: invalid URL port '" & u.raw.port & "'")
    if p < 1 or p > 65535:
      raise newException(ValueError, "navi: URL port out of range '" & u.raw.port & "'")
    return p
  if u.isTls: 443 else: 80

const dialableSchemes* = ["http", "https", "ws", "wss"]
  ## The schemes navi dials. A target with any other scheme (or none, e.g. a
  ## relative path resolved against an empty `prefixUrl`) is left to whatever
  ## consumes it, so `requireHost` does not judge it.

proc requireHost*(u: Url) =
  ## Reject a URL that names one of the schemes navi dials but carries no
  ## authority, e.g. `https:///path`, which `std/uri` happily parses to hostname
  ## "". Such a URL is not dialable, yet it used to travel all the way to the
  ## transport: over a configured `unixSocket` (which never resolves the host) or,
  ## on a platform whose `getaddrinfo("")` resolves to loopback (macOS), over TCP
  ## to 127.0.0.1. With TLS that was the dangerous case, because an empty host
  ## meant no SNI and no certificate identity check, so any chain-valid
  ## certificate was accepted (#435).
  ##
  ## Called by `buildRequest` and by the WebSocket openers, i.e. once per request
  ## on every client including `navi/js` and the HTTP/3 leg (which is only ever
  ## reached through a built request). Raises `ValueError`: a URL with no host is
  ## a caller mistake, in the same class as the out-of-range port `port` rejects.
  if u.host.len > 0: return
  let s = u.raw.scheme.toLowerAscii
  for known in dialableSchemes:
    if s == known:
      raise newException(ValueError,
        "navi: URL has no host: '" & $u & "'")

proc parseWsUrl*(url: string): Url =
  ## Parse a WebSocket target into the URL its transport dials: `ws://` becomes
  ## `http://` and `wss://` becomes `https://`, so one scheme pair drives the pool
  ## key, `isTls`, the default port and the TLS identity. Anything else (already
  ## `http`/`https`, or a scheme navi does not dial) is parsed as given.
  ##
  ## The scheme is case-insensitive (RFC 3986 3.1), so the prefix is matched on a
  ## lowercased copy. Matching it case-sensitively was a silent TLS downgrade:
  ## `WSS://host/chat` kept its scheme, `isTls` (which compares against `https`)
  ## then read false and the default port became 80, so a caller that asked for a
  ## secure WebSocket got a cleartext one.
  ##
  ## A target with no authority (`wss:///chat`) is rejected here for the reason
  ## `requireHost` gives: it is not dialable and carries no identity (#435). Shared
  ## by the sync and async WebSocket openers; `navi/js` hands the URL to the
  ## runtime's own `WebSocket` and only applies `requireHost`.
  let lowered = url.toLowerAscii
  var s = url
  if lowered.startsWith("ws://"): s = "http://" & s["ws://".len .. ^1]
  elif lowered.startsWith("wss://"): s = "https://" & s["wss://".len .. ^1]
  result = parseUrl(s)
  result.requireHost()

proc originKey*(scheme, host: string, port: int): string =
  ## Canonical origin key `scheme://host:port`. Scheme and host are lowercased
  ## (both are case-insensitive per RFC 3986 3.2.2), so differently-cased URLs for
  ## the same origin share one connection pool and one Alt-Svc cache entry. Single
  ## source of truth for pool keys and the Alt-Svc cache.
  scheme.toLowerAscii & "://" & host.toLowerAscii & ":" & $port

proc originKey*(u: Url): string =
  ## Pool key identifying a reusable connection: scheme, host, and port.
  originKey((if u.isTls: "https" else: "http"), u.host, u.port)

proc path*(u: Url): string =
  if u.raw.path.len == 0: "/" else: u.raw.path

proc requestTarget*(u: Url): string =
  ## The origin-form target sent on the request line: path plus query.
  result = if u.raw.path.len == 0: "/" else: u.raw.path
  if u.raw.query.len > 0:
    result.add('?')
    result.add(u.raw.query)

proc absoluteTarget*(u: Url): string =
  ## The absolute-form target sent to an HTTP proxy: scheme://host[:port]/path.
  let scheme = if u.isTls: "https" else: "http"
  var authority = u.hostLiteral
  let p = u.port
  if not ((u.isTls and p == 443) or (not u.isTls and p == 80)):
    authority.add(":" & $p)
  scheme & "://" & authority & u.requestTarget

proc join*(prefix: string, target: string): Url =
  ## Resolve `target` against `prefix` (ky's prefixUrl semantics). An absolute
  ## `target` (has a scheme) wins outright; otherwise it is appended to prefix.
  if target.len == 0:
    return parseUrl(prefix)
  let t = parseUri(target)
  if t.scheme.len > 0 or prefix.len == 0:
    return parseUrl(target)
  var base = prefix
  if not base.endsWith('/'): base.add('/')
  return parseUrl(base & target.strip(leading = true, trailing = false, chars = {'/'}))

proc resolve*(base: Url, location: string): Url =
  ## Resolve a redirect target (absolute or relative) against `base`, per
  ## RFC 3986. An absolute `location` replaces the base outright.
  Url(raw: combine(base.raw, parseUri(location)))

proc withQuery*(u: Url, params: openArray[(string, string)]): Url =
  ## Return a copy of `u` with `params` appended to the query string.
  result = u
  let extra = params.mapIt(encodeUrl(it[0]) & "=" & encodeUrl(it[1])).join("&")
  if extra.len == 0: return
  result.raw.query =
    if u.raw.query.len == 0: extra else: u.raw.query & "&" & extra
