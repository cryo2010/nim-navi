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

proc port*(u: Url): int =
  ## Explicit port, or the scheme default (443 for https, else 80).
  if u.raw.port.len > 0:
    return parseInt(u.raw.port)
  if u.isTls: 443 else: 80

proc originKey*(u: Url): string =
  ## Pool key identifying a reusable connection: scheme, host, and port.
  (if u.isTls: "https" else: "http") & "://" & u.host & ":" & $u.port

proc path*(u: Url): string =
  if u.raw.path.len == 0: "/" else: u.raw.path

proc requestTarget*(u: Url): string =
  ## The origin-form target sent on the request line: path plus query.
  result = if u.raw.path.len == 0: "/" else: u.raw.path
  if u.raw.query.len > 0:
    result.add('?')
    result.add(u.raw.query)

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
