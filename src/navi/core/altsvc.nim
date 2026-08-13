## Alt-Svc (RFC 7838) discovery for HTTP/3.
##
## HTTP/3 has no ALPN-on-first-connect path: a client reaches h3 by first talking
## h1/h2 and noticing an `Alt-Svc: h3=...` advertisement (or, later, an HTTPS DNS
## record). This module parses that header and keeps a small per-client cache of
## "origin -> h3 endpoint" so subsequent requests can upgrade. Nothing here dials
## or negotiates; it is pure bookkeeping, testable without a network, and safe to
## compile in every build (the transport that consumes it is `-d:naviHttp3`-only).

import std/[tables, options, strutils, monotimes, times]

const defaultMaxAge = 86_400   ## RFC 7838: `ma` defaults to 24h when absent.

type
  AltSvcEndpoint* = object
    ## Where h3 is offered for an origin. An empty `host` means "same host as the
    ## origin" (the common `h3=":443"` form); `record` resolves it to the origin.
    host*: string
    port*: int

  AltSvc* = object
    ## The parsed result of one Alt-Svc header value.
    h3*: bool                  ## an `h3=` advertisement was present
    endpoint*: AltSvcEndpoint  ## the h3 endpoint (valid when `h3`)
    maxAge*: int               ## `ma` in seconds (defaults to `defaultMaxAge`)
    clear*: bool               ## the `clear` directive was present

  CacheEntry = object
    endpoint: AltSvcEndpoint
    expires: MonoTime

  AltSvcCache* = ref object
    ## Per-client origin -> h3-endpoint cache, keyed by `scheme://host:port`.
    entries: Table[string, CacheEntry]

proc parseAuthority(auth: string): AltSvcEndpoint =
  ## Parse an alt-authority: `[host]:port`, host optional (`:443`). IPv6 literals
  ## are bracketed (`[::1]:443`). Returns port 0 if unparseable.
  var s = auth.strip(chars = {'"', ' '})
  if s.len == 0: return
  if s[0] == '[':                       # [ipv6]:port
    let close = s.find(']')
    if close < 0: return
    result.host = s[1 ..< close]
    let rest = s[close + 1 .. ^1]
    if rest.startsWith(':'):
      result.port = try: parseInt(rest[1 .. ^1]) except ValueError: 0
  else:
    let colon = s.rfind(':')
    if colon < 0: return
    result.host = s[0 ..< colon]
    result.port = try: parseInt(s[colon + 1 .. ^1]) except ValueError: 0

proc parseAltSvc*(value: string): AltSvc =
  ## Parse one Alt-Svc header value (RFC 7838). Recognizes the first `h3=`
  ## advertisement and the `clear` directive; unknown protocol ids (h2, h3-29,
  ## ...) are ignored. Malformed input yields a zeroed result rather than raising,
  ## so a hostile or garbled header can never break a request.
  result.maxAge = defaultMaxAge
  let v = value.strip()
  if v.len == 0: return
  if v.toLowerAscii == "clear":
    result.clear = true
    return
  # Comma-separated alternatives; each is `id=authority; param=value; ...`.
  for entry in v.split(','):
    let parts = entry.split(';')
    if parts.len == 0: continue
    let head = parts[0].strip()
    let eq = head.find('=')
    if eq <= 0: continue
    let id = head[0 ..< eq].strip()
    if id != "h3": continue             # only final RFC 9114 h3, not draft ids
    let ep = parseAuthority(head[eq + 1 .. ^1])
    if ep.port == 0: continue           # need a usable port
    result.h3 = true
    result.endpoint = ep
    for i in 1 ..< parts.len:           # params: we care about `ma`
      let p = parts[i].strip()
      let peq = p.find('=')
      if peq <= 0: continue
      if p[0 ..< peq].strip() == "ma":
        result.maxAge = try: parseInt(p[peq + 1 .. ^1].strip())
                        except ValueError: defaultMaxAge
    return                              # first h3 wins
  return

proc newAltSvcCache*(): AltSvcCache =
  AltSvcCache(entries: initTable[string, CacheEntry]())

proc originKey*(scheme, host: string, port: int): string =
  ## Canonical origin key: `scheme://host:port` (scheme/host lowercased).
  scheme.toLowerAscii & "://" & host.toLowerAscii & ":" & $port

proc record*(c: AltSvcCache, scheme, host: string, port: int, header: string) =
  ## Update the cache for an origin from its `Alt-Svc` response header. An `h3=`
  ## advertisement is stored with its max-age (an empty alt host resolves to the
  ## origin host); `clear`, an absent h3, or a non-positive `ma` drops the origin.
  if c == nil or header.len == 0: return
  let a = parseAltSvc(header)
  let key = originKey(scheme, host, port)
  if a.clear or not a.h3 or a.maxAge <= 0:
    c.entries.del(key)
    return
  var ep = a.endpoint
  if ep.host.len == 0: ep.host = host   # ":443" means same host as the origin
  c.entries[key] = CacheEntry(
    endpoint: ep,
    expires: getMonoTime() + initDuration(seconds = a.maxAge))

proc h3Endpoint*(c: AltSvcCache, scheme, host: string, port: int): Option[AltSvcEndpoint] =
  ## The cached, unexpired h3 endpoint for an origin, or `none`. Expired entries
  ## are evicted on read.
  if c == nil: return
  let key = originKey(scheme, host, port)
  let entry = c.entries.getOrDefault(key)
  if entry.endpoint.port == 0: return   # miss (zeroed default)
  if getMonoTime() >= entry.expires:
    c.entries.del(key)
    return
  some(entry.endpoint)

proc clear*(c: AltSvcCache) =
  ## Drop all cached advertisements (e.g. on client close).
  if c != nil: c.entries.clear()
