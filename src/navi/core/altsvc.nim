## Alt-Svc (RFC 7838) discovery for HTTP/3.
##
## HTTP/3 has no ALPN-on-first-connect path: a client reaches h3 by first talking
## h1/h2 and noticing an `Alt-Svc: h3=...` advertisement (or, later, an HTTPS DNS
## record). This module parses that header and keeps a small per-client cache of
## "origin -> h3 endpoint" so subsequent requests can upgrade. Nothing here dials
## or negotiates; it is pure bookkeeping, testable without a network, and safe to
## compile in every build (the transport that consumes it is `-d:naviHttp3`-only).

import std/[tables, options, strutils, monotimes, times]
import ./url

const defaultMaxAge = 86_400   ## RFC 7838: `ma` defaults to 24h when absent.

const
  brokenBackoffSecs* = 60      ## First backoff after an h3 endpoint fails to connect.
  brokenBackoffMaxSecs* = 960  ## Ceiling for the doubling (16 minutes).

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
    brokenUntil: MonoTime  ## while in the future, the endpoint is suppressed
    failures: int          ## consecutive connect failures, for the doubling backoff

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
  # A re-advertisement of the SAME endpoint must not clear an active backoff: an
  # origin repeats its Alt-Svc header on every TCP response, so resetting here would
  # put the client straight back on the UDP path it just failed to reach (#432). A
  # different alt-authority is a genuinely new alternative, so it starts clean.
  let prior = c.entries.getOrDefault(key)
  let same = prior.endpoint == ep
  c.entries[key] = CacheEntry(
    endpoint: ep,
    expires: getMonoTime() + initDuration(seconds = a.maxAge),
    brokenUntil: (if same: prior.brokenUntil else: default(MonoTime)),
    failures: (if same: prior.failures else: 0))

proc recordFrom*(c: AltSvcCache, url: Url, header: string) =
  ## Record one response's `Alt-Svc` header against the origin it came from. This
  ## is the ONLY entry point the transports use, because it carries the RFC 7838
  ## 2.1 gate: an alternative may only be learned from a response that arrived
  ## over TLS. A cleartext `http://host:port` response is ignored outright.
  ##
  ## The cache is keyed on the "https" scheme (h3 is TLS-only) and `h3Endpoint`
  ## is only consulted for `isTls` requests, so recording an advertisement seen on
  ## the cleartext leg of a host that is ALSO reached over https would let an
  ## on-path attacker on that cleartext leg pick the QUIC endpoint for the https
  ## origin (or clear the entry) for the advertised max-age (#434).
  ##
  ## The gate is `isTls` and not "verified TLS": whether the peer was
  ## authenticated is not recorded on the response, and the one bit that is
  ## reachable here (`insecureSkipVerify`) does not answer the question. It is
  ## also set by the pins-only posture (`insecureSkipVerify` plus `pinnedKeys` /
  ## `verifyCallback`), where the response IS authenticated, while on its own it
  ## already forfeits the whole channel, so an attacker who could inject the
  ## header could equally serve the response. Gating on it would therefore break
  ## a legitimate configuration for no gain.
  if not url.isTls: return
  c.record("https", url.host, url.port, header)

proc markBroken*(c: AltSvcCache, scheme, host: string, port: int) =
  ## Note the origin's h3 alternative as broken (RFC 7838 2.4): the QUIC handshake
  ## failed before anything was submitted, so `h3Endpoint` suppresses it for a
  ## backoff window and requests go straight to h2/h1. The window doubles from
  ## `brokenBackoffSecs` with each consecutive failure, up to `brokenBackoffMaxSecs`,
  ## so a permanently UDP-blocked network stops paying a handshake per request. A
  ## no-op when the origin has no cached advertisement.
  if c == nil: return
  let key = originKey(scheme, host, port)
  c.entries.withValue(key, e):
    e.failures = min(e.failures + 1, 16)      # clamped so the shift below cannot overflow
    let secs = min(brokenBackoffSecs shl (e.failures - 1), brokenBackoffMaxSecs)
    e.brokenUntil = getMonoTime() + initDuration(seconds = secs)

proc markWorking*(c: AltSvcCache, scheme, host: string, port: int) =
  ## Clear an origin's h3 backoff after a successful QUIC connection, so a network
  ## that recovers is used again immediately instead of serving out the window.
  if c == nil: return
  let key = originKey(scheme, host, port)
  c.entries.withValue(key, e):
    if e.failures > 0:
      e.failures = 0
      e.brokenUntil = default(MonoTime)

template openH3Tracked*(cache: AltSvcCache, host: string, port: int,
                        openExpr: untyped): untyped =
  ## Open an h3 connection through `openExpr`, keeping the origin's RFC 7838 2.4
  ## bookkeeping in one place: a `QuicError` out of the open marks the alternative
  ## broken (the next request then goes straight to TCP instead of paying the same
  ## stalled QUIC handshake again) and is re-raised unchanged, while a successful
  ## open clears any active backoff. Every h3 opener goes through here -- the sync
  ## buffered transport, the sync streaming leg and the shared async `getH3Conn` --
  ## so the policy can no longer be changed in two paths out of three, silently
  ## leaving the per-request stall in the third (#453).
  ##
  ## `openExpr` is untyped, so the same template serves a plain blocking call and an
  ## `await`ed one under both asyncdispatch and chronos. `QuicError` is deliberately
  ## left to bind at the expansion site: it lives in `navi/backend/quic`, which
  ## imports this module, so naming it here would be an import cycle.
  ##
  ## The scheme is always "https" (h3 is TLS-only) and `host`/`port` are the ORIGIN's,
  ## not the alternative's authority: that is how the cache is keyed.
  block:
    let trackedHost = host
    let trackedPort = port
    let trackedConn = try:
        openExpr
      except QuicError:
        cache.markBroken("https", trackedHost, trackedPort)
        raise
    cache.markWorking("https", trackedHost, trackedPort)
    trackedConn

proc h3Endpoint*(c: AltSvcCache, scheme, host: string, port: int): Option[AltSvcEndpoint] =
  ## The cached, unexpired h3 endpoint for an origin, or `none`. Expired entries
  ## are evicted on read.
  if c == nil: return
  let key = originKey(scheme, host, port)
  let entry = c.entries.getOrDefault(key)
  if entry.endpoint.port == 0: return   # miss (zeroed default)
  let now = getMonoTime()
  if now >= entry.expires:
    c.entries.del(key)
    return
  if now < entry.brokenUntil: return    # in the RFC 7838 2.4 backoff: stay on h2/h1
  some(entry.endpoint)

proc clear*(c: AltSvcCache) =
  ## Drop all cached advertisements (e.g. on client close).
  if c != nil: c.entries.clear()

const h3SkipHeaders* = ["host", "connection", "keep-alive", "proxy-connection",
                        "transfer-encoding", "upgrade", "content-length"]
  ## Request fields that must not cross to HTTP/3: pseudo-header sources and
  ## connection-specific fields (RFC 9114). accept-encoding IS forwarded, so the
  ## response is compressed on the wire; the policy layer's decodeBody decompresses
  ## it (keyed on the content-encoding header the h3 response carries), exactly as
  ## for h1/h2.
