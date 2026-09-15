## Resolve which proxy (if any) a request should use.
##
## Precedence: an explicit `proxy` option, else the standard environment
## variables (HTTP_PROXY/HTTPS_PROXY and lowercase forms), with NO_PROXY
## exclusions honored either way.
##
## The env vars are read, the proxy URL is parsed, and NO_PROXY is split ONCE
## when the client is built (`buildResolvedProxy`, called from `newNavi`/`extend`),
## not on every request attempt. Per request only the cheap NO_PROXY host match
## against the prepared list remains. Because `proxy` is documented as bound when
## connections open (not live-reconfigurable), freezing this at construction is
## consistent with navi's contract. An extended client with a different `proxy`
## string gets its own cache: `newNavi`/`extend` rebuild it after `mergeBase`.

import std/[os, strutils]
import ./url, ./request
import ../backend/api

proc parseTarget(raw: string): ProxyTarget =
  ## Parse a proxy URL into a dial target. Empty `raw` -> a direct target.
  ## Raises `ValueError` on a malformed/out-of-range port (surfaced at client
  ## construction now, rather than deferred to the first request).
  if raw.len == 0: return direct()
  let u = parseUrl(raw)
  let scheme = u.raw.scheme.toLowerAscii
  let socks = scheme == "socks5" or scheme == "socks5h" or scheme == "socks"
  let port =
    if u.raw.port.len > 0: u.port   # validated + range-checked, clear error on garbage
    elif socks: 1080
    else: 80
  ProxyTarget(kind: if socks: pkSocks5 else: pkHttp,
              host: u.host, port: port,
              user: u.raw.username, pass: u.raw.password)

proc prepareNoProxy(): seq[string] =
  ## The NO_PROXY entries, normalized once (trimmed, leading dots stripped,
  ## lowercased, blanks dropped). A `*` entry is preserved as-is.
  let noProxy = getEnv("no_proxy", getEnv("NO_PROXY"))
  for raw in noProxy.split(','):
    let entry = raw.strip.strip(chars = {'.'}).toLowerAscii
    if entry.len > 0: result.add entry

proc excludedBy(entries: seq[string], host: string): bool =
  ## True when `host` matches a prepared NO_PROXY entry.
  if entries.len == 0: return false
  let h = host.toLowerAscii
  for entry in entries:
    if entry == "*": return true
    if h == entry or h.endsWith("." & entry): return true

proc buildResolvedProxy*(opts: NaviConfigBase): ResolvedProxy =
  ## Resolve the proxy configuration once, at client construction. Reads the env
  ## vars, parses the effective proxy URL, and prepares the NO_PROXY list up front
  ## so that per-request resolution is a cheap host match. A configured
  ## `unixSocket` takes precedence and bypasses proxies entirely (it is a local
  ## dial target, not a proxy). When `opts.proxy` is set it wins over the env for
  ## both http and https targets; otherwise the http/https env vars are parsed
  ## separately since the request scheme selects which applies.
  new(result)
  if opts.unixSocket.len > 0:
    result.unix = ProxyTarget(kind: pkUnix, host: opts.unixSocket)
    return
  result.noProxy = prepareNoProxy()
  if opts.proxy.len > 0:
    let t = parseTarget(opts.proxy)
    result.httpTarget = t
    result.httpsTarget = t
  else:
    result.httpTarget = parseTarget(getEnv("http_proxy", getEnv("HTTP_PROXY",
                          getEnv("all_proxy", getEnv("ALL_PROXY")))))
    result.httpsTarget = parseTarget(getEnv("https_proxy", getEnv("HTTPS_PROXY",
                          getEnv("all_proxy", getEnv("ALL_PROXY")))))

proc resolveFor(r: ResolvedProxy, url: Url): ProxyTarget =
  ## Resolve against an already-built cache: a cheap per-target NO_PROXY match.
  if r.unix.kind == pkUnix:
    return r.unix
  if excludedBy(r.noProxy, url.host):
    return direct()
  if url.isTls: r.httpsTarget else: r.httpTarget

proc resolveProxy*(opts: NaviConfigBase, url: Url): ProxyTarget =
  ## The proxy to dial for `url`, or a direct target when none applies, using the
  ## cache built at construction (`buildResolvedProxy`). Only the NO_PROXY host
  ## match is done per call; env reads and URL parsing already happened once.
  let r = opts.resolvedProxy
  if r == nil:
    # Defensive: a config that skipped `newNavi`/`extend` (e.g. constructed by
    # hand in a test) still resolves correctly, just without the cache.
    return buildResolvedProxy(opts).resolveFor(url)
  r.resolveFor(url)
