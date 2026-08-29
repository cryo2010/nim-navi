## Resolve which proxy (if any) a request should use.
##
## Precedence: an explicit `proxy` option, else the standard environment
## variables (HTTP_PROXY/HTTPS_PROXY and lowercase forms), with NO_PROXY
## exclusions honored either way.

import std/[os, strutils]
import ./url, ./request
import ../backend/api

proc envProxy(url: Url): string =
  let names = if url.isTls: ["https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"]
              else: ["http_proxy", "HTTP_PROXY", "all_proxy", "ALL_PROXY"]
  for n in names:
    let v = getEnv(n)
    if v.len > 0: return v

proc excluded(host: string): bool =
  ## True when `host` matches an entry in NO_PROXY.
  let noProxy = getEnv("no_proxy", getEnv("NO_PROXY"))
  for raw in noProxy.split(','):
    let entry = raw.strip.strip(chars = {'.'}).toLowerAscii
    if entry.len == 0: continue
    if entry == "*": return true
    let h = host.toLowerAscii
    if h == entry or h.endsWith("." & entry): return true

proc resolveProxy*(opts: NaviConfigBase, url: Url): ProxyTarget =
  ## The proxy to dial for `url`, or a direct target when none applies. The proxy
  ## URL scheme selects the kind: `socks5`/`socks5h` -> SOCKS5, anything else ->
  ## HTTP. A `user:pass@` userinfo becomes the proxy credentials. A configured
  ## `unixSocket` takes precedence and bypasses proxies entirely (it is a local
  ## dial target, not a proxy).
  if opts.unixSocket.len > 0:
    return ProxyTarget(kind: pkUnix, host: opts.unixSocket)
  let raw = if opts.proxy.len > 0: opts.proxy else: envProxy(url)
  if raw.len == 0 or excluded(url.host):
    return direct()
  let u = parseUrl(raw)
  let scheme = u.raw.scheme.toLowerAscii
  let socks = scheme == "socks5" or scheme == "socks5h" or scheme == "socks"
  let port =
    if u.raw.port.len > 0: parseInt(u.raw.port)
    elif socks: 1080
    else: 80
  ProxyTarget(kind: if socks: pkSocks5 else: pkHttp,
              host: u.host, port: port,
              user: u.raw.username, pass: u.raw.password)
