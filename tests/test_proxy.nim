## Proxy resolution, cached once at construction (issue #361).
##
## Exercises `buildResolvedProxy` + `resolveProxy` directly: opts.proxy
## precedence, the http/https env-var fallback, NO_PROXY exclusions, the
## unixSocket bypass, socks scheme handling, and that a distinct proxy string
## resolves to a distinct cache (the .extend contract).
import unittest, std/os
import navi/core/[proxy, request, url]
import navi/backend/api

const proxyEnvVars = ["http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY",
                      "all_proxy", "ALL_PROXY", "no_proxy", "NO_PROXY"]

proc clearProxyEnv() =
  for n in proxyEnvVars: delEnv(n)

proc resolved(proxy = "", unixSocket = ""): NaviConfigBase =
  ## A base config with its proxy cache built the way `newNavi` builds it.
  result = NaviConfigBase(proxy: proxy, unixSocket: unixSocket)
  result.resolvedProxy = buildResolvedProxy(result)

suite "proxy resolution (cached at construction)":
  setup:
    clearProxyEnv()
  teardown:
    clearProxyEnv()

  test "no proxy configured and no env yields a direct target":
    let cfg = resolved()
    let t = cfg.resolveProxy(parseUrl("http://example.test/"))
    check not t.isSet
    check t.kind == pkHttp

  test "an explicit http proxy is parsed once and reused":
    let cfg = resolved(proxy = "http://user:pass@127.0.0.1:8080")
    let t = cfg.resolveProxy(parseUrl("https://example.test/"))
    check t.isSet
    check t.kind == pkHttp
    check t.host == "127.0.0.1"
    check t.port == 8080
    check t.user == "user"
    check t.pass == "pass"

  test "a socks5 scheme becomes a SOCKS5 target with the default port":
    let cfg = resolved(proxy = "socks5://127.0.0.1")
    let t = cfg.resolveProxy(parseUrl("https://example.test/"))
    check t.kind == pkSocks5
    check t.port == 1080

  test "opts.proxy wins over env vars":
    putEnv("HTTPS_PROXY", "http://env.proxy:3128")
    let cfg = resolved(proxy = "http://explicit.proxy:8080")
    let t = cfg.resolveProxy(parseUrl("https://example.test/"))
    check t.host == "explicit.proxy"
    check t.port == 8080

  test "env fallback selects HTTPS_PROXY for a tls target":
    putEnv("HTTPS_PROXY", "http://secure.proxy:8443")
    putEnv("HTTP_PROXY", "http://plain.proxy:8080")
    let cfg = resolved()
    let t = cfg.resolveProxy(parseUrl("https://example.test/"))
    check t.host == "secure.proxy"
    check t.port == 8443

  test "env fallback selects HTTP_PROXY for a plain target":
    putEnv("HTTPS_PROXY", "http://secure.proxy:8443")
    putEnv("HTTP_PROXY", "http://plain.proxy:8080")
    let cfg = resolved()
    let t = cfg.resolveProxy(parseUrl("http://example.test/"))
    check t.host == "plain.proxy"
    check t.port == 8080

  test "ALL_PROXY is the fallback when no scheme-specific var is set":
    putEnv("ALL_PROXY", "http://all.proxy:9000")
    let cfg = resolved()
    check cfg.resolveProxy(parseUrl("http://example.test/")).host == "all.proxy"
    check cfg.resolveProxy(parseUrl("https://example.test/")).host == "all.proxy"

  test "NO_PROXY excludes a matching host despite a configured proxy":
    putEnv("NO_PROXY", "example.test, .internal")
    let cfg = resolved(proxy = "http://127.0.0.1:8080")
    check not cfg.resolveProxy(parseUrl("https://example.test/")).isSet
    # subdomain of an entry with a leading dot
    check not cfg.resolveProxy(parseUrl("https://api.internal/")).isSet
    # a host not on the list still routes through the proxy
    check cfg.resolveProxy(parseUrl("https://other.test/")).isSet

  test "a NO_PROXY wildcard excludes everything":
    putEnv("no_proxy", "*")
    let cfg = resolved(proxy = "http://127.0.0.1:8080")
    check not cfg.resolveProxy(parseUrl("https://anything.test/")).isSet

  test "a unixSocket target bypasses proxies entirely":
    putEnv("HTTPS_PROXY", "http://env.proxy:3128")
    let cfg = resolved(unixSocket = "/tmp/navi.sock")
    let t = cfg.resolveProxy(parseUrl("https://example.test/"))
    check t.kind == pkUnix
    check t.host == "/tmp/navi.sock"

  test "distinct proxy strings resolve to distinct caches (extend contract)":
    let a = resolved(proxy = "http://a.proxy:1111")
    let b = resolved(proxy = "http://b.proxy:2222")
    check a.resolveProxy(parseUrl("http://x.test/")).host == "a.proxy"
    check b.resolveProxy(parseUrl("http://x.test/")).host == "b.proxy"
    check a.resolvedProxy != b.resolvedProxy

  test "the cache is reused across many resolutions (no per-call env read)":
    putEnv("HTTP_PROXY", "http://cached.proxy:8080")
    let cfg = resolved()
    let first = cfg.resolveProxy(parseUrl("http://example.test/"))
    # Change the env AFTER construction: a cached resolver must not observe it.
    putEnv("HTTP_PROXY", "http://changed.proxy:9999")
    let second = cfg.resolveProxy(parseUrl("http://example.test/"))
    check first.host == "cached.proxy"
    check second.host == "cached.proxy"

  test "an invalid proxy port raises at construction, not per request":
    expect ValueError:
      discard resolved(proxy = "http://127.0.0.1:notaport")
