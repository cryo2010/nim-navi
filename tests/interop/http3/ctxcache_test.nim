## Interop test: the QUIC SSL_CTX is built once per TLS policy and shared by every
## connection made with it (#454), instead of being rebuilt -- trust store, caBundle,
## client credential, ciphers -- inside every navi_h3_new. Asserts the four
## properties that make that safe: sharing, invalidation, release, and the bound.
##
## Also times `navi_h3_new` (which does no I/O beyond opening the UDP socket, so it
## is essentially the context work) so the win is measured, not assumed. run.sh runs
## this twice, the second time with NAVI_H3_CTX_CACHE=0, which restores the
## pre-#454 behaviour: that pair is the before/after measurement.
import std/[os, times, strutils]
import navi
import navi/backend/quic

let ca = getEnv("NAVI_H3_CA")
doAssert ca.len > 0, "NAVI_H3_CA must point at the origin cert"
let cacheOn = getEnv("NAVI_H3_CTX_CACHE") != "0"
echo "context cache ", (if cacheOn: "enabled" else: "DISABLED (pre-#454 behaviour)")

proc get200(c: QuicConn) =
  doAssert c.get("/").status == 200, "h3 GET failed"

# 1. Two connections made with the same policy share one context: it is built on the
#    first connection and only looked up on the second.
let s0 = h3CtxCacheStats()
let cfgA1 = TlsConfig(caFile: ca)
let c1 = h3Open("localhost", 4433, sni = "localhost", tls = cfgA1)
let s1 = h3CtxCacheStats()
let cfgA2 = TlsConfig(caFile: ca)   # an equal policy, built independently
let c2 = h3Open("localhost", 4433, sni = "localhost", tls = cfgA2)
let s2 = h3CtxCacheStats()
get200(c1)
get200(c2)                       # both connections work off the shared context
c1.close()
c2.close()
doAssert s1.builds == s0.builds + 1, "the first connection did not build a context"
if cacheOn:
  doAssert s2.builds == s1.builds, "the second connection rebuilt the context"
  doAssert s2.reuses == s1.reuses + 1, "the second connection was not a cache hit"
  echo "ok: two connections with the same TlsConfig shared one SSL_CTX"
else:
  doAssert s2.builds == s1.builds + 1, "the cache is disabled but nothing was built"
  echo "ok: cache disabled, every connection builds its own SSL_CTX"

# 2. A policy with different inputs never gets the cached context.
let cfgB = TlsConfig(caFile: ca, cipherSuites: "TLS_AES_128_GCM_SHA256")
let c3 = h3Open("localhost", 4433, sni = "localhost", tls = cfgB)
let s3 = h3CtxCacheStats()
get200(c3)
c3.close()
doAssert s3.builds == s2.builds + 1, "a different TlsConfig reused a context"
echo "ok: a different TlsConfig built its own SSL_CTX"

# 3. A client's context is released when the client is closed, together with the
#    TLS context store the TCP backends keep theirs in.
var ncfg = initNaviConfig()
ncfg.tls.caFile = ca
ncfg.http = {H1, H2, H3}
let api = newNavi(ncfg)
doAssert api.get("https://localhost:4433/").status == 200   # h2, learns Alt-Svc
let r = api.get("https://localhost:4433/")
doAssert r.httpVersion == "HTTP/3", "expected the h3 upgrade, got " & r.httpVersion
let s4 = h3CtxCacheStats()
api.close()
let s5 = h3CtxCacheStats()
if cacheOn:
  doAssert s5.entries == s4.entries - 1,
    "closing the client did not release its context: " & $s4.entries & " -> " & $s5.entries
  echo "ok: the client's SSL_CTX was released with its context store"

# 4. The cache is bounded, so churning policies cannot grow it without limit.
for i in 1 .. 12:
  # `password` is part of the policy (and so of the key) but is only consulted for a
  # client credential, so each of these is a distinct key over an identical context.
  let cfgC = TlsConfig(caFile: ca, password: "churn-" & $i)
  let c = h3Open("localhost", 4433, sni = "localhost", tls = cfgC)
  get200(c)
  c.close()
let s6 = h3CtxCacheStats()
doAssert s6.entries <= 8, "the context cache is unbounded: " & $s6.entries & " entries"
echo "ok: 12 churned policies left ", s6.entries, " cached contexts (bound 8)"

# 5. What it costs. navi_h3_new does the context work plus a UDP socket, no I/O, so
#    this is the per-connection setup an idle-timeout eviction or a server-forced
#    reconnect pays. A PKCS#12 credential (deliberately slow to decrypt) is included
#    when run.sh built one, since that is the worst case #454 is about.
let p12 = getEnv("NAVI_H3_P12")
var benchCfg = TlsConfig(caFile: ca, pkcs12File: p12,
                         password: getEnv("NAVI_H3_P12_PASS"))
var bt = toH3Tls(benchCfg, 1000)
const iters = 50
let t0 = epochTime()
for i in 1 .. iters:
  let h = navi_h3_new("127.0.0.1".cstring, "4433".cstring, "localhost".cstring,
                      addr bt, 0)
  doAssert h != nil, "navi_h3_new failed"
  navi_h3_close(h)
let ms = (epochTime() - t0) * 1000.0
echo "bench: ", iters, " x navi_h3_new+close in ", formatFloat(ms, ffDecimal, 2),
     " ms (", formatFloat(ms / iters.float, ffDecimal, 3), " ms each), pkcs12=",
     (if p12.len > 0: "yes" else: "no"), ", cache=", cacheOn

echo "NAVI HTTP/3 CTX CACHE OK"
