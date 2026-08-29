# Hardening Guide

navi is **secure by default**: certificate and hostname verification are on,
credentials never cross an origin boundary on a redirect, cookies are re-scoped
per host, and a hostile server's ability to make the client allocate, wait, or
retry is bounded. See [THREAT_MODEL.md](THREAT_MODEL.md) for what that covers.

This guide is for going *beyond* the defaults: pinning TLS, bounding response
size, adding timeouts, and locking down trust for higher-assurance deployments.
Every knob here lives on `NaviConfig` (built with `initNaviConfig`) and is applied
by `newNavi(config)`.

## Quick recipes

Copy one of these and adjust. Each is a complete, compiling `NaviConfig`.

### 1. Maximum-assurance public API client

For a well-known public endpoint where you want a modern TLS floor, a bounded
body, and deadlines so nothing hangs.

```nim
import navi

var config = initNaviConfig()
config.tls.minVersion   = tls13                      # refuse anything below TLS 1.3
config.tls.cipherSuites  = "TLS_AES_256_GCM_SHA384:TLS_AES_128_GCM_SHA256"
config.maxResponseBytes = 32 * 1024 * 1024           # cap decompressed body at 32 MiB
config.maxRedirects     = 5                           # fewer hops than the default 20
config.timeouts.connect = 5_000                       # 5 s to connect + handshake
config.timeouts.read    = 15_000                      # 15 s between response chunks
config.timeouts.total   = 60_000                      # 60 s for the whole request
let api = newNavi(config)
```

Why: `tls13` drops every legacy protocol version; the response cap makes a
decompression bomb harmless; the three timeouts bound establishment, a stalled
chunk, and the whole request so a slow-loris server cannot pin the call open.

### 2. Internal service behind a private CA

For a service whose certificate chains to your own CA rather than a public root,
optionally with mutual TLS.

```nim
import navi

var config = initNaviConfig()
config.tls.caFile   = "/etc/navi/internal-ca.pem"    # trust only this CA (verify stays on)
config.tls.minVersion = tls12
# Optional mutual TLS: present a client certificate.
config.tls.certFile = "/etc/navi/client.pem"
config.tls.keyFile  = "/etc/navi/client.key"          # "" reuses certFile if it holds the key
let api = newNavi(config)
```

Why: `caFile` replaces the system trust store, so navi accepts only certificates
that chain to your private root; verification stays on. The client certificate
lets the server authenticate navi in return. A PKCS#12 bundle
(`config.tls.pkcs12File = "client.p12"; config.tls.password = "..."`) is an
alternative to the cert/key pair.

### 3. Untrusted or hostile endpoint

For fetching from a server you do not control and cannot trust to behave.

```nim
import navi

var config = initNaviConfig()
config.maxResponseBytes = 5 * 1024 * 1024            # tight 5 MiB body cap
config.maxRedirects     = 0                            # do not auto-follow redirects
config.timeouts.connect = 3_000
config.timeouts.read    = 5_000
config.timeouts.total   = 20_000
config.retry.limit      = 0                            # no retries against a hostile peer
let api = newNavi(config)
```

Why: verification is already on, so this recipe adds resource bounds. The tight
body cap and short timeouts limit what a malicious server can consume;
`maxRedirects = 0` returns the 3xx as-is so you can inspect the `Location` and
decide whether to follow it (SSRF defense is the application's, see
[THREAT_MODEL.md](THREAT_MODEL.md#application-responsibilities)).

## Control reference

Each control below lists its default, the hardened setting, and why it matters.
Field names and types match `TlsConfig` in `src/navi/backend/api.nim` and the
`NaviConfig` table in the [README](README.md#naviconfig).

### TLS verification

| | |
|---|---|
| Default | `config.tls.verify = true` (chain **and** hostname) |
| Hardened | leave on; never set `false` outside tests |

Verification is on by default and covers both the certificate chain and the
hostname. `verify = false` disables both and is intended only for tests against
self-signed servers. If you need to trust a non-public CA, do **not** disable
verification; set `caFile` instead.

### Custom trust anchor (`caFile`)

```nim
config.tls.caFile = "/etc/navi/internal-ca.pem"
```

Default `""` uses the system trust store. Setting `caFile` restricts trust to the
given CA bundle, which both enables a private CA and narrows the accepted chain
for a public one. Verification stays on. `tls.caBundle` does the same from an
in-memory PEM string (added alongside the system roots / `caFile`).

### Public-key pinning (`pinnedKeys`)

```nim
# base64(SHA-256(DER SubjectPublicKeyInfo)); compute with:
#   openssl x509 -in cert.pem -pubkey -noout \
#     | openssl pkey -pubin -outform der | openssl dgst -sha256 -binary | base64
config.tls.pinnedKeys = @["r/pas0ue6zqoBH4vVvBxz7i+94EMJ3kAdyJWSd381TY="]
```

Beyond trusting a CA, `pinnedKeys` requires the peer's public key to match one of
the given SPKI pins, rejecting an otherwise chain-valid certificate whose key is
not pinned (e.g. a mis-issued cert from another CA). Pin the leaf and at least one
backup key so a routine key rotation does not lock you out. For arbitrary custom
logic over the leaf certificate, `tls.verifyCallback` receives it in DER form and
returns whether to accept; both run after the standard chain + hostname checks.

### TLS version floor and ceiling

```nim
config.tls.minVersion = tls12    # or tls13
config.tls.maxVersion = tls13
```

Default `tlsDefault` leaves the bound to the library. Pin `minVersion` to refuse
downgrade to a weak protocol; a negotiation outside the pinned range fails the
handshake. Enforced on the OpenSSL backends (sync, asyncdispatch); chronos
(BearSSL) tops out at TLS 1.2 and raises if you request `tls13`.

### Cipher restriction

```nim
config.tls.ciphers      = "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256"
config.tls.cipherSuites = "TLS_AES_256_GCM_SHA384:TLS_AES_128_GCM_SHA256"
```

Default `""` keeps OpenSSL's selection. `ciphers` restricts TLS <=1.2, and
`cipherSuites` restricts TLS 1.3, since OpenSSL exposes them through separate
APIs; set whichever applies to the versions you allow. A value with no cipher the
peer accepts fails the handshake rather than silently falling back.

### Client certificate (mTLS)

```nim
config.tls.certFile = "client.pem"
config.tls.keyFile  = "client.key"    # "" reuses certFile
# or a bundle:
config.tls.pkcs12File = "client.p12"
config.tls.password   = "secret"
```

Off by default. Precedence is `pkcs12File`, then in-memory (`certPem`/`keyPem`),
then the `certFile`/`keyFile` pair. OpenSSL backends only; chronos and js do not
present client certificates.

### Session resumption

```nim
config.tls.resumeSessions = false    # only if you must not reuse sessions
```

On by default and scoped per origin (a cached session is only presented back to
the server it came from), so it is safe to leave on. Disable it only if your
threat model forbids session reuse.

### Response body cap

```nim
config.maxResponseBytes = 32 * 1024 * 1024   # 32 MiB
```

Default `0` (unbounded). This is the single most important knob for untrusted
servers: on the native backends the cap counts *decompressed* bytes, so it is the
decompression-bomb guard. The overflowing chunk is never delivered and, for
HTTP/2, the stream is RST.

### Redirects

```nim
config.maxRedirects = 0     # or a small number
```

Default `20`. `0` returns the 3xx as-is so you can inspect `Location` before
following (useful against SSRF). Regardless of this value, navi strips
`Authorization` and re-scopes cookies across an origin change, so credentials do
not leak on a followed redirect.

### Timeouts

```nim
config.timeouts.connect = 5_000    # TCP connect + TLS handshake
config.timeouts.read    = 15_000   # per-read idle (a stalled chunk)
config.timeouts.total   = 60_000   # whole request, incl. retries/redirects
```

All default `0` (off). Set all three against servers that might stall. `total`
is enforced on all four backends; `connect`/`read` on the native ones.

### Retries

```nim
config.retry.limit    = 0        # disable, e.g. against a hostile peer
config.retry.maxDelay = 10_000   # clamps a hostile Retry-After (ms)
```

Default `limit = 2`, idempotent verbs only, `maxDelay = 10_000`. A `Retry-After`
header is honored but clamped to `maxDelay`, so a server cannot park the client
for hours. Set `limit = 0` to disable retries entirely.

### Decompression

```nim
config.decompress = false   # hand back the raw encoded body
```

On by default (decodes gzip/deflate/br). Leave it on with `maxResponseBytes` set;
the cap counts decoded bytes, so the two together bound a compression bomb.

### HTTP/2 limits

The HTTP/2 bounds (HPACK decoded-list cap, 128 KiB CONTINUATION accumulation cap,
bounded flow-control window, server push disabled) are always on and not
configurable, because their safe values are not a tuning decision. They need no
hardening; they are described in [THREAT_MODEL.md](THREAT_MODEL.md#denial-of-service-can-a-hostile-server-exhaust-the-client).

### HTTP/3

```nim
config.http = {H1, H2, H3}   # requires a -d:naviHttp3 build
```

Opt-in: HTTP/3 is honored only in a `-d:naviHttp3` build and must be listed
explicitly in `http` (an empty set does not imply it). It is reached per origin
after Alt-Svc discovery and performs the same certificate verification as the
other backends.

### Proxy

```nim
config.proxy = "http://proxy.internal:8080"
```

Default `""` falls back to the `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY`
environment variables. For an https target the backend issues a `CONNECT` tunnel,
so TLS is still end-to-end to the origin and the proxy sees only the encrypted
stream. Set `proxy` explicitly to avoid depending on ambient environment.

### Auth

```nim
config.auth = basicAuth("user", "pass")   # or bearerAuth("token"), etc.
```

`Authorization` set via `config.auth` (or a header) is applied to every request
and stripped when a redirect crosses to a different origin, so a credential is
never sent to a host that did not originally receive it. Secrets placed directly
in a URL query string are not managed by navi.
