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
config.tls.caFile   = "/etc/navi/internal-ca.pem"    # trust only this CA (verification stays on)
config.tls.minVersion = tls12
# Optional mutual TLS: present a client certificate.
config.tls.certFile = "/etc/navi/client.pem"
config.tls.keyFile  = "/etc/navi/client.key"          # "" reuses certFile if it holds the key
let api = newNavi(config)
```

Why: `caFile` replaces the system trust store (it does not add to it), so navi
accepts only certificates that chain to your private root; verification stays on.
Public https endpoints stop verifying under that config, which is the point here
and a surprise anywhere else -- see "Custom trust anchor" below. The client certificate
lets the server authenticate navi in return. A PKCS#12 bundle
(`config.tls.pkcs12File = "client.p12"; config.tls.password = "..."`) is an
alternative to the cert/key pair, and the intermediates inside it are presented
along with the leaf, so a server that trusts only your root can still build the
path.

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
| Default | `config.tls.insecureSkipVerify = false` (chain **and** hostname are checked) |
| Hardened | leave it unset; never set `true` outside tests |

Verification is on for every config, including a bare `TlsConfig()`, and covers both the certificate chain and the
hostname. The hostname (or IP literal) is bound into the handshake, so a mismatch
aborts it before any client certificate is sent, and the match follows RFC 9525:
partial wildcards such as `fo*.example.com` are rejected, and a certificate that
carries dNSName SANs is judged on those alone -- its subject CN counts only when it
has no SAN at all. `insecureSkipVerify = true` disables both and is intended only for tests
against self-signed servers. If you need to trust a non-public CA, do **not** disable
verification; set `caFile` instead (or `caBundle`, if the system roots must keep
working alongside it).

### Custom trust anchor (`caFile`)

```nim
config.tls.caFile = "/etc/navi/internal-ca.pem"
```

Default `""` uses the system trust store. Setting `caFile` **replaces** those
system roots with the given CA bundle -- curl's `--cacert` semantics -- on every
backend, HTTP/3 included: std/net's `newContext` scans the system store only when
`caFile` is empty, and the QUIC leg calls either
`SSL_CTX_load_verify_locations(caFile)` or `SSL_CTX_set_default_verify_paths()`,
never both. That both enables a private CA and narrows the accepted chain for a
public one, and it means any public endpoint the process also talks to fails
verification until its root is in the file. Verification itself stays on.

`tls.caBundle` is the **additive** option: an in-memory PEM string whose
certificates are added to whatever the store already holds (the system roots when
`caFile` is empty, the `caFile` anchors when it is not). Use it to trust a private
CA while public roots keep working; use `caFile` to trust nothing else.

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
handshake. Enforced on all three native OpenSSL backends (sync, asyncdispatch,
chronos), so `tls13` is honored on chronos too.

### Renegotiation (not configurable: off on OpenSSL 1.1.0 and newer)

On **OpenSSL 1.1.0 and newer** every navi TLS context sets
`SSL_OP_NO_RENEGOTIATION`, so a TLS 1.2 (or earlier) peer cannot start a
mid-connection handshake: OpenSSL answers a `HelloRequest` with a
`no_renegotiation` warning alert and the connection carries on. TLS 1.3 has no
renegotiation at all, RFC 9113 9.2.1 forbids it for HTTP/2 regardless of version,
and navi never requests one itself.

This is not free on every backend. A TLS 1.2 server that defers its
client-certificate request to a renegotiation -- the per-directory pattern some
Apache and IIS deployments use -- used to be served transparently by the sync and
asyncdispatch backends (a blocking `SSL_read` with `SSL_MODE_AUTO_RETRY` just
completed the new handshake); such a server now gets a `no_renegotiation` alert
and the request typically fails. Have the server ask for the certificate in the
initial handshake instead (`config.tls.certFile` and friends below). There is no
knob to re-enable renegotiation.

The option is deliberately **not** set on OpenSSL 1.0.x or on LibreSSL: those
libraries spend that option bit on something else entirely (OpenSSL 1.0.x on
`SSL_OP_NETSCAPE_DEMO_CIPHER_CHANGE_BUG`, LibreSSL on `SSL_OP_NO_DTLSv1`, which
numbers its own `SSL_OP_NO_RENEGOTIATION` elsewhere), so setting it there would
change an unrelated flag rather than refuse renegotiation. Against such a library
a peer-driven renegotiation is still refused, just later and less politely: the
sync and asyncdispatch backends let OpenSSL complete it, and the chronos backend
fails that connection with "TLS peer requested renegotiation during a write".

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
then the `certFile`/`keyFile` pair. Supported on the native OpenSSL backends
(sync, asyncdispatch, chronos); js does not present client certificates.

### Key material in memory (`clearTlsSecrets`)

```nim
let api = newNavi(config)
api.clearTlsSecrets()          # zero navi's copy of password/keyPem/certPem
config.tls.clearTlsSecrets()   # and your own: newNavi copied the config by value
```

`certPem`, `keyPem` and `password` are plain Nim strings, and a client holds its
copy for its whole lifetime because the TLS contexts are built lazily, one per
ALPN shape on first connect. Left alone, a core file, a heap dump or a
memory-disclosure bug in a long-running process yields the passphrase and the PEM
private key in cleartext at several addresses, long after OpenSSL has the
decrypted key and navi has no further use for them.

`clearTlsSecrets` builds the remaining contexts eagerly and then zeroes navi's
copy, so the client keeps working, mTLS included. Worth calling in a service that
presents a client certificate and expects to run for days; pointless for a
short-lived process, and a no-op for a client with no in-memory credential.

What it does not do: it cannot reach the config *you* built (`newNavi` takes it by
value), so wipe that yourself; the same `cleanse` is exported for any other secret
string you hold. And it is refused with a `ValueError` while HTTP/3 is enabled on
a `-d:naviHttp3` build, because the h3 driver rebuilds its TLS context from these
fields per connection: drop `H3` from `config.http` if you want the wipe, or keep
the material in memory. Whichever you choose, the plaintext navi reads out of
`certFile`/`keyFile`/`pkcs12File` is now zeroed before its buffer is freed.

One thing no wipe can do is reach a secret that was compiled in. A string built
from a literal, a `const` or `staticRead` is backed by the binary's read-only
data, and under `--mm:arc`/`--mm:orc` every copy shares that payload, so
`cleanse` writes over a private copy while the original stays readable for the
life of the process (and sits in the binary on disk regardless). Read the
passphrase and the key at run time, from a file, an environment variable or a
secrets API.

This is a defence in depth against a *process*-level disclosure, not against an
attacker who can already run code in the process: OpenSSL still holds the
decrypted key, and nothing here protects it.

### Session resumption

```nim
config.tls.resumeSessions = false    # only if you must not reuse sessions
```

On by default and scoped per origin (a cached session is only presented back to
the server it came from), so it is safe to leave on. Disable it only if your
threat model forbids session reuse.

### Truncation of a body delimited by the connection close

No knob: navi always rejects it. A response with neither `Content-Length` nor
chunked framing (an HTTP/1.0 origin, a `Connection: close` error page, an
un-chunked event stream) ends where the connection ends, so the only proof that
the body is complete is TLS's `close_notify` alert. Over https navi refuses such
a body when the transport died without one -- an injected RST, a bare FIN, a
crashed origin -- and raises `IOError` ("TLS connection closed without
close_notify; response may be truncated") instead of returning a short body as a
complete 200. Length- and chunk-delimited bodies are unaffected (their framing
already detects a short read), plain http cannot be protected this way, and an
EOF that arrives before any response bytes is still treated as a keep-alive race
and retried on a fresh connection.

### Response body cap

```nim
config.maxResponseBytes = 32 * 1024 * 1024   # 32 MiB
```

Default `0` (unbounded). This is the single most important knob for untrusted
servers: on the native backends the cap counts *decompressed* bytes, so it is the
decompression-bomb guard. The overflowing chunk is never delivered and, for
HTTP/2, the stream is RST.

The cap belongs to the **request**, not to the connection it rides: on HTTP/2 it is
applied per stream, so a pooled or multiplexed connection applies the cap each
request was issued with rather than the one its opener happened to have. Two
consequences worth knowing when you set a cap:

* **An SSE stream is not covered by it.** `sse()` deliberately runs with the cap off
  (an event stream is unbounded by design). If you need a bound on one, bound it
  yourself in the `each`/`next` loop (count bytes, or stop after N events);
  `maxSseEventBytes` already caps a single event, not the stream's total.
* **HTTP/3 buffered responses are capped per connection, not per request.** The h3
  driver enforces the cap while it buffers a whole response in C memory, and a shared
  h3 connection carries the cap of whichever client opened it. navi hands it the cap
  of the client that *owns* the connection, never an `sse()` view's, so a cap you set
  is never silently dropped by a stream that happened to open the connection. h3
  **streamed** reads (`stream()`, and so every SSE stream) are submitted with that
  connection-wide enforcement switched off, because the body is drained incrementally
  and nothing accumulates: they are capped navi-side per request instead, from the
  `maxResponseBytes` of the client that issued the read, and a breach raises the same
  `ResponseTooLargeError`. So an SSE stream reads unbounded over a connection your
  capped requests share, and your own `stream()` on it still stops at your cap.

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

### Read deadlines on a shared HTTP/2 connection

`timeouts.read` is a **connection-level** bound on an async client's shared HTTP/2
connection, not a per-stream one: it is the transport's socket read timeout, bound
once from the config that opened the connection and carried for its whole life. A
whole `read` with no inbound byte on the connection means the peer has gone dark, so
the reader exits, every in-flight stream on that connection fails with a replayable
error class, and the connection is retired and replaced. It is therefore navi's
dead-connection detector, and it cannot be two values at once -- which is why an
`sse()` stream, which must run with no read bound, rides a **separate SSE-only** h2
connection per origin instead of yours (all of one client's streams share that one;
see [Server-Sent Events](README.md#server-sent-events)).

Because that SSE connection has no read bound, its only liveness check is the HTTP/2
PING keepalive, `timeouts.h2KeepAlive` (20 s by default). An SSE stream therefore
always runs the keepalive on its connection: if you set `h2KeepAlive = 0`, your
requests' connections go without it but the SSE connection falls back to the 20 s
default, so a black-holed peer is still detected rather than leaving a zombie
connection that every later `sse()` on the client reconnects onto. Leave it on for
your requests too: with `0` and a hostile or flaky peer, a dead connection stays
pooled and every request dispatched on it burns its own read timeout before failing.

### HTTP/2 limits

The HTTP/2 bounds (HPACK decoded-list cap, 128 KiB CONTINUATION accumulation cap,
bounded flow-control window, server push disabled) are always on and not
configurable, because their safe values are not a tuning decision. They need no
hardening; they are described in [THREAT_MODEL.md](THREAT_MODEL.md#denial-of-service-can-a-hostile-server-exhaust-the-client).

### HTTP/3

```nim
config.http = {H1, H2}       # opt a client OUT of h3 in a -d:naviHttp3 build
config.http = {H1, H2, H3}   # the default of such a build, spelled out
```

The opt-in is the build flag, not the field: h3 exists only in a `-d:naviHttp3`
build, but `H3` is in that build's default `http` set, so every client that leaves
`config.http` alone negotiates h3. Assign an `http` set without `H3` to keep a
given client on h1/h2 (an empty set names no protocol and does not imply h3
either, unlike h2). h3 is reached per origin after Alt-Svc discovery and honors
the same `TlsConfig` as the other backends:
`caFile`/`caBundle`, the client credential (mTLS), `ciphers`/`cipherSuites`, the
chain and hostname check, and `pinnedKeys`/`verifyCallback` on the peer leaf before
the connection is used. QUIC is TLS 1.3 only, so a `maxVersion` below TLS 1.3 rules
h3 out: navi skips the advertised endpoint and stays on h2/h1 rather than failing
the request.

The QUIC handshake is bounded by `connectMs` (then `totalMs`, else 30 s), and an h3
endpoint whose handshake fails before anything is submitted is marked broken for a
doubling backoff window (RFC 7838 2.4), so a network that drops UDP/443 costs one
stalled handshake rather than one per request.

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
