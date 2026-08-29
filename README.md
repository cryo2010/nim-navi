# navi

[![CI](https://github.com/cryo2010/nim-navi/actions/workflows/ci.yml/badge.svg)](https://github.com/cryo2010/nim-navi/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A fast HTTP/1.1-3 client for Nim with TLS, streaming, SSE and WebSockets. One API, four interchangeable clients for sync, async, async (chronos) and JavaScript; pick one via import.

```nim
# Imports the synchronous client
import navi

let api = newNavi()
let res = api.get("https://example.com")
echo res.status, " ", res.body
```

```nim
# Imports the async client
import navi/asyncdispatch  # or navi/chronos

proc main() {.async.} =
  let api = newNavi()
  let res = await api.get("https://example.com")
  echo res.status, " ", res.data

waitFor main()
```

```nim
# Imports the javascript client
import navi/js   # compiles with `nim js`, runs over the runtime's fetch

proc main() {.async.} =
  let api = newNavi()
  let res = await api.get("https://example.com")
  echo res.status, " ", res.body

discard main()
```

## Contents

- [Features](#features)
- [Install](#install)
- [Requirements](#requirements)
- [Choosing a client](#choosing-a-client)
  - [Capability matrix](#capability-matrix)
  - [The browser client (`navi/js`)](#the-browser-client-navijs)
- [Usage](#usage)
  - [Creating a client](#creating-a-client)
  - [Configuration](#configuration)
  - [Requests](#requests)
  - [Responses](#responses)
  - [Headers](#headers)
  - [TLS](#tls)
  - [Errors](#errors)
  - [Retries, redirects, and timeouts](#retries-redirects-and-timeouts)
  - [Query parameters](#query-parameters)
  - [Cancellation](#cancellation)
  - [Response size limits](#response-size-limits)
  - [Auth and proxy](#auth-and-proxy)
  - [Unix domain sockets](#unix-domain-sockets)
  - [Cookies](#cookies)
  - [Middleware](#middleware)
    - [Batteries-included middleware](#batteries-included-middleware)
  - [Decompression](#decompression)
  - [HTTP/2](#http2)
  - [Keep-alive](#keep-alive)
  - [Happy Eyeballs](#happy-eyeballs)
  - [Streaming](#streaming)
  - [WebSocket](#websocket)
- [Security](#security)
- [Thanks](#thanks)
- [License](#license)

## Features

- **HTTP/1.1 and HTTP/2:** IPv4 and IPv6 and ALPN-negotiated with h1 fallback.
- **HTTP/2 multiplexing:** concurrent async requests on a single HTTP/2 connection
- **HTTP/3:** (QUIC), opt-in via `-d:naviHttp3`, automatic `Alt-Svc: h3` upgrades
- **Sync and async** from a common API
- **Browser and Node** via a JavaScript client
- **TLS** on all clients with certificate verification
- **Connection pooling / keep-alive** with automatic retry on a stale pooled connection
- **Happy Eyeballs:** (RFC 8305) address racing for fast connections
- **Streaming** uploads (chunked) and downloads (with backpressure)
- **Server-Sent Events** with transparent reconnection
- **Retries** with capped exponential backoff, honoring `Retry-After`
- **Redirect following** with method rewrites and cross-origin `Authorization` / `Proxy-Authorization` stripping
- **Throw-on-non-2xx** by default, opt-out available
- **Automatic decompression**: gzip, deflate, brotli and zstd; `res.text` charset decoding
- **Response trailers** surfaced (chunked HTTP/1.1 and HTTP/2)
- **Request timeouts** per-phase (connect / read / total)
- **Connection-pool sizing:** per-host and global idle caps, idle-timeout eviction
- **Middleware**: onion-style functions that modify, observe, or short-circuit requests
- **Cookie jar:** (RFC 6265) automatic and per-client; per-domain with expiration and `__Host-`/`__Secure-` prefixes
- **Basic/bearer/digest auth:** digest: MD5 and SHA-256, RFC 7616
- **Proxy:** http absolute-URI, https CONNECT (with Proxy-Authorization), and SOCKS5
- **Unix domain sockets** (native backends, POSIX): dial a socket path instead of TCP
- **TLS controls:** custom/in-memory CA, mTLS, version and cipher pinning, SPKI certificate pinning, and a custom verify callback
- **WebSockets** (RFC 6455) text and binary messages, fragmentation reassembly, and automatic ping/pong

## Install

```shell
nimble add navi
```

## Requirements

- Nim >= 2.2.10
- OpenSSL, for https. Compile your program with `-d:ssl`:
  ```
  nim c -r -d:ssl yourapp.nim
  ```
- `checksums` (MD5 and SHA-256 for Digest auth; the former `std/md5`, now maintained by nim-lang as a separate package). This is navi's only required Nim dependency.
- `chronos` >= 4.0, only if you `import navi/chronos`. The chronos client runs OpenSSL for TLS (like sync/asyncdispatch), so `https` needs a `-d:ssl` build. Aside from `checksums`, the sync and asyncdispatch clients pull in no third-party Nim packages.
- `libbrotlidec` and `libzstd` (system libraries) are optional: needed only to decode `br`/`zstd` responses. They load lazily, so navi runs fine without them until a server actually sends those encodings.
- HTTP/3 is opt-in via `-d:naviHttp3`, which needs **ngtcp2**, **nghttp3**, and **OpenSSL >= 3.5** (system libraries, located at build time via `pkg-config`) plus a C++ compiler. Without the flag none of these are required and h3 is unavailable; it applies to the sync, asyncdispatch, and chronos clients.
- **On Windows**, the DLL names decide which OpenSSL is loaded. Nim's default 64-bit
  list names only the (EOL) 1.1 pair, so navi targets 3.x via `-d:sslVersion=3-x64`
  -- already set for this repo in `nim.cfg`; set it in your own app too. Nim's
  Windows distribution bundles OpenSSL 1.1 and a **32-bit** `zlib1.dll`, neither of
  which serves an x64 build, so install 64-bit `libssl-3-x64`/`libcrypto-3-x64`,
  `zlib1`, and (optionally) `libbrotlidec`/`libzstd` -- from MSYS2 (`mingw-w64-x86_64-*`)
  or vcpkg -- and put them on `PATH`. `std/net` also finds CA certificates there only
  by locating a file named `cacert.pem` beside the executable or on `PATH`; there is no
  system trust-store fallback.
- For `import navi/js`: nothing beyond Nim. Compile with `nim js` and run in a browser or on Node 18+ (which provides a global `fetch`); no `-d:ssl`, since the runtime handles TLS.

## Choosing a client

Use one of the below import statements to get started.

| Import | Style | Call site | Engine |
| --- | --- | --- | --- |
| `import navi` | sync | `let r = api.get(url)` | blocking |
| `import navi/asyncdispatch` | async | `let r = await api.get(url)` | `std/asyncdispatch` |
| `import navi/chronos` | async | `let r = await api.get(url)` | `chronos` |
| `import navi/js` | async | `let r = await api.get(url)` | `fetch` (browser / Node) |

> [!NOTE]
> When compiled using `nim js`, the `navi/asyncdispatch` and `navi/chronos` clients transparently fall back to using the `navi/js` client.

### Capability matrix

Every client shares the same API, and the below table details where they differ.

| Capability | `navi` (sync) | `navi/asyncdispatch` | `navi/chronos` | `navi/js` |
| --- | :---: | :---: | :---: | :---: |
| HTTP/2 | ✓ | ✓ | ✓ | runtime |
| HTTP/3 | opt-in | opt-in | opt-in | runtime |
| Concurrent multiplexing | `parallel()` | transparent | transparent | runtime |
| TLS engine | OpenSSL | OpenSSL | OpenSSL | runtime |
| Custom CA (`caFile`) | ✓ | ✓ | ✓ | runtime |
| Client cert / mTLS | ✓ | ✓ | ✓ | ✗ |
| Max TLS version | system | system | system | runtime |
| Keep-alive / connection pool | ✓ | ✓ | ✓ | ✗ |
| Streaming upload | ✓ | ✓ | ✓ | buffered |
| Streaming download (pull) | ✓ | ✓ | ✓ | ✓ |
| Cookie jar | ✓ | ✓ | ✓ | ✓ |
| Proxy (HTTP + SOCKS5) | ✓ | ✓ | ✓ | ✗ |
| Unix domain sockets | ✓ | ✓ | ✓ | ✗ |

Legend: ✓ supported · ✗ not supported · **opt-in** = requires a `-d:naviHttp3`
build (ngtcp2 + nghttp3 + OpenSSL >= 3.5), reached transparently via Alt-Svc ·
**runtime** = provided by the
browser/Node platform rather than navi · **buffered** = `bodyStream` is accepted
but drained and sent as one body (`fetch` cannot reliably stream a request body) ·
**pull download** = `stream()` returns a headers-first handle consumed with
`each`/`drain`, which back-pressures the peer per chunk (see
[Streaming](#streaming)); `chunk` is a `string` on the native clients and
`seq[byte]` on `navi/js`. (`navi/js` keeps its own cookie jar off a browser, and
defers to the browser store on one; see below.)

Two clients carry caveats:

- **chronos runs OpenSSL over its transport, so `https` needs `-d:ssl`.** It runs
  OpenSSL (like the sync and asyncdispatch clients) rather than its bundled
  BearSSL, reaching full TLS parity: ALPN + HTTP/2, TLS 1.3, cipher selection,
  mTLS, and session resumption. TLS therefore links OpenSSL and requires a
  `-d:ssl` build (plaintext `http` does not). HTTP/3 (opt-in via `-d:naviHttp3`)
  is available here too, as on the other OpenSSL backends.
- **`navi/js` runs on `fetch`/`WebSocket`,** so the platform owns connections,
  cookies, redirects, decompression, and TLS; navi keeps request building,
  retries, throw-on-non-2xx, and middleware. Its WebSocket wraps the native one, so
  custom handshake headers are ignored and the runtime handles ping/pong. On a
  runtime with no cookie store (Node, Deno, Bun, Workers), navi keeps its own
  cookie jar automatically so cookies persist across requests; in a browser the
  store handles that. Either way it needs no configuration.

### The browser client (`navi/js`)

`import navi/js` compiles with `nim js` and runs over the runtime's `fetch`, so the platform handles TLS, HTTP-version negotiation, redirects, cookies, and decompression. navi keeps the request building, retries, throw-on-non-2xx, and (async) middleware. It has no connection pool, streaming uploads are unavailable, and `res.httpVersion` is empty because `fetch` does not expose it. Cookies persist automatically with no configuration: in a browser the store handles them, and on a runtime without one (Node, Deno, Bun, Workers) navi keeps its own jar. Middleware is async, as on the other async clients.

```nim
import navi/js

proc main() {.async.} =
  let api = newNavi()
  api.config.prefixUrl = "https://api.example.com"
  let user = await api.get("users/42")
  echo user.data["name"].getStr

discard main()   # a browser or Node runs the returned Promise
```

## Usage

### Creating a client

`newNavi()` creates a client with the default config, then you configure it in
place through `api.config`:

```nim
let api = newNavi()
api.config.prefixUrl = "https://api.example.com"
api.config.headers["authorization"] = "Bearer ..."
api.config.retry.limit = 5

# Relative targets resolve against prefixUrl.
let user = api.get("users/42").data
```

`api.config` is the client's **live** configuration: mutate it any time and the
change takes effect from the next request on (auth, headers, timeouts, retries,
redirects, decompression, middleware, and so on). This is the simplest way to
adjust a running client, e.g. refresh a token with
`api.config.headers["authorization"] = "Bearer " & newToken`.

Three fields are the exception: `tls`, `http`, and `proxy` are bound when
connections are opened, so changing them on a live client does not affect its
existing pooled connections. Set those before the first request, or build a new
client (or `extend`).

To start from a prepared config instead of mutating after construction, pass one
to `newNavi`:

```nim
var config = initNaviConfig()
config.timeouts.total = 30_000
let custom = newNavi(config)
```

Derive a client that layers new defaults over an existing one with `extend`. It
layers the override's identity fields (prefixUrl, headers, http, auth, proxy) over
the parent and inherits the rest:

```nim
var config = initNaviConfig()
config.headers["x-api-key"] = "..."
let authed = api.extend(config)
```

### Configuration

The fastest path is to mutate `api.config` on a live client (above). To build a
config up front, use `initNaviConfig()`, which sets the safe defaults
(verification on, decompression on, 2 retries, 20 redirects); then set the fields
you want and pass it to `newNavi`. `NaviConfig` has `{.requiresInit.}`, so a bare
or partial `NaviConfig(...)` literal is a compile error; `initNaviConfig()` is the
only way to build one, which keeps the defaults from being silently zeroed.

```nim
let api = newNavi()
api.config.prefixUrl = "https://api.example.com"
api.config.headers["authorization"] = "Bearer ..."

# Relative targets resolve against prefixUrl.
let user = api.get("users/42").data
```

Every field, and the default `initNaviConfig()` gives it:

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `auth` | `Auth` | `akNone` | Authorization for every request; build via `basicAuth` / `bearerAuth` / `digestAuth`. |
| `decompress` | `bool` | `true` | Decode `gzip`/`deflate`/`br`/`zstd` response bodies. |
| `headers` | `Headers` | empty | Headers sent on every request. |
| `http` | `set[HttpVersion]` | `{H1, H2}` | HTTP versions to negotiate; add `H3` (needs `-d:naviHttp3`). |
| `idleConnTimeout` | `int` | `0` | Evict and close an idle pooled connection after this many ms; `0` = no timeout. |
| `maxIdleConns` | `int` | `0` | Global cap on idle pooled connections; `0` = unlimited. |
| `maxIdleConnsPerHost` | `int` | `0` | Idle pooled connections kept per origin; `0` = default (8). |
| `maxRedirects` | `int` | `20` | Redirects to follow; `0` disables. |
| `maxResponseBytes` | `int` | `0` | Max response body size in bytes; `0` is unlimited. |
| `middleware` | `seq[NaviMiddleware]` | `@[]` | Onion-style steps wrapping each request. |
| `prefixUrl` | `string` | `""` | Base URL that relative request targets resolve against. |
| `proxy` | `string` | `""` | Proxy URL (`http://`, `https://`, or `socks5://`/`socks5h://`, with optional `user:pass@`); `""` falls back to `HTTP(S)_PROXY` / `ALL_PROXY` / `NO_PROXY`. |
| `retry.limit` | `int` | `2` | Retry attempts; `0` disables. |
| `retry.maxDelay` | `int` | `10000` | Upper bound on the wait between attempts, in ms. |
| `retry.methods` | `set[HttpVerb]` | `{GET, HEAD, PUT, DELETE, OPTIONS}` | Verbs eligible for retry. |
| `retry.statuses` | `seq[int]` | `@[408, 413, 429, 500, 502, 503, 504]` | Response statuses that trigger a retry. |
| `throwHttpErrors` | `bool` | `true` | Raise `HttpError` on a non-2xx response. |
| `unixSocket` | `string` | `""` | Dial this Unix socket path instead of TCP (POSIX; native backends); the URL host is used only for the Host header and TLS SNI. Bypasses proxies. |
| `timeouts.connect` | `int` | `0` | TCP connect + TLS handshake deadline (ms); `0` disables. |
| `timeouts.read` | `int` | `0` | Per-read idle deadline (ms); `0` disables. |
| `timeouts.total` | `int` | `0` | Whole-request deadline including retries/redirects (ms); `0` disables. |
| `tls.caBundle` | `string` | `""` | Extra trusted CA certificates as an in-memory PEM string (added alongside `caFile` / the system roots). |
| `tls.caFile` | `string` | `""` | Custom CA bundle path; `""` uses the system trust store. |
| `tls.certFile` | `string` | `""` | Client certificate file (PEM or DER) for mTLS. |
| `tls.certPem` | `string` | `""` | Client certificate as an in-memory PEM string. |
| `tls.cipherSuites` | `string` | `""` | TLS 1.3 ciphersuites (colon-separated); `""` = library default. |
| `tls.ciphers` | `string` | `""` | TLS <=1.2 cipher list (OpenSSL colon format); `""` = library default. |
| `tls.keyFile` | `string` | `""` | Private key file for `certFile`; `""` reuses `certFile`. |
| `tls.keyPem` | `string` | `""` | Private key as an in-memory PEM string; `""` reuses `certPem`. |
| `tls.maxVersion` | `TlsVersion` | `tlsDefault` | Highest TLS version to negotiate (`tlsDefault` = unset). |
| `tls.minVersion` | `TlsVersion` | `tlsDefault` | Lowest TLS version to negotiate (`tlsDefault` = unset). |
| `tls.password` | `string` | `""` | Passphrase for an encrypted key, or the PKCS#12 password. |
| `tls.pinnedKeys` | `seq[string]` | `@[]` | SPKI SHA-256 pins (base64, HPKP form); the peer public key must match one or the connection is rejected. |
| `tls.pkcs12File` | `string` | `""` | PKCS#12/PFX bundle (cert + key + chain); highest precedence. |
| `tls.resumeSessions` | `bool` | `true` | Reuse TLS sessions across connections (abbreviated handshake). |
| `tls.verify` | `bool` | `true` | Verify the certificate chain and hostname. |
| `tls.verifyCallback` | `proc` | `nil` | Hook run after the chain + hostname checks; receives the peer leaf cert (DER), returns whether to accept. |

### Requests

```nim
discard api.get("path", headers = initHeaders({"accept": "application/json"}))
discard api.post("path", body = """{"name":"navi"}""")
discard api.post("path", json = %*{"name": "navi"})          # sets application/json
discard api.post("path", form = @[("a", "1"), ("b", "2")])   # url-encoded
discard api.put("path", body = payload)
discard api.delete("path")
discard api.head("path")

# Any verb, explicitly:
discard api.request(POST, "path", body = payload)
```

### Responses

```nim
let res = api.get("https://example.com")
res.status            # int, e.g. 200
res.ok                # true for 2xx
res.headers.get("content-type")
res.body              # body as a string; a Nim string is a byte buffer, so this
                      # is also your bytes (res.body.toOpenArrayByte(...) for a view)
res.text              # body decoded to UTF-8 from its Content-Type charset (or BOM)
res.data              # body parsed as JsonNode (cached; raises on invalid)
res.trailers          # trailing header fields, if the response carried any
```

`std/json` is re-exported, so `res.data["field"].getBool()` works without importing it yourself. `data` parses the body regardless of Content-Type, caches it, and raises `JsonParsingError` on invalid JSON.

`res.body` is the raw bytes; `res.text` decodes them to UTF-8 using the `Content-Type` charset (or a leading BOM, else UTF-8), covering UTF-8, ISO-8859-1, Windows-1252, and UTF-16, so a non-UTF-8 response reads correctly. An unrecognized charset falls back to the raw bytes. `res.trailers` is a `Headers` holding the fields after the body (chunked HTTP/1.1 or an HTTP/2 trailing block, e.g. `grpc-status`); it is empty when there are none.

### Headers

`Headers` is case-insensitive and order-preserving.

```nim
var h = initHeaders({"accept": "application/json"})
h.add("x-trace", "abc")     # append (keeps duplicates)
h["accept"] = "text/plain"  # replace
h.get("ACCEPT")             # case-insensitive lookup
for (name, value) in h.pairs: discard
```

### TLS

```nim
var config = initNaviConfig()
config.tls.caFile = "/path/to/ca-bundle.pem"   # verify is already on
let api = newNavi(config)
```

`verify` defaults to on. `caFile` is honored by all three native clients, each through OpenSSL (chronos included; without a `caFile` they verify against the system trust store). All three negotiate modern TLS (up to the library's maximum, typically TLS 1.3) and support client certificates (mTLS).

#### Trusting a CA in memory

`caBundle` adds trusted CA certificates from an in-memory PEM string, alongside
the system roots (and any `caFile`). Handy when the CA is embedded in the binary
or fetched at runtime rather than on disk:

```nim
config.tls.caBundle = readFile("corp-root.pem")   # or an embedded const
```

#### Certificate pinning and a custom verify callback

For an extra check beyond chain + hostname verification, pin the peer's public
key or inspect the leaf certificate yourself. Both run after the standard checks
(and still run when `verify` is off, so you can replace verification entirely).

```nim
# SPKI SHA-256 pins (base64, the HPKP form). Compute one with:
#   openssl x509 -in cert.pem -pubkey -noout \
#     | openssl pkey -pubin -outform der | openssl dgst -sha256 -binary | base64
config.tls.pinnedKeys = @["r/pas0ue6zqoBH4vVvBxz7i+94EMJ3kAdyJWSd381TY="]

# Or a callback over the leaf certificate (DER); return false to reject.
config.tls.verifyCallback = proc(leafDer: string): bool =
  leafDer.len > 0
```

A non-matching pin, or a callback returning false, rejects the connection at
connect time. Both are honored on the three native clients (`navi/js` defers TLS
to the runtime).

#### Session resumption

navi caches TLS sessions per client and resumes them on later connections to the
same origin, so a reconnect skips the certificate exchange and the server's
signature (an abbreviated handshake). This is on by default and matters most for
workloads that open many short-lived connections (`Connection: close`, no pooling);
a pooled, kept-alive connection already amortizes the handshake. Disable it with:

```nim
config.tls.resumeSessions = false
```

Supported on all three native clients (sync, asyncdispatch, chronos), which now
share the same OpenSSL session cache; the sync client sees the largest gain.

#### TLS version pinning

Pin the acceptable TLS protocol range with `minVersion` / `maxVersion` (each a
`TlsVersion`: `tlsDefault`, `tls10`, `tls11`, `tls12`, `tls13`). `tlsDefault` (the
default) leaves that bound to the library.

```nim
config.tls.minVersion = tls12    # refuse anything below TLS 1.2
config.tls.maxVersion = tls13
```

Enforced on all three native clients (sync, asyncdispatch, chronos), which run
OpenSSL, so `tls13` is honored on chronos too. On `navi/js` the runtime controls
the TLS version, so these are ignored. A negotiation outside the pinned range
fails the handshake (`ValueError`). If the loaded OpenSSL/LibreSSL is too old to
support version pinning (e.g. the LibreSSL some macOS builds link), setting a
bound raises rather than silently ignoring it.

#### Cipher selection

Restrict the offered ciphers with `ciphers` (TLS ≤1.2) and `cipherSuites` (TLS 1.3)
— they use OpenSSL's two separate cipher APIs, so set whichever applies to the
versions you allow. Both are colon-separated OpenSSL names; "" (default) leaves the
library's selection.

```nim
config.tls.ciphers      = "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256"
config.tls.cipherSuites = "TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384"
```

Enforced on all three native clients (sync, asyncdispatch, chronos), which run
OpenSSL; a value with no cipher the peer accepts fails the handshake, and an
all-invalid list raises. On `navi/js` the runtime controls ciphers, so they are
ignored.

#### Client certificates (mTLS)

On the OpenSSL clients (sync, asyncdispatch) navi can present a client certificate for mutual TLS, from several sources. Precedence is `pkcs12File`, then in-memory (`certPem`/`keyPem`), then the `certFile`/`keyFile` pair.

```nim
# PEM cert + key files (a single PEM may hold both; leave keyFile empty)
config.tls.certFile = "client.pem"
config.tls.keyFile  = "client.key"

# Encrypted PEM key
config.tls.certFile = "client.pem"
config.tls.keyFile  = "client.enc.key"
config.tls.password = "secret"

# DER-encoded cert and key (encoding auto-detected from content)
config.tls.certFile = "client.crt"; config.tls.keyFile = "client.key"

# PKCS#12 / PFX bundle (password is the bundle password)
config.tls.pkcs12File = "client.p12"
config.tls.password   = "secret"

# In-memory PEM (e.g. from a secrets manager; no files touched)
config.tls.certPem = certString
config.tls.keyPem  = keyString
```

Key algorithms (RSA, ECDSA, Ed25519) work in any of these as long as OpenSSL supports them. In-memory PEM may carry an intermediate chain; a PKCS#12 bundle's extra chain certs are not installed (only its leaf and key), which is all a client needs to present. Supported on all three native clients (sync, asyncdispatch, chronos), which run OpenSSL; `navi/js` does not present client certificates.

### Errors

By default a non-2xx response raises `HttpError`, which carries the full response:

```nim
try:
  discard api.get("https://example.com/missing")
except HttpError as e:
  echo e.response.status      # e.g. 404
  echo e.response.body

# Opt out to handle status codes yourself:
let api = newNavi()
api.config.throwHttpErrors = false
```

### Retries, redirects, and timeouts

Idempotent requests that hit a transient failure (network error or 408/413/429/500/502/503/504) are retried with capped exponential backoff, honoring `Retry-After` (both the seconds and HTTP-date forms). Redirects are followed by default.

```nim
let api = newNavi()
api.config.retry.limit = 3        # default 2; 0 disables retries
api.config.maxRedirects = 5       # default 20; 0 disables
api.config.timeouts.total = 5000  # 5s; 0 (default) disables. Raises TimeoutError.
```

The whole retry policy is configurable via `config.retry` (a `RetryPolicy`):

```nim
let api = newNavi()
api.config.retry.limit = 5
api.config.retry.methods = {GET, HEAD}            # verbs eligible for retry
api.config.retry.statuses = @[429, 503]           # response statuses that trigger one
api.config.retry.maxDelay = 30_000                # cap the wait between attempts (ms)
```

`timeouts.total` bounds the whole request (raising `TimeoutError`): on the async clients (asyncdispatch/chronos/js) it covers all retries; on the sync client it is per attempt.

#### Per-phase timeouts

For finer control, set `config.timeouts` (a `Timeouts`) to bound individual phases instead of just the overall request. Each field is milliseconds; 0 (the default) disables that phase's limit.

```nim
let api = newNavi()
api.config.timeouts.connect = 2_000   # TCP connect + TLS handshake
api.config.timeouts.read    = 5_000   # stall waiting for a response chunk
api.config.timeouts.total   = 30_000  # whole request, including retries/redirects
```

- **connect** and **read** are enforced on the native clients (sync, asyncdispatch, chronos).
- **total** is enforced on all four clients.
- On `navi/js` only **total** applies (via `AbortSignal.timeout` — `fetch` hides the connect/read phases).

All raise `TimeoutError`. A timed-out phase on the async clients abandons the in-flight operation (asyncdispatch drains it in the background; chronos cancels it).

### Query parameters

Pass `params` on any verb to append an url-encoded query string to the target (resolved against `prefixUrl` first). It accepts a seq/array of pairs, the map-like `@{}` (or bare `{}`) form, or a `Table` / `OrderedTable`:

```nim
let res = api.get("/search", params = @{"q": "http client", "page": "2"})
# GET /search?q=http+client&page=2
```

Pairs preserve order and allow duplicate keys (`@{"tag": "a", "tag": "b"}` gives `?tag=a&tag=b`), which a plain `Table` cannot; use pairs, `@{}`, or an `OrderedTable` when order or repeats matter.

### Cancellation

Pass a `CancelToken` to abort a request. On the async clients (asyncdispatch/chronos/js) `cancel()` aborts the in-flight request; on the sync client it is cooperative (checked between attempts, so it cannot interrupt a socket read already blocked in a syscall -- use `timeouts.read` for that). A cancelled request raises `RequestCancelledError`.

```nim
let tok = newCancelToken()
let fut = api.get("https://slow.example", cancel = tok)
# ... later, from a timer or another task:
tok.cancel()
```

### Response size limits

`maxResponseBytes` caps the response body; a larger response raises `ResponseTooLargeError`. Streaming enforces the cap incrementally (per chunk); buffered requests enforce it on the assembled body. On the native clients the cap counts decompressed bytes, so it also guards against decompression bombs.

```nim
let api = newNavi()
api.config.maxResponseBytes = 10 * 1024 * 1024   # 10 MiB; 0 (default) is unlimited
```

### Auth and proxy

`auth` can be changed on a live client (it applies per request); `proxy` is bound
when connections open, so set it before the first request or build a new client.

```nim
var config = initNaviConfig()
config.proxy = "http://proxy:8080"     # else HTTP(S)_PROXY / ALL_PROXY / NO_PROXY env
let api = newNavi(config)

api.config.auth = bearerAuth("token")  # or basicAuth("user", "pass"); safe any time
```

The proxy URL scheme selects the kind: `http://` / `https://` for an HTTP proxy
(a `CONNECT` tunnel for https targets, absolute-URI for http), or `socks5://` /
`socks5h://` for a SOCKS5 proxy (a raw TCP tunnel for every target). A `user:pass@`
userinfo authenticates to the proxy: `Proxy-Authorization` on an HTTP `CONNECT`,
or the RFC 1929 username/password method on SOCKS5. Proxies are supported on the
three native clients (`navi/js` delegates connection setup to `fetch`).

```nim
config.proxy = "socks5://user:pass@127.0.0.1:1080"   # SOCKS5 with auth
```

### Unix domain sockets

Set `unixSocket` to dial a Unix socket path instead of TCP, for services that
listen on a socket file (the Docker daemon, systemd-activated services, local
sidecars). The URL still carries the host (used for the `Host` header and, over
https, the TLS SNI/verification name) and the path; only where the bytes go
changes. Proxies are bypassed. Supported on the native clients on POSIX
(`navi/js` and Windows raise a clear error).

```nim
var cfg = initNaviConfig()
cfg.unixSocket = "/var/run/docker.sock"
cfg.prefixUrl = "http://localhost"       # host is only for the Host header
let info = newNavi(cfg).get("/v1.45/info")
```

### Cookies

Each client keeps a cookie jar automatically: cookies from `Set-Cookie` are stored and replayed on later requests to the same client (matched by domain, path, and Secure). `__Host-` and `__Secure-` name-prefixed cookies are enforced per RFC 6265bis (rejected unless Secure over https, and for `__Host-` also host-only with `Path=/`). There is nothing to configure.

### Middleware

Middleware wraps a request onion-style. Each is a **`proc(ctx: NaviContext)`** that
reads and mutates a shared `NaviContext` (`ctx.req`, `ctx.res`, `ctx.client`),
calls `ctx.next()` to run the rest of the chain, and then inspects or replaces
`ctx.res`, or skips `next` to short-circuit without sending. `middleware[0]`
is the outermost layer; everything before the `ctx.next()` call is "before" and
everything after is "after".

Middleware are **closures**, so a factory can capture per-instance config and
return a configured step. Write a plain `proc(ctx: NaviContext)` for a fixed
step, or a factory `proc(...): NaviMiddleware` that closes over settings:

```nim
proc trace(prefix: string): NaviMiddleware =       # sync (import navi)
  result = proc(ctx: NaviContext) =
    ctx.req.headers["x-trace-id"] = newTraceId()               # before (captures prefix)
    let t0 = epochTime()
    ctx.next()
    log(prefix, ctx.req.verb, ctx.res.status, epochTime() - t0)  # after

let api = newNavi()
api.config.middleware = @[trace("api")]
```

Short-circuit by setting `ctx.res` and *not* calling `next` (a cache hit or
a mock), and nothing goes over the wire:

```nim
proc cache(ctx: NaviContext) =
  if ctx.req.url in store: ctx.res = store[ctx.req.url]  # no next()
  else:
    ctx.next()
    store[ctx.req.url] = ctx.res
```

On the async entries (`navi/asyncdispatch`, `navi/chronos`, `navi/js`) a
middleware is `proc(ctx: NaviContext): Future[void]` and you `await ctx.next()`:

```nim
proc refreshToken(ctx: NaviContext): Future[void] {.async.} =
  ctx.req.headers["authorization"] = "Bearer " & await fetchToken()
  await ctx.next()
```

Middleware wraps the whole request including the built-in retries and redirects,
so it runs once per call; to act on each retry, implement the retry loop in a
middleware. It does not apply to `websocket()`.

Per-instance config is captured by a middleware factory (or kept on the
`NaviContext`). Write middleware the same way on every async client
(`navi/asyncdispatch`, `navi/chronos`, `navi/js`): a plain `{.async.}` closure
returning `Future[void]`. navi handles the per-client details itself (chronos's
strict-raises obligation is discharged inside navi, not stamped into the public
type), so the same middleware source compiles on all of them.

#### Batteries-included middleware

Ready-made middleware ships under `mw`, imported to **mirror your client import**
(the middleware type is per-backend, so there is no single universal import):

| Your client | Middleware import |
| --- | --- |
| `import navi` | `import navi/mw` |
| `import navi/asyncdispatch` | `import navi/asyncdispatch/mw` |
| `import navi/chronos` | `import navi/chronos/mw` |
| `import navi/js` | `import navi/js/mw` |

The factories return `NaviMiddleware`; add them to `config.middleware`. `mw` is
also the call qualifier (`mw.cache`, `mw.rateLimit`, ...).

```nim
import navi
import navi/mw

let api = newNavi()
api.config.middleware = @[
  mw.rateLimit(perSec = 10),        # token-bucket throttle
  mw.cache()]                       # RFC 9111 response cache (in-memory)
```

- **`cache(store = newCacheStore())`** — serves fresh GET/HEAD responses from an
  in-memory store, revalidates stale ones with `If-None-Match`/`If-Modified-Since`
  (refreshing on `304`), and stores cacheable responses. Honors `Cache-Control`
  (`max-age`, `no-store`, `no-cache`, `private`), `Expires`, and `Vary`. Pass a
  shared `store` to reuse a cache across clients.
- **`rateLimit(perSec, burst = 0)`** — token bucket; over budget, a request waits
  its turn (async: awaits; sync: blocks). `burst` defaults to `ceil(perSec)`.
- **`concurrencyLimit(maxInFlight)`** — caps concurrent in-flight requests (native
  async backends; a no-op on the serial sync client, and omitted on `navi/js`
  where the runtime manages fetch concurrency).
- **`bearer(token)`**, **`basic(user, pass)`** — set the `Authorization` header.

Because middleware wraps `request()` only (not `stream()`/`sse()`), the cache
serves and stores buffered responses; streamed responses are not cached. Order
matters (outermost first): put `rateLimit` before `cache` so cache hits are not
counted against the budget.

### Decompression

Responses are decoded transparently: clients send `Accept-Encoding: gzip, deflate, br, zstd` and decode the body per `Content-Encoding`. gzip/deflate use the system zlib (present everywhere); `br` and `zstd` use `libbrotlidec` and `libzstd`, loaded lazily, so they are only required if a server actually sends those encodings. Disable all of it with `decompress: false`.

### HTTP/2

HTTP/2 is transparent: over https navi offers `h2` via ALPN and, if the server
agrees, speaks h2; otherwise it falls back to HTTP/1.1. Your code is unchanged;
check `res.httpVersion` if you care which was used.

```nim
let res = api.get("https://nghttp2.org/")
echo res.httpVersion   # "HTTP/2" or "HTTP/1.1"
```

Concurrent async requests to the same origin **multiplex over one connection**.
Just start them and await together (like `Promise.all`):

```nim
import navi/asyncdispatch

proc main() {.async.} =
  let api = newNavi()
  let results = await all(@[
    api.get("https://nghttp2.org/httpbin/get"),
    api.get("https://nghttp2.org/httpbin/ip"),
    api.get("https://nghttp2.org/httpbin/user-agent"),
  ])                       # three streams, one connection
  for r in results: echo r.status

waitFor main()
```

On the sync client (which can't have requests in flight at once), the same
multiplexing is available through a batch call:

```nim
import navi

let api = newNavi()
let results = api.parallel(@[
  "https://nghttp2.org/httpbin/get",
  "https://nghttp2.org/httpbin/ip",
])   # multiplexed over one h2 connection; each result still goes through the
     # policy layer (redirects, retries, decompression, cookies)
```

`parallel` collects every response (it does not raise on non-2xx); inspect
`.ok` per result.

HTTP/2 runs on the sync and asyncdispatch clients. To disable it and force
HTTP/1.1, set `http: {H1}` in `NaviConfig`.

### Keep-alive

Connection reuse is automatic. Each client keeps an idle-connection pool keyed by origin; responses that are self-delimited (content-length or chunked) and not marked `Connection: close` return their connection to the pool. A pooled connection that the server has since closed is transparently retried on a fresh connection.

Pool sizing is configurable: `maxIdleConnsPerHost` caps idle connections per origin (default 8), `maxIdleConns` caps them globally, and `idleConnTimeout` evicts and closes a connection that has sat idle too long (never handing out a stale one). All default to no limit beyond the per-host cap.

```nim
config.maxIdleConnsPerHost = 4
config.maxIdleConns = 100
config.idleConnTimeout = 90_000   # ms
```

### Happy Eyeballs

When a host resolves to several addresses (typical of dual-stack IPv4/IPv6 hosts and CDN pools), navi follows [RFC 8305](https://www.rfc-editor.org/rfc/rfc8305): it interleaves the address families and **races** the connection attempts, staggered by ~250ms, using whichever completes first. A slow or blackholed address no longer stalls the whole connect for the full timeout before the next is tried. If the TLS handshake then fails on the winning address, the remaining addresses are re-raced (handshake-aware fallback). This is automatic and needs no configuration, and works the same on all connecting backends (sync, asyncdispatch, chronos); `navi/js` delegates connection setup to the host runtime's `fetch`.

### Streaming

`stream()` returns a handle whose status and headers are available immediately,
while the body is pulled on demand. You inspect the headers, then consume the body
a chunk at a time with `each`. The connection is returned to the pool once the body
is fully read (or closed if you `close` the handle first).

```nim
var file = open("out.bin", fmWrite)
let res = api.stream(GET, "https://example.com/large")
if res.status == 200:
  res.each(chunk):
    discard file.writeBuffer(unsafeAddr chunk[0], chunk.len)
file.close()
```

On the **async** clients (`navi/asyncdispatch`, `navi/chronos`, `navi/js`) the
same code awaits the open, and the `each` body may await (the `await` is baked into
`each`, so there is none on the `each` line itself):

```nim
let res = await api.stream(GET, "https://example.com/large")
res.each(chunk):
  await sink.write(chunk)
```

`chunk` is an owned `string` on the native clients, moved out of navi's read
buffer with no copy; on `navi/js` it is `seq[byte]` (the bytes come from a JS
`Uint8Array`). Because `each`'s body runs as a proc, `break`/`continue`/`return`
cannot escape it: to stop early, don't call `each` and `close` the handle, or raise
from the body (which closes the connection and propagates). For an explicit sink
instead of the loop, use `res.drain(sink)`.

A handle you open but do not fully drain holds its connection, so `close` it if you
decide not to read the body. The native handles also close on destruction as a
backstop; on `navi/js` (no deterministic destructors) call `close` explicitly.

Because each chunk is awaited, a slow consumer applies **cooperative
backpressure**: over HTTP/2 the stream's receive window is only replenished once
the consumer has taken each chunk, so the peer stalls that one stream (without
starving the other multiplexed streams or blocking the connection reader) instead
of the body piling up in memory. Over HTTP/1.1 the awaited consumer pauses the read
loop, which back-pressures the peer through TCP. The size cap
(`maxResponseBytes`) is enforced incrementally on the streamed bytes.

Stream an upload from a pull-based producer, sent as chunked transfer-encoding:

```nim
let parts = @["hello ", "streaming ", "world"]
var i = 0
discard api.request(POST, "https://example.com/upload", bodyStream = proc(): string =
  if i < parts.len:
    result = parts[i]
    inc i)
```

### Server-Sent Events

`sse()` opens a `text/event-stream` and returns a handle consumed with a pull
`next()` or the `each` sugar. It **reconnects transparently** on a drop (resending
`Last-Event-ID` and honoring the server's `retry:`), and unlike the platform
`EventSource` it accepts any method, headers, and body.

```nim
let s = await api.sse("https://example.com/events")   # sync: no await
s.each(ev):
  echo ev.event, " #", ev.id, ": ", ev.data
  if ev.event == "done": break                        # a real loop, so break works
s.close()
```

Because `next()` is a pull (it returns `none` at end), `each` is a real loop, so
`break`/`continue`/`return` work inside it. Parameters: `verb`/`body`/`headers`/
`params` (POST-SSE, auth), `lastEventId` (resume a prior stream), `reconnect`
(default true), and `retryMs`/`maxRetryMs` (reconnect backoff). The initial response
must be `200 text/event-stream`, or `sse()` raises.

The stream runs with the size cap and read/total timeouts off (SSE is long-lived)
and shares the client's cookie jar. **Call `close()` when done** so the connection
is disposed (on the native clients it also joins the h2 mux reader). On `navi/js`
events go through `fetch`, so any method/headers work and chunks are decoded as
UTF-8 text.

### WebSocket

Open a WebSocket with `websocket()`, then `send`, `receive`, and `close`. It works
on all four clients and accepts `ws://` / `wss://` (or `http` / `https`, which are
mapped). The calls block on the sync client and are `await`ed on the async ones.

```nim
let ws = api.websocket("wss://example.com/socket")   # sync

ws.send("hello")                       # text; use binary = true to send bytes
let msg = ws.receive()                 # blocks until a whole message arrives
case msg.kind
of wmText, wmBinary: echo msg.data     # the payload
of wmClose:          echo "closed: ", msg.closeCode

ws.close()                             # sends a close frame (default code 1000)
```

On `navi/asyncdispatch`, `navi/chronos`, and `navi/js` the same calls are `await`ed:

```nim
let ws = await api.websocket("wss://example.com/socket")
await ws.send("hello")
let msg = await ws.receive()
await ws.close()
```

`receive` returns a `WsMessage`: `kind` is `wmText`, `wmBinary`, or `wmClose`; `data`
is the payload (or the reason on a close); `closeCode` is set on `wmClose`. navi
answers pings automatically and reassembles fragmented messages, so `receive` always
yields a whole message. Middleware does not apply to `websocket()`.

On `navi/js` the WebSocket wraps the runtime's native one, so custom handshake
`headers` are ignored and the runtime handles ping/pong; the send/receive/close
surface is otherwise the same.

## Security

navi is secure by default: certificate and hostname verification are on,
credentials never follow a request across an origin boundary, cookies are
re-scoped per host, and everything a hostile server can make the client do
(allocate, wait, retry) is bounded. See the below files for more.

- [SECURITY.md](SECURITY.md) - how to report a vulnerability.
- [THREAT_MODEL.md](THREAT_MODEL.md) - what navi defends against and how it is verified.
- [HARDENING.md](HARDENING.md) - configuring navi beyond the secure defaults.

## Thanks

- [ky](https://github.com/sindresorhus/ky) by Sindre Sorhus, whose minimalist API shaped navi's request surface.
- [nghttp2](https://nghttp2.org/): the reference HTTP/2 server navi is tested against, and the source of the [HPACK test corpus](https://github.com/http2jp/hpack-test-case) (via [http2jp](https://github.com/http2jp)).
- [dart-archive/http2](https://github.com/dart-archive/http2) for the RFC 7541 Huffman decoding table (BSD-licensed).

## License

MIT
