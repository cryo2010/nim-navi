# navi

[![CI](https://github.com/cryo2010/nim-navi/actions/workflows/ci.yml/badge.svg)](https://github.com/cryo2010/nim-navi/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

An HTTP client for Nim, inspired by [ky](https://github.com/sindresorhus/ky). One API with four interchangeable backends for sync, async and JavaScript targets. You select a backend via import.

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
- [Choosing a backend](#choosing-a-backend)
  - [Capability matrix](#capability-matrix)
  - [The browser backend (`navi/js`)](#the-browser-backend-navijs)
- [Usage](#usage)
  - [Clients and options](#clients-and-options)
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
  - [Cookies](#cookies)
  - [Middleware](#middleware)
  - [Decompression](#decompression)
  - [HTTP/2](#http2)
  - [Keep-alive](#keep-alive)
  - [Streaming](#streaming)
  - [WebSocket](#websocket)
- [API](#api)
  - [NaviConfig](#naviconfig)
  - [Response](#response)
  - [HttpError](#httperror)
- [Thanks](#thanks)
- [License](#license)

## Features

- **HTTP/1.1 and HTTP/2** over http and https, IPv4 and IPv6. h2 is native (own
  frames + HPACK + Huffman), ALPN-negotiated with automatic h1 fallback.
- **HTTP/2 multiplexing**: concurrent async requests to one origin share a
  single HTTP/2 connection (automatic on the h2 async backend, asyncdispatch;
  chronos is HTTP/1.1 so it uses separate pooled connections). A `parallel` batch
  API does the same on the sync backend.
- **Sync and async** from one API, via mutually exclusive entry modules
- **Browser and Node** via a JavaScript backend (`import navi/js`) that runs on the runtime's `fetch`
- **TLS** on all three backends (OpenSSL for sync/asyncdispatch, BearSSL for chronos), with certificate verification on by default
- **Connection pooling / keep-alive**, with automatic retry on a stale pooled connection
- **Streaming** uploads (chunked) and downloads (chunk sink)
- **Retries** with capped exponential backoff, honoring `Retry-After`
- **Redirect following** with method rewrites and cross-origin `Authorization` stripping
- **Throw-on-non-2xx** by default (`HttpError`), opt-out available
- **Automatic decompression**: gzip/deflate (zlib), plus brotli and zstd when `libbrotlidec`/`libzstd` are present
- **Request timeouts** via the `timeout` option (`TimeoutError`)
- **Middleware**: onion-style `proc(ctx)` steps that modify, observe, or short-circuit a request
- **Cookie jar**, **basic/bearer/digest auth** (Digest: MD5 and SHA-256, RFC 7616), **proxy** (http absolute-URI and https CONNECT)
- **WebSockets** (RFC 6455) on all four backends, text and binary messages, fragmentation reassembly, and automatic ping/pong

## Install

```shell
nimble install navi
```

## Requirements

- Nim >= 2.2.10
- OpenSSL, for https. Compile your program with `-d:ssl`:
  ```
  nim c -r -d:ssl yourapp.nim
  ```
- `checksums` (MD5 and SHA-256 for Digest auth; the former `std/md5`, now maintained by nim-lang as a separate package). This is navi's only required Nim dependency.
- `chronos` >= 4.0, only if you `import navi/chronos`. Aside from `checksums`, the sync and asyncdispatch backends pull in no third-party Nim packages.
- `libbrotlidec` and `libzstd` (system libraries) are optional: needed only to decode `br`/`zstd` responses. They load lazily, so navi runs fine without them until a server actually sends those encodings.
- For `import navi/js`: nothing beyond Nim. Compile with `nim js` and run in a browser or on Node 18+ (which provides a global `fetch`); no `-d:ssl`, since the runtime handles TLS.

## Choosing a backend

Import exactly one entry module. Each exports the same `newNavi`/`get`/`post`/... surface; only the return type differs.

| Import | Style | Call site | Engine |
| --- | --- | --- | --- |
| `import navi` | sync | `let r = api.get(url)` | blocking |
| `import navi/asyncdispatch` | async | `let r = await api.get(url)` | `std/asyncdispatch` |
| `import navi/chronos` | async | `let r = await api.get(url)` | `chronos` |
| `import navi/js` | async | `let r = await api.get(url)` | `fetch` (browser / Node) |

The async entry modules re-export their event loop, so `await` and `waitFor` are available without a separate import. Importing more than one entry module is a compile-time error:

```
navi: import only one entry module, but both 'navi' and 'navi/asyncdispatch'
were imported. Choose one of navi (sync), navi/asyncdispatch, navi/chronos,
or navi/js.
```

**Cross-target:** under `nim js`, `import navi/asyncdispatch` and `import navi/chronos` transparently fall back to `navi/js` (neither `std/asyncdispatch` nor `chronos` has a JavaScript backend). So a **library** written on either -- request code, plus middleware as a plain `{.async.}` closure -- compiles for native *and* the browser/Node from one source. The one target-specific line is at the **application** entry point: `waitFor main()` natively vs `discard main()` under js (JS cannot block). js capability limits still apply (no streaming upload, `res.httpVersion` empty, TLS/proxy are the runtime's; see the matrix below).

### Capability matrix

Every backend shares the same request surface: HTTP/1.1, WebSocket (`ws`/`wss`),
TLS certificate verification, retries, redirects, middleware, throw-on-non-2xx,
streaming download, and response decompression all work everywhere. Where the
backends differ:

| Capability | `navi` (sync) | `navi/asyncdispatch` | `navi/chronos` | `navi/js` |
| --- | :---: | :---: | :---: | :---: |
| HTTP/2 | ✓ | ✓ | ✗ | runtime |
| Concurrent multiplexing | `parallel()` | transparent | ✗ | runtime |
| TLS engine | OpenSSL | OpenSSL | BearSSL | runtime |
| Custom CA (`caFile`) | ✓ | ✓ | ✓ | runtime |
| Client cert / mTLS | ✓ | ✓ | ✗ | ✗ |
| Max TLS version | system | system | 1.2 | runtime |
| Keep-alive / connection pool | ✓ | ✓ | ✓ | ✗ |
| Streaming upload | ✓ | ✓ | ✓ | ✗ |
| Cookie jar | ✓ | ✓ | ✓ | ✓ |
| Proxy configuration | ✓ | ✓ | ✓ | ✗ |

Legend: ✓ supported · ✗ not supported · **runtime** = provided by the
browser/Node platform rather than navi. (`navi/js` keeps its own cookie jar off
a browser, and defers to the browser store on one; see below.)

Two backends carry caveats:

- **chronos is HTTP/1.1 only.** Its bundled BearSSL exposes no client ALPN (so
  no h2 negotiation) and no client-certificate hook (so no mTLS), and negotiates
  up to TLS 1.2. Custom-CA verification via `caFile` does work.
- **`navi/js` runs on `fetch`/`WebSocket`,** so the platform owns connections,
  cookies, redirects, decompression, and TLS; navi keeps request building,
  retries, throw-on-non-2xx, and middleware. Its WebSocket wraps the native one, so
  custom handshake headers are ignored and the runtime handles ping/pong. On a
  runtime with no cookie store (Node, Deno, Bun, Workers), navi keeps its own
  cookie jar automatically so cookies persist across requests; in a browser the
  store handles that. Either way it needs no configuration.

### The browser backend (`navi/js`)

`import navi/js` compiles with `nim js` and runs over the runtime's `fetch`, so the platform handles TLS, HTTP-version negotiation, redirects, cookies, and decompression. navi keeps the request building, retries, throw-on-non-2xx, and (async) middleware. It has no connection pool, streaming uploads are unavailable, and `res.httpVersion` is empty because `fetch` does not expose it. Cookies persist automatically with no configuration: in a browser the store handles them, and on a runtime without one (Node, Deno, Bun, Workers) navi keeps its own jar. Middleware is async, as on the other async backends.

```nim
import navi/js

proc main() {.async.} =
  var cfg = newNaviConfig()
  cfg.prefixUrl = "https://api.example.com"
  let api = newNavi(cfg)
  let user = await api.get("users/42")
  echo user.data["name"].getStr

discard main()   # a browser or Node runs the returned Promise
```

## Usage

### Clients and options

Build a config with `newNaviConfig()`, which sets the safe defaults (verification
on, decompression on, 2 retries, 20 redirects); then set the fields you want and
pass it to `newNavi`. `NaviConfig` has `{.requiresInit.}`, so a bare or partial
`NaviConfig(...)` literal is a compile error; `newNaviConfig()` is the only way
to build one, which keeps the defaults from being silently zeroed.

```nim
var cfg = newNaviConfig()
cfg.prefixUrl = "https://api.example.com"
cfg.headers = initHeaders({"authorization": "Bearer ..."})
let api = newNavi(cfg)

# Relative targets resolve against prefixUrl.
let user = api.get("users/42").data
```

Derive a client that layers new defaults over an existing one. Build the override
with `newNaviConfig()` too; `extend` layers its identity fields (prefixUrl,
headers, http, auth, proxy) over the parent and inherits the rest:

```nim
var ovr = newNaviConfig()
ovr.headers = initHeaders({"x-api-key": "..."})
let authed = api.extend(ovr)
```

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
res.data              # body parsed as JsonNode (cached; raises on invalid)
```

`std/json` is re-exported, so `res.data["field"].getBool()` works without importing it yourself. `data` parses the body regardless of Content-Type, caches it, and raises `JsonParsingError` on invalid JSON.

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
var cfg = newNaviConfig()
cfg.tls.caFile = "/path/to/ca-bundle.pem"   # verify is already on
let api = newNavi(cfg)
```

`verify` defaults to on. `caFile` is honored by all three backends: sync and asyncdispatch through OpenSSL, and chronos through BearSSL (which otherwise verifies against its bundled Mozilla trust anchors). The chronos backend negotiates up to TLS 1.2 and does not support client certificates (mTLS).

#### Client certificates (mTLS)

On the OpenSSL backends (sync, asyncdispatch) navi can present a client certificate for mutual TLS, from several sources. Precedence is `pkcs12File`, then in-memory (`certPem`/`keyPem`), then the `certFile`/`keyFile` pair.

```nim
# PEM cert + key files (a single PEM may hold both; leave keyFile empty)
cfg.tls.certFile = "client.pem"
cfg.tls.keyFile  = "client.key"

# Encrypted PEM key
cfg.tls.certFile    = "client.pem"
cfg.tls.keyFile     = "client.enc.key"
cfg.tls.keyPassword = "secret"

# DER-encoded cert and key
cfg.tls.certFile = "client.crt"; cfg.tls.keyFile = "client.key"
cfg.tls.format   = tlsDer

# PKCS#12 / PFX bundle (keyPassword is the bundle password)
cfg.tls.pkcs12File  = "client.p12"
cfg.tls.keyPassword = "secret"

# In-memory PEM (e.g. from a secrets manager; no files touched)
cfg.tls.certPem = certString
cfg.tls.keyPem  = keyString
```

Key algorithms (RSA, ECDSA, Ed25519) work in any of these as long as OpenSSL supports them. In-memory PEM may carry an intermediate chain; a PKCS#12 bundle's extra chain certs are not installed (only its leaf and key), which is all a client needs to present. chronos (BearSSL) and js do not present client certificates.

### Errors

By default a non-2xx response raises `HttpError`, which carries the full response:

```nim
try:
  discard api.get("https://example.com/missing")
except HttpError as e:
  echo e.response.status      # e.g. 404
  echo e.response.body

# Opt out to handle status codes yourself:
var cfg = newNaviConfig()
cfg.throwHttpErrors = false
let api = newNavi(cfg)
```

### Retries, redirects, and timeouts

Idempotent requests that hit a transient failure (network error or 408/413/429/500/502/503/504) are retried with capped exponential backoff, honoring `Retry-After` (both the seconds and HTTP-date forms). Redirects are followed by default.

```nim
var cfg = newNaviConfig()
cfg.retry.limit = 3        # default 2; 0 disables retries
cfg.maxRedirects = 5       # default 20; 0 disables
cfg.timeout = 5000         # 5s; 0 (default) disables. Raises TimeoutError.
let api = newNavi(cfg)
```

The whole retry policy is configurable via `cfg.retry` (a `RetryPolicy`):

```nim
var cfg = newNaviConfig()
cfg.retry.limit = 5
cfg.retry.methods = {GET, HEAD}            # verbs eligible for retry
cfg.retry.statuses = @[429, 503]           # response statuses that trigger one
cfg.retry.backoffCap = 30_000              # cap the wait between attempts (ms)
```

`timeout` is per socket read on the sync backend and bounds the whole request (including retries) on the async backends.

### Query parameters

Pass `params` on any verb to append an url-encoded query string to the target (resolved against `prefixUrl` first). It accepts a seq/array of pairs, the map-like `@{}` (or bare `{}`) form, or a `Table` / `OrderedTable`:

```nim
let res = api.get("/search", params = @{"q": "http client", "page": "2"})
# GET /search?q=http+client&page=2
```

Pairs preserve order and allow duplicate keys (`@{"tag": "a", "tag": "b"}` gives `?tag=a&tag=b`), which a plain `Table` cannot; use pairs, `@{}`, or an `OrderedTable` when order or repeats matter.

### Cancellation

Pass a `CancelToken` to abort a request. On the async backends (asyncdispatch/chronos/js) `cancel()` aborts the in-flight request; on the sync backend it is cooperative (checked between attempts, so it cannot interrupt a socket read already blocked in a syscall -- use `timeout` for that). A cancelled request raises `RequestCancelledError`.

```nim
let tok = newCancelToken()
let fut = api.get("https://slow.example", cancel = tok)
# ... later, from a timer or another task:
tok.cancel()
```

### Response size limits

`maxResponseBytes` caps the response body; a larger response raises `ResponseTooLargeError`. Streaming enforces the cap incrementally (per chunk); buffered requests enforce it on the assembled body. On the native backends the cap counts decompressed bytes, so it also guards against decompression bombs.

```nim
var cfg = newNaviConfig()
cfg.maxResponseBytes = 10 * 1024 * 1024   # 10 MiB; 0 (default) is unlimited
let api = newNavi(cfg)
```

### Auth and proxy

```nim
var cfg = newNaviConfig()
cfg.auth = bearerAuth("token")      # or basicAuth("user", "pass")
cfg.proxy = "http://proxy:8080"     # else HTTP(S)_PROXY / NO_PROXY env
let api = newNavi(cfg)
```

### Cookies

Each client keeps a cookie jar automatically: cookies from `Set-Cookie` are stored and replayed on later requests to the same client (matched by domain, path, and Secure). There is nothing to configure.

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

var cfg = newNaviConfig()
cfg.middleware = @[trace("api")]
let api = newNavi(cfg)
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
`NaviContext`). Write middleware the same way on every async backend
(`navi/asyncdispatch`, `navi/chronos`, `navi/js`): a plain `{.async.}` closure
returning `Future[void]`. navi handles the per-backend details itself (chronos's
strict-raises obligation is discharged inside navi, not stamped into the public
type), so the same middleware source compiles on all of them.

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

On the sync backend (which can't have requests in flight at once), the same
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

HTTP/2 runs on the sync and asyncdispatch backends. To disable it and force
HTTP/1.1, set `http: {H1}` in `NaviConfig`.

### Keep-alive

Connection reuse is automatic. Each client keeps an idle-connection pool keyed by origin; responses that are self-delimited (content-length or chunked) and not marked `Connection: close` return their connection to the pool. A pooled connection that the server has since closed is transparently retried on a fresh connection.

### Streaming

Stream a download to a sink as bytes arrive (the returned `Response.body` stays empty):

```nim
var file = open("out.bin", fmWrite)
discard api.stream(GET, "https://example.com/large", sink = proc(data: openArray[byte]) =
  discard file.writeBuffer(unsafeAddr data[0], data.len))
file.close()
```

Stream an upload from a pull-based producer, sent as chunked transfer-encoding:

```nim
let parts = @["hello ", "streaming ", "world"]
var i = 0
discard api.request(POST, "https://example.com/upload", bodyStream = proc(): string =
  if i < parts.len:
    result = parts[i]
    inc i)
```

### WebSocket

Open a WebSocket with `websocket()`, then `send`, `receive`, and `close`. It works
on all four backends and accepts `ws://` / `wss://` (or `http` / `https`, which are
mapped). The calls block on the sync backend and are `await`ed on the async ones.

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

## API

### newNavi(config = newNaviConfig())

Create a client. `config` supplies the defaults applied to every request and
inherited via `extend`. Returns a `Navi`. Read it back (read-only) via
`client.config`; the config is fixed at construction, so build a fresh client or
`extend` to change it rather than mutating a live one.

### client.get / head / delete / options (target, headers = initHeaders(), params = @[], cancel = nil)
### client.post / put / patch (target, body = "", json = nil, form = @[], headers = initHeaders(), params = @[], cancel = nil)

Make a request with that verb. A relative `target` resolves against `prefixUrl`.
`json` and `form` encode the body and set a matching `Content-Type` unless you
supplied one. `params` appends an url-encoded query string and accepts pairs
(`@[...]` / `@{...}` / `{...}`) or a `Table` / `OrderedTable`;
`cancel: CancelToken` aborts the request (raising `RequestCancelledError`).
Returns a `Response` on the sync backend, or a `Future[Response]` on
`navi/asyncdispatch`, `navi/chronos`, and `navi/js`.

### client.request(verb, target, headers = initHeaders(), body = "", json = nil, form = @[], bodyStream = nil, params = @[], cancel = nil)

Any verb explicitly. `bodyStream: proc(): string` streams an upload as chunked
transfer-encoding (return `""` to end). Not available on `navi/js`.

### client.stream(verb, target, sink, headers = initHeaders(), params = @[], cancel = nil)

Deliver the response body to `sink: proc(data: openArray[byte])` as it arrives;
the returned `Response.body` stays empty.

### client.websocket(url, headers = initHeaders())

Open a WebSocket (RFC 6455). Accepts `ws://` / `wss://` (or `http` / `https`, mapped);
`wss` uses TLS. Returns a `WebSocket` on the sync backend, or a `Future[WebSocket]` on
the async ones. Then use `ws.send(data, binary = false)`, `ws.receive(): WsMessage`,
`ws.close(code = closeNormal, reason = "")`, and `ws.ping(data = "")` (native backends;
`navi/js` leaves ping/pong to the runtime). On `navi/js` it wraps the runtime's native
`WebSocket`, so `headers` are ignored.

### client.parallel(targets) (sync backend)

Fetch many URLs concurrently, multiplexed over one HTTP/2 connection when the
server supports it. Returns `seq[Response]`; non-2xx responses are returned, not
raised, so inspect `.ok` per result. On `navi/asyncdispatch`, awaiting concurrent
requests together with `all(@[...])` multiplexes them the same way.

### client.extend(options)

Derive a new client, layering `options` over this one: headers are merged, middleware
is appended, and other set fields override. The derived client gets its own
connection pool and cookie jar.

### NaviConfig

Build one with `newNaviConfig()`, which sets the defaults below, then assign the
fields you want. `NaviConfig` has `{.requiresInit.}`, so a bare or partial
`NaviConfig(...)` is a compile error; `newNaviConfig()` is the only builder.

- **prefixUrl** `string`: prepended to relative request targets.
- **headers** `Headers`: sent on every request (merged with per-call headers).
- **http** `set[HttpVersion]`: protocol preference. Default `{H1, H2}` negotiates
  h2 via ALPN with h1 fallback; set `{H1}` to force HTTP/1.1. Ignored by `navi/js`.
- **tls** `TlsConfig`: `verify` (`bool`, default `true`) and `caFile` (custom CA
  bundle, honored on all backends), plus `certFile`/`keyFile` for mTLS.
- **decompress** `bool`: decode gzip/deflate response bodies. Default on.
- **throwHttpErrors** `bool`: raise `HttpError` on a non-2xx response. Default on.
- **maxRedirects** `int`: redirects to follow. Default 20; 0 disables.
- **retry** `RetryPolicy`: `limit` (attempts, default 2, 0 disables), `methods`
  (verbs eligible, default the idempotent ones), `statuses` (response codes that
  trigger a retry), and `backoffCap` (ms ceiling on the wait between attempts).
- **maxResponseBytes** `int`: cap on the response body size. A larger response
  raises `ResponseTooLargeError`. 0 (default) is unlimited. Enforced incrementally
  when streaming; counts decompressed bytes on the native backends.
- **timeout** `int`: request timeout in milliseconds. 0 (default) disables it. A
  stalled request raises `TimeoutError`. The sync backend applies it per socket
  read; the async backends bound the whole request.
- **auth** `Auth`: `basicAuth(user, pass)`, `bearerAuth(token)`, or
  `digestAuth(user, pass)`. Basic/bearer set `Authorization` on every request;
  digest answers the server's 401 challenge (MD5 or SHA-256) on a one-shot retry.
- **proxy** `string`: proxy URL. `""` (default) falls back to `HTTP(S)_PROXY` /
  `NO_PROXY`.
- **middleware** `seq[NaviMiddleware]`: onion-style steps run in order, with
  `middleware[0]` outermost. Each is a closure `proc(ctx: NaviContext)` (sync) or
  `proc(ctx: NaviContext): Future[void]` (async): modify `ctx.req`, call
  `ctx.next()` to proceed, then inspect or replace `ctx.res`, or skip
  `next` to short-circuit without sending. A factory `proc(...): NaviMiddleware` can
  capture per-instance config. See [Middleware](#middleware).

### Response

- **status** `int`, e.g. 200.
- **ok** `bool`: true for a 2xx status.
- **reason** `string`: the status text.
- **httpVersion** `string`: `"HTTP/1.1"` or `"HTTP/2"` (empty on `navi/js`).
- **headers** `Headers`.
- **body** `string`: the raw body. A Nim string is a byte buffer, so this is also
  your bytes (`res.body.toOpenArrayByte(...)` for a view).
- **data** `JsonNode`: the body parsed as JSON, cached; raises `JsonParsingError`
  on invalid input.

### HttpError

Raised for a non-2xx response when `throwHttpErrors` is on. Carries the full
response as `.response`. Other request errors: `TimeoutError` (exceeded
`timeout`), `RequestCancelledError` (a `CancelToken` was cancelled), and
`ResponseTooLargeError` (body exceeded `maxResponseBytes`).

## Thanks

- [ky](https://github.com/sindresorhus/ky) by Sindre Sorhus, whose minimalist API shaped navi's request surface.
- [nghttp2](https://nghttp2.org/): the reference HTTP/2 server navi is tested against, and the source of the [HPACK test corpus](https://github.com/http2jp/hpack-test-case) (via [http2jp](https://github.com/http2jp)).
- [dart-archive/http2](https://github.com/dart-archive/http2) for the RFC 7541 Huffman decoding table (BSD-licensed).

## License

MIT
