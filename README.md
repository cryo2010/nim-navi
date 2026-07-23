# navi

[![CI](https://github.com/cryo2010/nim-navi/actions/workflows/ci.yml/badge.svg)](https://github.com/cryo2010/nim-navi/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

An HTTP client for Nim, with a minimalist [ky](https://github.com/sindresorhus/ky)-inspired API. One request surface, four interchangeable backends: synchronous, `std/asyncdispatch`, `chronos`, or a JavaScript/`fetch` backend for the browser and Node. You pick one by which module you import.

```nim
import navi

let api = newNavi()
let res = api.get("https://example.com")
echo res.status, " ", res.body
```

```nim
import navi/chronos   # or navi/asyncdispatch

proc main() {.async.} =
  let api = newNavi()
  let res = await api.get("https://example.com")
  echo res.status, " ", res.data

waitFor main()
```

```nim
import navi/js   # compiles with `nim js`, runs over the runtime's fetch

proc main() {.async.} =
  let api = newNavi()
  let res = await api.get("https://example.com")
  echo res.status, " ", res.body

discard main()
```

## Install

navi is not yet in the Nimble registry. Install it from the repository:

```
nimble install https://github.com/cryo2010/nim-navi
```

Or pin it in your project's `.nimble`:

```nim
requires "https://github.com/cryo2010/nim-navi >= 0.1.0"
```

You still `import navi` (and `navi/asyncdispatch`, `navi/chronos`, `navi/js`)
regardless of how it was installed.

## Status

navi is under active development. What works today:

- **HTTP/1.1 and HTTP/2** over http and https, IPv4 and IPv6. h2 is native (own
  frames + HPACK + Huffman), ALPN-negotiated with automatic h1 fallback.
- **HTTP/2 multiplexing**: concurrent async requests to one origin share a
  single connection (transparent on asyncdispatch); a `parallel` batch API does
  the same on the sync backend.
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
- **Body helpers**: `json=`, `form=`, and `multipart=`
- **WebSocket** (RFC 6455) on all four backends (sync, asyncdispatch, chronos, and js): `websocket()` with `send`/`receive`/`close`, text and binary messages, fragmentation reassembly, and automatic ping/pong
- **Response helpers**: `.status`, `.headers`, `.body`, `.data`, `.ok`
- **Reusable clients** with default options and `.extend()`

HTTP/2 currently runs on the sync and asyncdispatch backends; chronos stays
http/1.1 (its bundled TLS exposes no client ALPN). The `navi/js` backend defers
the protocol to the browser/runtime. WebSocket runs on all four backends; the
`navi/js` one wraps the runtime's native `WebSocket` (so it ignores custom
handshake headers and the runtime handles ping/pong). Not built yet: **HTTP/3**.
See [Roadmap](#roadmap).

WebSocket in brief (sync; on `navi/asyncdispatch` the same calls are `await`ed):

```nim
let ws = api.websocket("wss://example.com/socket")
ws.send("hello")                       # text; use binary = true for bytes
let msg = ws.receive()                 # blocks; auto-answers pings, reassembles fragments
if msg.kind == wmText: echo msg.data
ws.close()
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
pass it to `newNavi`. Prefer this over a bare `NaviConfig(...)` literal, which
leaves every unmentioned field zeroed (including `verify`, i.e. off).

```nim
var cfg = newNaviConfig()
cfg.prefixUrl = "https://api.example.com"
cfg.headers = initHeaders({"authorization": "Bearer ..."})
let api = newNavi(cfg)

# Relative targets resolve against prefixUrl.
let user = api.get("users/42").data
```

Derive a client that layers new defaults over an existing one. `extend` takes a
sparse override: only the fields you set (prefixUrl, headers, http, auth, proxy)
layer over the parent, and everything else is inherited:

```nim
let authed = api.extend(NaviConfig(headers: initHeaders({"x-api-key": "..."})))
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

Idempotent requests that hit a transient failure (network error or 408/413/429/500/502/503/504) are retried with capped exponential backoff, honoring `Retry-After`. Redirects are followed by default.

```nim
var cfg = newNaviConfig()
cfg.maxRetries = 3     # default 2
cfg.maxRedirects = 5   # default 20; 0 disables
cfg.timeout = 5000     # 5s; 0 (default) disables. Raises TimeoutError.
let api = newNavi(cfg)
```

`timeout` is per socket read on the sync backend and bounds the whole request (including retries) on the async backends.

### Auth, cookies, and proxy

```nim
var cfg = newNaviConfig()
cfg.auth = bearerAuth("token")      # or basicAuth("user", "pass")
cfg.proxy = "http://proxy:8080"     # else HTTP(S)_PROXY / NO_PROXY env
let api = newNavi(cfg)
```

Each client keeps a cookie jar: cookies from `Set-Cookie` are stored and replayed on later requests to the same client (matched by domain, path, and Secure).

### Middleware

Middleware wraps a request onion-style. Each is a **`proc(ctx: NaviContext)`** that
reads and mutates a shared `NaviContext` (`ctx.req`, `ctx.res`, `ctx.client`),
calls `ctx.next()` to run the rest of the chain, and then inspects or replaces
`ctx.res`, or skips `next` to short-circuit without sending. `middleware[0]`
is the outermost layer; everything before the `ctx.next()` call is "before" and
everything after is "after".

Middleware are **`nimcall` procs, not closures**, so they cannot capture local
variables; shared state lives at module scope or on the `NaviContext`. Write them as
top-level procs:

```nim
proc trace(ctx: NaviContext) =                 # sync (import navi)
  ctx.req.headers["x-trace-id"] = newTraceId()   # before
  let t0 = epochTime()
  ctx.next()
  log(ctx.req.verb, ctx.res.status, epochTime() - t0)   # after

var cfg = newNaviConfig()
cfg.middleware = @[Middleware(trace)]
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

Because middleware cannot capture, cross-request state lives at module scope or
on the `NaviContext`. One backend caveat: on `navi/chronos` the middleware type
is `gcsafe`, so a middleware there may read and write value-type globals (an
`int` counter, say) but not globals that hold GC'd memory (`string`, `seq`,
`ref`, `Table`), not even a `let` one. Keep such state on the `NaviContext`, or
guard the access with `{.cast(gcsafe).}` if the program is single-threaded (navi
clients are). The other three backends have no such restriction.

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

## API

### newNavi(options = newNaviConfig())

Create a client. `options` are the defaults applied to every request and
inherited via `extend`. Returns a `Navi`.

### client.get / head / delete / options (target, headers = initHeaders())
### client.post / put / patch (target, body = "", json = nil, form = @[], headers = initHeaders())

Make a request with that verb. A relative `target` resolves against `prefixUrl`.
`json` and `form` encode the body and set a matching `Content-Type` unless you
supplied one. Returns a `Response` on the sync backend, or a `Future[Response]`
on `navi/asyncdispatch`, `navi/chronos`, and `navi/js`.

### client.request(verb, target, headers = initHeaders(), body = "", json = nil, form = @[], bodyStream = nil)

Any verb explicitly. `bodyStream: proc(): string` streams an upload as chunked
transfer-encoding (return `""` to end). Not available on `navi/js`.

### client.stream(verb, target, sink, headers = initHeaders())

Deliver the response body to `sink: proc(data: openArray[byte])` as it arrives;
the returned `Response.body` stays empty.

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
fields you want. A bare `NaviConfig()` leaves every field at its zero value
(e.g. `verify = false`), so prefer `newNaviConfig()`.

- **prefixUrl** `string`: prepended to relative request targets.
- **headers** `Headers`: sent on every request (merged with per-call headers).
- **http** `set[HttpVersion]`: protocol preference. Default `{H1, H2}` negotiates
  h2 via ALPN with h1 fallback; set `{H1}` to force HTTP/1.1. Ignored by `navi/js`.
- **tls** `TlsConfig`: `verify` (`bool`, default `true`) and `caFile` (custom CA
  bundle, honored on all backends), plus `certFile`/`keyFile` for mTLS.
- **decompress** `bool`: decode gzip/deflate response bodies. Default on.
- **throwHttpErrors** `bool`: raise `HttpError` on a non-2xx response. Default on.
- **maxRedirects** `int`: redirects to follow. Default 20; 0 disables.
- **maxRetries** `int`: retry attempts for transient failures. Default 2.
- **timeout** `int`: request timeout in milliseconds. 0 (default) disables it. A
  stalled request raises `TimeoutError`. The sync backend applies it per socket
  read; the async backends bound the whole request.
- **auth** `Auth`: `basicAuth(user, pass)`, `bearerAuth(token)`, or
  `digestAuth(user, pass)`. Basic/bearer set `Authorization` on every request;
  digest answers the server's 401 challenge (MD5 or SHA-256) on a one-shot retry.
- **proxy** `string`: proxy URL. `""` (default) falls back to `HTTP(S)_PROXY` /
  `NO_PROXY`.
- **middleware** `seq[Middleware]`: onion-style steps run in order, with
  `middleware[0]` outermost. Each is a `nimcall` `proc(ctx: NaviContext)` (sync) or
  `proc(ctx: NaviContext): Future[void]` (async): modify `ctx.req`, call
  `ctx.next()` to proceed, then inspect or replace `ctx.res`, or skip
  `next` to short-circuit without sending. See [Middleware](#middleware).

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
response as `.response`.

### Helpers

- **initHeaders(pairs)**: build a case-insensitive, order-preserving `Headers`.
- **basicAuth(user, pass)** / **bearerAuth(token)** / **digestAuth(user, pass)**: construct an `Auth`.

## Roadmap

HTTP/1.1 and HTTP/2 with the full policy layer (retries, redirects, middleware,
cookies, auth, proxy, decompression, body helpers, throw-on-non-2xx) are done.

- **HTTP/3**: reserved for when a usable QUIC stack lands on the chronos backend.

Known follow-ups on the chronos backend: HTTP/2 and client certificates (mTLS).
Its bundled BearSSL exposes no client ALPN (so no h2 negotiation) and no
client-certificate hook. Custom-CA verification via `caFile` works there today.

## Testing

```
nimble test
```

The suite runs the sans-io protocol cores deterministically (HTTP/1.1 parser;
HTTP/2 frames, HPACK, and Huffman against RFC 7541 vectors; the h2 connection
against simulated server frames) and exercises all three backends end to end
against in-process servers (keep-alive reuse, streaming, IPv6, redirects,
retries, cookies, auth, parallel). Tests do not touch the network. The
`examples/` directory holds manual smoke tests that do, including real HTTP/2
requests and concurrent multiplexing.

## Thanks

- [ky](https://github.com/sindresorhus/ky) by Sindre Sorhus, whose minimalist API shaped navi's request surface.
- The [nghttp2](https://nghttp2.org/) project: the reference server navi's HTTP/2 is tested against, and the source of the [HPACK test corpus](https://github.com/http2jp/hpack-test-case) (via [http2jp](https://github.com/http2jp)).
- [dart-archive/http2](https://github.com/dart-archive/http2) for the RFC 7541 Huffman decoding table (BSD-licensed).
- [chronos](https://github.com/status-im/nim-chronos) for the async backend of the same name.

## License

MIT
