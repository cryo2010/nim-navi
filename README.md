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
- **Hooks**: `beforeRequest` / `afterResponse` / `beforeRetry`
- **Cookie jar**, **basic/bearer auth**, **proxy** (http absolute-URI and https CONNECT)
- **Body helpers**: `json=` and `form=`
- **Response helpers**: `.status`, `.headers`, `.body`, `.data`, `.ok`
- **Reusable clients** with default options and `.extend()`

HTTP/2 currently runs on the sync and asyncdispatch backends; chronos stays
http/1.1 (its bundled TLS exposes no client ALPN). The `navi/js` backend defers
the protocol to the browser/runtime. Not built yet: **HTTP/3**.
See [Roadmap](#roadmap).

## Requirements

- Nim >= 2.2.10
- OpenSSL, for https. Compile your program with `-d:ssl`:
  ```
  nim c -r -d:ssl yourapp.nim
  ```
- `chronos` >= 4.0, only if you `import navi/chronos`. The sync and asyncdispatch backends have no third-party Nim dependencies.
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

### The browser backend (`navi/js`)

`import navi/js` compiles with `nim js` and runs over the runtime's `fetch`, so the platform handles TLS, HTTP-version negotiation, redirects, cookies, and decompression. navi keeps the request building, retries, throw-on-non-2xx, and (async) hooks. It has no connection pool or cookie jar (the browser owns both), streaming uploads are unavailable, and `res.httpVersion` is empty because `fetch` does not expose it. Hooks are async, as on the other async backends.

```nim
import navi/js

proc main() {.async.} =
  let api = newNavi(NaviOptions(prefixUrl: "https://api.example.com"))
  let user = await api.get("users/42")
  echo user.data["name"].getStr

discard main()   # a browser or Node runs the returned Promise
```

## Usage

### Clients and options

```nim
let api = newNavi(NaviOptions(
  prefixUrl: "https://api.example.com",
  headers: initHeaders({"authorization": "Bearer ..."}),
))

# Relative targets resolve against prefixUrl.
let user = api.get("users/42").data
```

Derive a client that layers new defaults over an existing one:

```nim
let authed = api.extend(NaviOptions(headers: initHeaders({"x-api-key": "..."})))
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
let api = newNavi(NaviOptions(
  tls: TlsConfig(verify: some(true), caFile: "/path/to/ca-bundle.pem"),
))
```

`verify` defaults to on. `caFile` is honored by the sync and asyncdispatch backends; the chronos backend verifies against its bundled Mozilla trust anchors and negotiates up to TLS 1.2.

### Errors

By default a non-2xx response raises `HttpError`, which carries the full response:

```nim
try:
  discard api.get("https://example.com/missing")
except HttpError as e:
  echo e.response.status      # e.g. 404
  echo e.response.body

# Opt out to handle status codes yourself:
let api = newNavi(NaviOptions(throwHttpErrors: some(false)))
```

### Retries, redirects, and timeouts

Idempotent requests that hit a transient failure (network error or 408/413/429/500/502/503/504) are retried with capped exponential backoff, honoring `Retry-After`. Redirects are followed by default.

```nim
let api = newNavi(NaviOptions(
  maxRetries: some(3),    # default 2
  maxRedirects: some(5),  # default 20; 0 disables
  timeout: some(5000),    # 5s; unset (default) disables. Raises TimeoutError.
))
```

`timeout` is per socket read on the sync backend and bounds the whole request (including retries) on the async backends.

### Auth, cookies, and proxy

```nim
let api = newNavi(NaviOptions(
  auth: bearerAuth("token"),          # or basicAuth("user", "pass")
  proxy: some("http://proxy:8080"),   # else HTTP(S)_PROXY / NO_PROXY env
))
```

Each client keeps a cookie jar: cookies from `Set-Cookie` are stored and replayed on later requests to the same client (matched by domain, path, and Secure).

### Hooks

Hooks receive a mutable `HookCtx` (`ctx.request`, `ctx.response`, `ctx.attempt`):

```nim
let api = newNavi(NaviOptions(hooks: Hooks(
  beforeRequest: @[proc(ctx: HookCtx) {.closure.} =
    ctx.request.headers["x-trace-id"] = newTraceId()],
  afterResponse: @[proc(ctx: HookCtx) {.closure.} =
    log(ctx.request.verb, ctx.response.status)],
)))
```

On the async entries (`navi/asyncdispatch`, `navi/chronos`) a hook may be async
and `await` inside it; the type is `proc(ctx: HookCtx): Future[void]`:

```nim
let refreshToken: Hook = proc(ctx: HookCtx): Future[void] {.async.} =
  ctx.request.headers["authorization"] = "Bearer " & await fetchToken()
```

### Decompression

Responses are decoded transparently: clients send `Accept-Encoding: gzip, deflate, br, zstd` and decode the body per `Content-Encoding`. gzip/deflate use the system zlib (present everywhere); `br` and `zstd` use `libbrotlidec` and `libzstd`, loaded lazily, so they are only required if a server actually sends those encodings. Disable all of it with `decompress: some(false)`.

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
     # policy layer (redirects, retries, decompression, cookies, hooks)
```

`parallel` collects every response (it does not raise on non-2xx); inspect
`.ok` per result.

HTTP/2 runs on the sync and asyncdispatch backends. To disable it and force
HTTP/1.1, set `http: {H1}` in `NaviOptions`.

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

### newNavi(options = NaviOptions())

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

Derive a new client, layering `options` over this one: headers are merged, hooks
are appended, and other set fields override. The derived client gets its own
connection pool and cookie jar.

### NaviOptions

Every field is optional.

- **prefixUrl** `string`: prepended to relative request targets.
- **headers** `Headers`: sent on every request (merged with per-call headers).
- **http** `set[HttpVersion]`: protocol preference. Default `{H1, H2}` negotiates
  h2 via ALPN with h1 fallback; set `{H1}` to force HTTP/1.1. Ignored by `navi/js`.
- **tls** `TlsConfig`: `verify` (default `true`) and `caFile` (custom CA bundle,
  honored by the sync and asyncdispatch backends).
- **decompress** `Option[bool]`: decode gzip/deflate response bodies. Default on.
- **throwHttpErrors** `Option[bool]`: raise `HttpError` on a non-2xx response.
  Default on.
- **maxRedirects** `Option[int]`: redirects to follow. Default 20; 0 disables.
- **maxRetries** `Option[int]`: retry attempts for transient failures. Default 2.
- **timeout** `Option[int]`: request timeout in milliseconds. Unset (default)
  disables it. A stalled request raises `TimeoutError`. The sync backend applies
  it per socket read; the async backends bound the whole request.
- **auth** `Auth`: `basicAuth(user, pass)` or `bearerAuth(token)`; sets
  `Authorization` on every request.
- **proxy** `Option[string]`: proxy URL. Unset falls back to `HTTP(S)_PROXY` /
  `NO_PROXY`.
- **hooks** `Hooks`: lifecycle callbacks, each a `seq`:
  - **beforeRequest**: mutate `ctx.request` before it is sent.
  - **afterResponse**: read or replace `ctx.response`.
  - **beforeRetry**: runs before a retry; see `ctx.attempt`.

  A hook receives a mutable `HookCtx` (`ctx.request`, `ctx.response`,
  `ctx.attempt`). On the sync backend the type is `proc(ctx: HookCtx)`; on the
  async backends it is `proc(ctx: HookCtx): Future[void]` and may `await`.

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
- **basicAuth(user, pass)** / **bearerAuth(token)**: construct an `Auth`.

## Roadmap

HTTP/1.1 and HTTP/2 with the full policy layer (retries, redirects, hooks,
cookies, auth, proxy, decompression, body helpers, throw-on-non-2xx) are done.

- **HTTP/3**: reserved for when a usable QUIC stack lands on the chronos backend.

Known follow-ups: HTTP/2 on the chronos backend and `caFile`/client certificates
there (its bundled BearSSL TLS exposes no client ALPN, custom-CA hook, or client
cert). Streamed HTTP/1.1 response bodies are now decompressed incrementally as
they arrive.

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
