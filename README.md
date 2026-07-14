# navi

[![CI](https://github.com/cryo2010/nim-navi/actions/workflows/ci.yml/badge.svg)](https://github.com/cryo2010/nim-navi/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

An HTTP client for Nim, with a minimalist [ky](https://github.com/sindresorhus/ky)-inspired API. One request surface, three interchangeable engines: synchronous, `std/asyncdispatch`, or `chronos`. You pick the engine by which module you import.

```nim
import navi

let api = newNavi()
let res = api.get("https://example.com")
echo res.status, " ", res.text()
```

```nim
import navi/chronos   # or navi/asyncdispatch

proc main() {.async.} =
  let api = newNavi()
  let res = await api.get("https://example.com")
  echo res.status, " ", res.json()

waitFor main()
```

## Status

navi is under active development. What works today:

- **HTTP/1.1 and HTTP/2** over http and https, IPv4 and IPv6. h2 is native (own
  frames + HPACK + Huffman), ALPN-negotiated with automatic h1 fallback.
- **HTTP/2 multiplexing**: concurrent async requests to one origin share a
  single connection (transparent on asyncdispatch); a `parallel` batch API does
  the same on the sync backend.
- **Sync and async** from one API, via three mutually exclusive entry modules
- **TLS** on all three backends (OpenSSL for sync/asyncdispatch, BearSSL for chronos), with certificate verification on by default
- **Connection pooling / keep-alive**, with automatic retry on a stale pooled connection
- **Streaming** uploads (chunked) and downloads (chunk sink)
- **Retries** with capped exponential backoff, honoring `Retry-After`
- **Redirect following** with method rewrites and cross-origin `Authorization` stripping
- **Throw-on-non-2xx** by default (`HttpError`), opt-out available
- **Automatic decompression** (gzip/deflate) via the system zlib
- **Hooks**: `beforeRequest` / `afterResponse` / `beforeRetry`
- **Cookie jar**, **basic/bearer auth**, **proxy** (http absolute-URI and https CONNECT)
- **Body helpers**: `json=` and `form=`
- **Response helpers**: `.status`, `.headers`, `.text()`, `.bytes()`, `.json()`, `.ok`
- **Reusable clients** with default options and `.extend()`

HTTP/2 currently runs on the sync and asyncdispatch backends; chronos stays
http/1.1 (its bundled TLS exposes no client ALPN). Not built yet: **HTTP/3**.
See [Roadmap](#roadmap).

## Requirements

- Nim >= 2.2.10
- OpenSSL, for https. Compile your program with `-d:ssl`:
  ```
  nim c -r -d:ssl yourapp.nim
  ```
- `chronos` >= 4.0, only if you `import navi/chronos`. The sync and asyncdispatch backends have no third-party dependencies.

## Choosing a backend

Import exactly one entry module. Each exports the same `newNavi`/`get`/`post`/... surface; only the return type differs.

| Import | Style | Call site | Engine |
| --- | --- | --- | --- |
| `import navi` | sync | `let r = api.get(url)` | blocking |
| `import navi/asyncdispatch` | async | `let r = await api.get(url)` | `std/asyncdispatch` |
| `import navi/chronos` | async | `let r = await api.get(url)` | `chronos` |

The async entry modules re-export their event loop, so `await` and `waitFor` are available without a separate import. Importing more than one entry module is a compile-time error:

```
navi: import only one entry module, but both 'navi' and 'navi/asyncdispatch'
were imported. Choose one of navi (sync), navi/asyncdispatch, or navi/chronos.
```

## Usage

### Clients and options

```nim
let api = newNavi(NaviOptions(
  prefixUrl: "https://api.example.com",
  headers: initHeaders({"authorization": "Bearer ..."}),
))

# Relative targets resolve against prefixUrl.
let user = api.get("users/42").json()
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
res.text()            # body as string
res.bytes()           # body as seq[byte]
res.json()            # body parsed as JsonNode
```

`std/json` is re-exported, so `res.json()["field"].getBool()` works without importing it yourself.

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
  tls: TlsConfig(verify: true, caFile: "/path/to/ca-bundle.pem"),
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
  echo e.response.text()

# Opt out to handle status codes yourself:
let api = newNavi(NaviOptions(throwHttpErrors: some(false)))
```

### Retries and redirects

Idempotent requests that hit a transient failure (network error or 408/413/429/500/502/503/504) are retried with capped exponential backoff, honoring `Retry-After`. Redirects are followed by default.

```nim
let api = newNavi(NaviOptions(
  maxRetries: some(3),    # default 2
  maxRedirects: some(5),  # default 20; 0 disables
))
```

### Auth, cookies, and proxy

```nim
let api = newNavi(NaviOptions(
  auth: bearerAuth("token"),          # or basicAuth("user", "pass")
  proxy: some("http://proxy:8080"),   # else HTTP(S)_PROXY / NO_PROXY env
))
```

Each client keeps a cookie jar: cookies from `Set-Cookie` are stored and replayed on later requests to the same client (matched by domain, path, and Secure).

### Hooks

```nim
let api = newNavi(NaviOptions(hooks: Hooks(
  beforeRequest: @[proc(req: var Request) {.closure.} =
    req.headers["x-trace-id"] = newTraceId()],
  afterResponse: @[proc(req: Request, resp: var Response) {.closure.} =
    log(req.verb, resp.status)],
)))
```

### Decompression

Responses are decoded transparently: clients send `Accept-Encoding: gzip, deflate` and decode the body per `Content-Encoding` (via the system zlib). Disable with `decompress: some(false)`.

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

## Roadmap

HTTP/1.1 and HTTP/2 with the full policy layer (retries, redirects, hooks,
cookies, auth, proxy, decompression, body helpers, throw-on-non-2xx) are done.

- **HTTP/3**: reserved for when a usable QUIC stack lands on the chronos backend.

Known follow-ups: HTTP/2 on the chronos backend (its bundled TLS exposes no
client ALPN), send-side flow control for very large request bodies and the
per-connection concurrent-stream cap, brotli/zstd decompression,
streaming-response decompression, `caFile` on the chronos backend, and fuller
cookie expiry.

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

## License

MIT. Copyright Craig Younker.
