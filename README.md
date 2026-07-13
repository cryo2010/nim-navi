# navi

A sugary HTTP client for Nim, with a minimalist [ky](https://github.com/sindresorhus/ky)-inspired API. One request surface, three interchangeable engines: synchronous, `std/asyncdispatch`, or `chronos`. You pick the engine by which module you import.

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

- **HTTP/1.1** over http and https, IPv4 and IPv6
- **Sync and async** from one API, via three mutually exclusive entry modules
- **TLS** on all three backends (OpenSSL for sync/asyncdispatch, BearSSL for chronos), with certificate verification on by default
- **Connection pooling / keep-alive**, with automatic retry on a stale pooled connection
- **Streaming** uploads (chunked) and downloads (chunk sink)
- **Response helpers**: `.status`, `.headers`, `.text()`, `.bytes()`, `.json()`, `.ok`
- **Reusable clients** with default options and `.extend()`

Not built yet (planned): HTTP/2 and HTTP/3, retries with backoff, redirect following, hooks/interceptors, cookies, auth helpers, proxy support, automatic decompression, and throw-on-non-2xx. See [Roadmap](#roadmap).

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

navi is being built in phases. HTTP/1.1 with the full transport feature set (above) is done.

- **Policy layer**: retries with backoff, redirect following, hooks (beforeRequest / afterResponse / beforeRetry), cookie jar, basic/bearer auth, proxy support, automatic gzip/deflate/zstd decompression, `json`/`form`/`multipart` body helpers, and throw-on-non-2xx by default.
- **HTTP/2**: a native HPACK + framing implementation over the shared transport, negotiated via ALPN with HTTP/1.1 fallback.
- **HTTP/3**: reserved for when a usable QUIC stack lands on the chronos backend.

## Testing

```
nimble test
```

The suite covers the sans-io HTTP/1.1 parser deterministically and exercises all three backends end to end against in-process servers (keep-alive reuse, streaming, IPv6). Tests do not touch the network. The `examples/` directory holds manual smoke tests that do (for example, a real HTTPS GET).

## License

MIT. Copyright Craig Younker.
