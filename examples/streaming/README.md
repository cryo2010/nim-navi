# Streaming files

Two self-contained examples that stream a file in **constant memory** (one chunk
at a time, regardless of file size) and then **verify the bytes arrived intact**
by SHA-1'ing the round-trip against the original.

Each spins up a tiny local echo server ([`echo_server.nim`](echo_server.nim)) on
a background thread, so they run with no network, no external service, and no
setup — and the hash check makes the verification real, not cosmetic.

| Example | Streams via | Verifies |
|---------|-------------|----------|
| [`upload_file.nim`](upload_file.nim) | `request(POST, url, bodyStream = producer)` — a pull-based producer navi calls for each chunk until it returns `""` | the server echoes the upload; SHA-1(echo) == SHA-1(file) |
| [`download_file.nim`](download_file.nim) | `stream(GET, url, sink = ...)` — a sink navi hands each chunk as it arrives | SHA-1(downloaded file) == SHA-1(served payload) |

## Run

```sh
nimble exampleUpload        # or: nim c -r examples/streaming/upload_file.nim
nimble exampleDownload      # or: nim c -r examples/streaming/download_file.nim
```

Both generate a 3 MiB payload by default (large enough that the body outgrows the
HTTP/2 send window and is released incrementally). Pass a path to upload your own
file, or an output path to the download example:

```sh
nim c -r examples/streaming/upload_file.nim   /path/to/bigfile
nim c -r examples/streaming/download_file.nim /tmp/out.bin
```

## Notes

- **Constant memory.** The upload producer is pulled only once the previous chunk
  is on the wire, and the download sink writes each chunk straight to the file, so
  neither example ever holds the whole file in RAM.
- **Transport is automatic.** navi negotiates HTTP/2 over ALPN when the server
  offers it and falls back to HTTP/1.1 otherwise; the `bodyStream` / `sink` API is
  identical either way. (The local server here is HTTP/1.1; streaming upload over
  real HTTP/2 is covered by navi's interop tests.)
- **Decompression** happens on the download path before the sink sees the bytes,
  so a `Content-Encoding: gzip` response is written decoded.
