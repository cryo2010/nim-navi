# Streaming demo (navi/asyncdispatch vs a FastAPI HTTP/2 server)

A Dockerized round-trip that streams a file **up** and **down** over **HTTP/2**
and verifies the bytes arrived intact by comparing SHA-1 hashes at both ends.

- **Server** — FastAPI served by **Hypercorn over TLS + HTTP/2** ([`server.py`](server.py)):
  `GET /download` streams a 3 MiB payload back (with its SHA-1 in `x-sha1`);
  `POST /upload` consumes the streamed body and returns its SHA-1 and size.
- **Client** — the **navi/asyncdispatch** backend, whose h2 multiplexer streams
  the request body chunk by chunk:
  - [`upload.nim`](upload.nim) streams a file via `bodyStream` and checks the
    server's SHA-1 against the file's own.
  - [`download.nim`](download.nim) streams the response to disk via `stream()`/`each`
    and checks the file's SHA-1 against the `x-sha1` header.

Each check `doAssert`s, so a corrupted transfer fails the run.

## Run

```sh
nimble demoStreaming
# or:
cd demos/streaming && docker compose up --build --abort-on-container-exit --exit-code-from client
```

Expected output:

```
download: 3145728 bytes over HTTP/2
  verified: streamed download matches the original -- ok
upload: 3145728 bytes over HTTP/2
  verified: streamed upload matches the original -- ok
```

## Notes

- **HTTP/2 on purpose.** The server speaks h2 (Hypercorn + TLS), so navi uses its
  async h2 multiplexer — the path that streams request bodies over HTTP/2. The
  3 MiB payload is larger than the flow-control window, so the upload is released
  incrementally as the server sends `WINDOW_UPDATE`s.
- **Constant memory.** The upload producer is pulled one chunk at a time and the
  download `each` loop writes each chunk straight to disk; neither side holds the
  whole file in RAM.
- **TLS.** The server uses a throwaway self-signed cert and the client sets
  `tls.verify = false` — the demo verifies *body integrity*, not the certificate.
  Point navi at a real endpoint with a trusted chain to verify that too.
