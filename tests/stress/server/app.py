"""Single FastAPI app for the navi stress workloads, served by hypercorn over TLS
(h1/h2) or behind Caddy (h3). Every instance serves every route; a workload hits
the route it needs. Endpoints:

  ANY /echo            request/response soak: echoes method + x-stress header,
                       decodes a Content-Encoding request body, and re-encodes the
                       response per x-want-encoding (gzip/deflate/br/zstd).
  WS  /ws              WebSocket echo (text + binary).
  GET /events          SSE: numbered events, dropped every K to force reconnect
                       (honors Last-Event-ID) -- runs indefinitely across reconnects.
  POST /upload         hash the streamed body incrementally, return {sha1,size}.
  GET  /download?size= stream `size` bytes (a fixed 1 MiB block repeated) with the
                       payload's SHA-1 in x-sha1. Constant memory on both sides.
"""
import gzip
import hashlib
import os
import zlib

from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, StreamingResponse

try:
    import brotli  # optional (RESP_COMPRESSION=br)
except ImportError:
    brotli = None
try:
    import zstandard  # optional (RESP_COMPRESSION=zstd)
except ImportError:
    zstandard = None

app = FastAPI()

BLOCK = os.urandom(1 << 20)            # 1 MiB, fixed per process; non-compressible
_SHA_CACHE: dict[int, str] = {}        # size -> sha1 of BLOCK repeated to `size`


def encode(data: bytes, how: str) -> bytes:
    if how == "gzip":
        return gzip.compress(data)
    if how == "deflate":
        return zlib.compress(data)     # zlib-wrapped; navi's inflate auto-detects
    if how == "br" and brotli:
        return brotli.compress(data)
    if how == "zstd" and zstandard:
        return zstandard.ZstdCompressor().compress(data)
    return data                        # unknown/unavailable: send plain


def decode(data: bytes, how: str) -> bytes:
    if not how:
        return data
    if how == "gzip":
        return gzip.decompress(data)
    if how == "deflate":
        return zlib.decompress(data)
    return data


@app.api_route("/echo", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])
async def echo(request: Request) -> Response:
    raw = await request.body()
    body = decode(raw, request.headers.get("content-encoding", ""))
    headers = {
        "x-echo-method": request.method,
        "x-echo-stress": request.headers.get("x-stress", ""),
    }
    if request.method == "HEAD":
        return Response(status_code=200, headers=headers)
    want = request.headers.get("x-want-encoding", "")
    out = encode(body, want) if want else body
    if want and out is not body:
        headers["content-encoding"] = want
    media = request.headers.get("content-type", "application/octet-stream")
    return Response(content=out, media_type=media, headers=headers)


@app.websocket("/ws")
async def ws(sock: WebSocket) -> None:
    await sock.accept()
    try:
        while True:
            msg = await sock.receive()
            if msg["type"] == "websocket.disconnect":
                break
            if msg.get("text") is not None:
                await sock.send_text(msg["text"])
            elif msg.get("bytes") is not None:
                await sock.send_bytes(msg["bytes"])
    except WebSocketDisconnect:
        pass


@app.get("/events")
async def events(request: Request) -> StreamingResponse:
    last = request.headers.get("last-event-id")
    start = int(last) if (last and last.isdigit()) else 0
    # Stream continuously so an SSE soak is mostly steady-state consumption; drop
    # only every SSE_DROP_EVERY events (default 1000) so reconnect + Last-Event-ID
    # resume is still exercised, without the churn (and error noise) of dropping
    # after a handful of events.
    drop_every = int(os.environ.get("NAVI_SSE_DROP_EVERY", "1000"))

    async def gen():
        eid = start + 1
        sent = 0
        while True:
            yield f"id: {eid}\ndata: event-{eid}\n\n"
            eid += 1
            sent += 1
            if drop_every > 0 and sent >= drop_every:
                return                 # occasional drop; client reconnects + resumes

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.post("/upload")
async def upload(request: Request) -> JSONResponse:
    digest = hashlib.sha1()
    size = 0
    async for chunk in request.stream():   # consume incrementally: constant memory
        digest.update(chunk)
        size += len(chunk)
    return JSONResponse({"sha1": digest.hexdigest(), "size": size})


def _download_sha(size: int) -> str:
    if size not in _SHA_CACHE:
        h = hashlib.sha1()
        remaining = size
        while remaining > 0:
            n = min(len(BLOCK), remaining)
            h.update(BLOCK[:n] if n < len(BLOCK) else BLOCK)
            remaining -= n
        _SHA_CACHE[size] = h.hexdigest()
    return _SHA_CACHE[size]


@app.get("/download")
def download(size: int = 1 << 20) -> StreamingResponse:
    sha1 = _download_sha(size)         # computed once per size, then cached

    def gen():
        remaining = size
        while remaining > 0:
            n = min(len(BLOCK), remaining)
            yield BLOCK[:n] if n < len(BLOCK) else BLOCK
            remaining -= n

    return StreamingResponse(
        gen(),
        media_type="application/octet-stream",
        headers={"x-sha1": sha1},
    )
