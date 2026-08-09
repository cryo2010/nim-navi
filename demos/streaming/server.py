"""FastAPI server for the navi streaming demo.

Served over HTTP/2 (Hypercorn + TLS, see run.sh) so navi's asyncdispatch backend
uses its h2 multiplexer -- the path that streams request bodies.

- GET  /download  streams a fixed payload back, with its SHA-1 in the x-sha1 header
- POST /upload    reads the (streamed) request body and returns its SHA-1 + size

The client hashes each transfer and checks it against ours, proving the streamed
bytes match the original end to end.
"""

import hashlib

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

app = FastAPI()


def make_payload(n: int) -> bytes:
    """Deterministic, varied bytes (a small LCG) -- no fixture file needed."""
    out = bytearray(n)
    x = 0x12345678
    for i in range(n):
        x = (x * 1664525 + 1013904223) & 0xFFFFFFFF
        out[i] = (x >> 24) & 0xFF
    return bytes(out)


PAYLOAD = make_payload(3 * 1024 * 1024)  # 3 MiB
PAYLOAD_SHA1 = hashlib.sha1(PAYLOAD).hexdigest()


@app.get("/download")
def download(size: int | None = None) -> Response:
    # Default streams the fixed 3 MiB payload (the demo). `?size=N` streams N
    # deterministic bytes instead, used by the concurrent-streaming interop test
    # to fan out many lighter transfers.
    content = PAYLOAD if size is None else make_payload(size)
    sha1 = PAYLOAD_SHA1 if size is None else hashlib.sha1(content).hexdigest()
    return Response(
        content=content,
        media_type="application/octet-stream",
        headers={"x-sha1": sha1},
    )


@app.post("/upload")
async def upload(request: Request) -> JSONResponse:
    digest = hashlib.sha1()
    size = 0
    async for chunk in request.stream():  # consume the upload incrementally
        digest.update(chunk)
        size += len(chunk)
    return JSONResponse({"sha1": digest.hexdigest(), "size": size})
