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
import base64
import gzip
import hashlib
import os
import zlib

from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse

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


def _stamped(idx: int) -> bytes:
    # Index-stamp the first 8 bytes so the repeated blocks are distinct; a
    # whole-block reorder/duplication on the wire then changes the SHA-1.
    return idx.to_bytes(8, "big") + BLOCK[8:]


def _download_sha(size: int) -> str:
    if size not in _SHA_CACHE:
        h = hashlib.sha1()
        remaining, idx = size, 0
        while remaining > 0:
            n = min(len(BLOCK), remaining)
            blk = _stamped(idx)
            h.update(blk[:n] if n < len(BLOCK) else blk)
            remaining -= n
            idx += 1
        _SHA_CACHE[size] = h.hexdigest()
    return _SHA_CACHE[size]


@app.get("/download")
def download(size: int = 1 << 20) -> StreamingResponse:
    sha1 = _download_sha(size)         # computed once per size, then cached

    def gen():
        remaining, idx = size, 0
        while remaining > 0:
            n = min(len(BLOCK), remaining)
            blk = _stamped(idx)
            yield blk[:n] if n < len(BLOCK) else blk
            remaining -= n
            idx += 1

    return StreamingResponse(
        gen(),
        media_type="application/octet-stream",
        headers={"x-sha1": sha1},
    )


# --- coverage routes: status codes, redirects, auth, cookies -----------------

@app.api_route("/status/{code}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def status_route(code: int) -> Response:
    """Return exactly `code`, for the client's error-status handling."""
    body = b"" if code in (204, 304) or code < 200 else f"status-{code}".encode()
    return Response(status_code=code, content=body)


@app.get("/redirect/{n}")
async def redirect(n: int):
    """Redirect n times, then 200 'redirect-done' — exercises redirect following."""
    if n <= 0:
        return Response(content=b"redirect-done", media_type="text/plain")
    return RedirectResponse(url=f"/redirect/{n - 1}", status_code=302)


@app.get("/needs-auth")
async def needs_auth(request: Request) -> Response:
    """401 unless Basic stress:secret is presented."""
    auth = request.headers.get("authorization", "")
    if not auth.startswith("Basic "):
        return Response(status_code=401, headers={"www-authenticate": 'Basic realm="stress"'})
    try:
        userpass = base64.b64decode(auth[len("Basic "):]).decode()
    except Exception:
        userpass = ""
    if userpass != "stress:secret":
        return Response(status_code=403)
    return Response(content=b"authed")


@app.get("/setcookie")
async def setcookie() -> Response:
    r = Response(content=b"ok")
    r.set_cookie("stress-cookie", "abc123")
    return r


@app.get("/needs-cookie")
async def needs_cookie(request: Request) -> Response:
    """400 unless the client sent back the cookie /setcookie set (jar round-trip)."""
    if request.cookies.get("stress-cookie", "") != "abc123":
        return Response(status_code=400, content=b"missing cookie")
    return Response(content=b"cookie-ok")
