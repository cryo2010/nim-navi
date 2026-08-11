"""Multi-protocol server for navi's leak-check matrix.

Run twice by run-server.sh: once plaintext (HTTP/1.1) and once over TLS (HTTP/1.1
and HTTP/2 via ALPN). One app serves every scenario endpoint:

  GET  /get           tiny body
  POST /upload        read the streamed request body, return its size
  GET  /download      stream ~256 KiB, uncompressed
  GET  /download-gz   the same bytes gzipped, with Content-Encoding: gzip
  GET  /sse           emit a handful of events then end
  WS   /ws            echo a few messages then close
"""
import gzip

from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import JSONResponse, Response, StreamingResponse

app = FastAPI()

PAYLOAD = bytes((i * 2654435761) & 0xFF for i in range(256 * 1024))  # ~256 KiB
GZIPPED = gzip.compress(PAYLOAD)
SSE_EVENTS = 6


@app.get("/get")
def get() -> Response:
    return Response(content=b"ok", media_type="text/plain")


@app.post("/upload")
async def upload(request: Request) -> JSONResponse:
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
    return JSONResponse({"size": size})


@app.get("/download")
def download() -> StreamingResponse:
    def gen():
        for i in range(0, len(PAYLOAD), 16384):
            yield PAYLOAD[i:i + 16384]
    return StreamingResponse(gen(), media_type="application/octet-stream")


@app.get("/download-gz")
def download_gz() -> Response:
    return Response(content=GZIPPED, media_type="application/octet-stream",
                    headers={"content-encoding": "gzip"})


@app.get("/sse")
async def sse() -> StreamingResponse:
    async def gen():
        for n in range(1, SSE_EVENTS + 1):
            yield f"id: {n}\nevent: tick\ndata: event-{n}\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")


@app.websocket("/ws")
async def ws(sock: WebSocket) -> None:
    # Echo every message until the client disconnects. The client drives closure
    # (the leak harness sends N, reads N, then closes); a server-initiated close
    # right after the last echo races the frame and some clients report it as an
    # abnormal 1006 close, dropping the final echo.
    await sock.accept()
    try:
        while True:
            msg = await sock.receive_text()
            await sock.send_text(msg)
    except Exception:
        pass
