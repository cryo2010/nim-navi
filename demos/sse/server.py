"""FastAPI Server-Sent Events demo for navi.

Streams a short series of "tick" events over HTTP/2, dropping the connection once
midway. navi reconnects transparently and resumes from Last-Event-ID, so the
client receives every tick in order without noticing the drop. The two "connection
opened" lines below are the reconnection made visible.
"""
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

app = FastAPI()
TICKS = 8

@app.get("/ticks")
async def ticks(request: Request):
    last = request.headers.get("last-event-id")
    start = int(last) if (last and last.isdigit()) else 0
    print(f"[server] connection opened (resuming after id {start})", flush=True)

    async def gen():
        n = start + 1
        while n <= TICKS:
            yield f"id: {n}\nevent: tick\ndata: tick {n} of {TICKS}\n\n"
            if n == 4 and start < 4:                 # drop once, after the 4th tick
                print("[server] dropping the connection after tick 4", flush=True)
                return
            n += 1
        yield "event: end\ndata: done\n\n"           # final sentinel

    return StreamingResponse(gen(), media_type="text/event-stream")
