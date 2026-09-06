"""FastAPI Server-Sent Events server for the navi SSE interop.

Serves /events as text/event-stream. It sends numbered events (id: 1..TOTAL),
but drops the connection after 3 events per request. So the client only gets the
whole sequence if it reconnects and resumes from Last-Event-ID. All TOTAL events
arriving in order, with no gaps or duplicates, proves reconnection and
Last-Event-ID resume work end to end.

/stall is the same sequence but, instead of closing after each 3-event batch, it
goes SILENT with the h2 stream still open (no END_STREAM). That is the wedge from
issue #229: with all timeouts off, the client's read parks forever and reconnect
never runs. It proves the SSE idle timeout catches the wedge and reconnects.
"""
import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

app = FastAPI()
TOTAL = 10

@app.get("/events")
async def events(request: Request):
    last = request.headers.get("last-event-id")
    start = int(last) if (last and last.isdigit()) else 0

    async def gen():
        sent = 0
        eid = start + 1              # resume after the client's last event
        while eid <= TOTAL:
            yield f"id: {eid}\ndata: event-{eid}\n\n"
            sent += 1
            eid += 1
            if sent >= 3 and eid <= TOTAL:
                return               # drop mid-stream; client must reconnect

    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/stall")
async def stall(request: Request):
    last = request.headers.get("last-event-id")
    start = int(last) if (last and last.isdigit()) else 0

    async def gen():
        sent = 0
        eid = start + 1
        while eid <= TOTAL:
            yield f"id: {eid}\ndata: event-{eid}\n\n"
            sent += 1
            eid += 1
            if sent >= 3 and eid <= TOTAL:
                await asyncio.sleep(3600)   # wedge: silent, stream left open (no END_STREAM)

    return StreamingResponse(gen(), media_type="text/event-stream")
