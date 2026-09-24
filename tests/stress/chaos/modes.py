"""Mode registry + shared low-level helpers for the chaos sidecar.

The sidecar is a *pure function of the request*: the client's seeded schedule
picks a mode and its parameters and encodes them in the request path/query
(e.g. ``GET /chaos/slowbody?len=1048576&rate=1024``). The server never rolls
dice -- every misbehavior is fully determined by the incoming request (or, for
the accept-time modes, by which port the connection landed on). That keeps both
sides' logs naming the same mode for every interaction and makes reruns with the
same seed byte-identical.

This module is protocol-agnostic. Each protocol module (h1.py, h2.py, h3.py)
imports `register` / `handler_for` to attach its own handlers, plus the wire
helpers below (RST close, drip writer). The server core in chaos_server.py only
ever asks this registry which modes a protocol advertises (for /health) and
dispatches to the handler a request selected.
"""

import asyncio
import socket
import struct
from urllib.parse import urlsplit, parse_qs


# proto -> { mode name -> handler }. Handlers are registered by the per-proto
# modules at import time; their signature is defined by each protocol module
# (h1 handlers take (writer, params); h2/h3 differ). The registry only tracks
# names so /health can advertise them and the dispatcher can look one up.
_REGISTRY = {"h1": {}, "h2": {}, "h3": {}}


def register(proto, name, handler):
    """Attach `handler` for `name` under `proto`. Called at module import."""
    _REGISTRY[proto][name] = handler


def handler_for(proto, name):
    """The handler for (proto, name), or None if this proto has no such mode."""
    return _REGISTRY.get(proto, {}).get(name)


def mode_names(proto):
    """Sorted mode names a proto advertises, for the /health payload."""
    return sorted(_REGISTRY.get(proto, {}).keys())


# --- request parsing --------------------------------------------------------

def parse_target(target):
    """Split an h1/h2/h3 request target into (mode, params).

    The mode is the last path segment under /chaos/ (``/chaos/slowbody`` ->
    ``slowbody``); params is a flat dict of the *first* value of each query key
    (the schedule never sends repeated keys). A target without a recognisable
    /chaos/<mode> yields ("", {}) so the caller can fall back to a benign 404.
    """
    parts = urlsplit(target)
    segs = [s for s in parts.path.split("/") if s]
    mode = ""
    if len(segs) >= 2 and segs[0] == "chaos":
        mode = segs[1]
    raw = parse_qs(parts.query, keep_blank_values=True)
    params = {k: v[0] for k, v in raw.items()}
    return mode, params


def qint(params, key, default):
    """Query param as int with a default; a malformed value falls back rather
    than crashing the connection (the client controls these, but be defensive)."""
    try:
        return int(params.get(key, default))
    except (TypeError, ValueError):
        return default


# --- wire helpers -----------------------------------------------------------

def rst_close(writer):
    """Abrupt RST close: set SO_LINGER=0 so close() sends a TCP RST instead of a
    graceful FIN/ACK. This is the `vanish` teardown for h1/h2 -- it forces the
    client to observe a reset mid-stream rather than a clean EOF, exercising the
    reset-classification and retry paths. Best-effort: if the socket is already
    gone we just close.
    """
    try:
        sock = writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER,
                struct.pack("ii", 1, 0))
    except OSError:
        pass
    try:
        writer.close()
    except OSError:
        pass


async def drip(writer, data, rate, gap=3.0):
    """Write `data` in ~`rate`-byte chunks, pausing `gap` seconds between chunks
    (flushing after each). Used by slowbody and the slow prefix of vanish.
    `rate<=0` collapses to a single write. Any transport error (peer went away)
    propagates so the caller's connection handler can log + move on.

    The default `gap` (3s) is deliberately longer than the chaos client's per-read
    stall timeout (2s), so a slow-body read reliably trips that timeout -> a clean,
    fast TimeoutError on every backend (including the blocking sync client, whose
    whole thread would otherwise stall for the full transfer). The client controls
    the shape via ?rate=; the gap stays server-side so the stall is deterministic.
    """
    if rate <= 0:
        writer.write(data)
        await writer.drain()
        return
    chunk = max(1, rate)
    for off in range(0, len(data), chunk):
        writer.write(data[off:off + chunk])
        await writer.drain()
        await asyncio.sleep(gap)
