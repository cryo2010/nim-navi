"""Release a hypercorn connection as soon as the client closes it.

hypercorn 0.18's asyncio TCPServer leaks every connection the client closes
while it is idle between requests: `_read_data` sees EOF and hands the protocol
`Closed()`, which with no stream in flight does nothing, and the connection's
idle-timeout task is never stopped. `TCPServer.run()` then sits in its
TaskGroup's `__aexit__` waiting for that task, which sleeps the full
`keep_alive_timeout` before it closes a socket that is already gone. The task,
its TLS transport and asyncio's 256 KiB SSLProtocol buffer stay alive the whole
time: about 370 KB per closed connection.

run.sh sets `keep_alive_timeout` past the whole soak (see hypercorn_servers),
so in the harness "the whole time" means the rest of the run. A client that
pools forever (navi's native backends) never closes an idle connection and never
notices. undici, under navi/js, closes each idle socket after ~4s, so the js
mixed cell leaked a steady stream of them: a hypercorn worker grew to ~0.9 GB in
about 5.5h, the Docker VM's OOM killer SIGKILLed it, hypercorn's master shut its
port when the worker exited non-zero, and the client saw ECONNREFUSED.

The fix, applied to the class when app.py is imported (in each hypercorn
worker, before it serves): once the read side is at EOF, stop the idle timer and
never re-arm it, so the connection ends the moment its in-flight streams finish
(immediately, when it was idle). Lowering keep_alive_timeout instead would bound
the leak, but hypercorn's timer also cuts busy h2 connections (hypercorn_servers
explains), which is exactly what the long value is there to prevent.

Guarded: if a future hypercorn lacks the methods this wraps, nothing is patched
and a line on stderr (the srv-*.log) says so.
"""
from __future__ import annotations

import sys


def apply() -> bool:
    try:
        from hypercorn.asyncio.tcp_server import TCPServer
        from hypercorn.events import Updated
    except ImportError:
        return False
    if getattr(TCPServer, "_navi_eof_patched", False):
        return True
    orig_read = getattr(TCPServer, "_read_data", None)
    orig_send = getattr(TCPServer, "protocol_send", None)
    if orig_read is None or orig_send is None:
        print("hypercorn_eof: TCPServer._read_data/protocol_send not found; "
              "idle connections closed by the client will linger until "
              "keep_alive_timeout", file=sys.stderr, flush=True)
        return False

    async def _read_data(self) -> None:
        try:
            await orig_read(self)
        finally:
            # The peer is gone: nothing more can arrive, so there is nothing to
            # wait idle for. Stopping the timer lets run()'s TaskGroup exit.
            self._navi_eof = True
            await self.idle_task.stop()

    async def protocol_send(self, event) -> None:
        # An h1 stream that completes after the peer's EOF recycles the
        # connection and asks for the idle timer again; do not re-arm it.
        if isinstance(event, Updated) and event.idle and getattr(self, "_navi_eof", False):
            return
        await orig_send(self, event)

    TCPServer._read_data = _read_data
    TCPServer.protocol_send = protocol_send
    TCPServer._navi_eof_patched = True
    return True
