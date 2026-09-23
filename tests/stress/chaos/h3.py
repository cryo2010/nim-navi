"""HTTP/3 chaos handlers -- PHASE 3 STUB.

Not implemented yet. The design (issue #384) uses aioquic (already in the h3
image): stalls, drip-feed via timed send_stream_data + transmit, partial DATA
then reset_stream, stop_sending, quic.close mid-body, silent UDP-endpoint drop,
raw non-H3 bytes on streams, bogus/reserved frame types, and duplicate SETTINGS
on a second control stream. Flow-control starvation and surgical QPACK
corruption are out of scope without patching aioquic (so `zerowindow` is
h2-only).

Phase 3 will implement the h3 rows of the mode catalog and register them via
`modes.register("h3", name, handler)` from a `start()` that stands up the QUIC
data endpoint plus the accept-time endpoints. Until then `register` is a no-op
and selecting --proto h3 hard-errors in chaos_server.py.
"""

# Intentionally register nothing: mode_names("h3") stays empty.


async def start(host, base, band, certfile, keyfile, log):
    raise NotImplementedError("h3 chaos not implemented yet (phase 3)")
