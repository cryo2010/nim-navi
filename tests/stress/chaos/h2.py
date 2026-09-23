"""HTTP/2 chaos handlers -- PHASE 2 STUB.

Not implemented yet. The design (issue #384) is a hybrid: hyper-h2 decodes the
inbound preface/SETTINGS/HEADERS and its bundled hpack.Encoder builds legitimate
HEADERS preludes, while all *violation* output is hand-rolled raw frames (9-byte
header via struct.pack) written straight to the transport -- hyper-h2 is too
well-behaved to emit oversized frames, HEADERS on stream 0, corrupt HPACK, or
illegal SETTINGS.

Phase 2 will implement the h2 rows of the mode catalog (truncate, garbage,
vanish, badframes, zerowindow, headerbomb, redirectloop, stall, slowbody) and
register them via `modes.register("h2", name, handler)` from a `start()` that
mirrors h1.start's shape (data + vanish-on-accept + stall-on-accept listeners,
ALPN pinned to h2). Until then `register` is a no-op and selecting --proto h2
hard-errors in chaos_server.py.
"""

# Intentionally register nothing: mode_names("h2") stays empty, so /health would
# advertise no modes and chaos_server refuses to start for --proto h2.


async def start(host, base, band, certfile, keyfile, log):
    raise NotImplementedError("h2 chaos not implemented yet (phase 2)")
