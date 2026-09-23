#!/usr/bin/env python3
"""Chaos sidecar entrypoint for the navi stress harness.

Launched once per cell by tests/stress/run.sh with the cell's pinned protocol.
It stands up, on loopback only, relative to --base-port + --band:

    base+band     data port    -- path-selected modes (per the mode catalog)
    base+band+1   vanish-on-accept -- RST right after accept
    base+band+2   stall-on-accept  -- accepts, never progresses
    base+band+99  control port -- plain well-behaved HTTP, GET /health

The control port is the healthcheck for a server whose entire job is to fail
healthchecks: run.sh polls it for `"ready": true` before launching the client,
so the client never races the listeners. It is always plain TCP HTTP (no TLS),
so a curl/poll needs no cert.

The server is a pure function of the request -- no randomness lives here; the
client's seeded schedule encodes the mode + coins in the path/query. Every
connection is mode-tagged on stdout for the post-mortem log.

Only the pinned protocol is offered. h2/h3 are phase 2/3: their modules are
clean stubs that register no modes, so selecting --proto h2|h3 hard-errors
early with a clear message rather than starting a half-server.
"""

import argparse
import asyncio
import importlib.util
import json
import os
import sys
import time

import modes


def _load_proto(name, filename):
    """Load a per-protocol module (h1.py/h2.py/h3.py) from this directory under a
    NON-colliding module name. This matters for h2.py specifically: its file is
    named `h2.py`, which would otherwise shadow the installed hyper-h2 package the
    moment it were imported as bare `h2` -- hyper-h2's own internal `import
    h2.<submodule>` statements would then resolve back to our file instead of the
    real library and blow up. Loading by path under `chaos_proto_h2` keeps
    `sys.modules['h2']` reserved for the real package, so `h2.py` can `import
    h2.connection` normally. The proto modules `import modes` / `register` at load
    time, which still resolves (modes is not shadowed and the dir is on the path).
    """
    here = os.path.dirname(os.path.abspath(__file__))
    spec = importlib.util.spec_from_file_location(
        name, os.path.join(here, filename))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


h1 = _load_proto("chaos_proto_h1", "h1.py")
h2 = _load_proto("chaos_proto_h2", "h2.py")
h3 = _load_proto("chaos_proto_h3", "h3.py")


_PROTO_MODULES = {"h1": h1, "h2": h2, "h3": h3}


def log(msg):
    """Mode-tagged, timestamped line to stdout (run.sh redirects to
    srv-chaos.log, which the post-mortem glob preserves on failure)."""
    sys.stdout.write("[chaos %.3f] %s\n" % (time.time(), msg))
    sys.stdout.flush()


async def start_control(host, port, proto):
    """A minimal, well-behaved HTTP/1.1 server on loopback serving GET /health ->
    {"proto":..., "modes":[...], "ready": true}. Deliberately boring: this is the
    one endpoint that must NOT misbehave, so readiness polling is reliable."""
    payload = json.dumps({
        "proto": proto,
        "modes": modes.mode_names(proto),
        "ready": True,
    }).encode("utf-8")

    async def handle(reader, writer):
        try:
            await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError,
                asyncio.TimeoutError):
            writer.close()
            return
        body = payload
        resp = (
            b"HTTP/1.1 200 OK\r\n"
            b"content-type: application/json\r\n"
            b"content-length: " + str(len(body)).encode() + b"\r\n"
            b"connection: close\r\n\r\n" + body
        )
        writer.write(resp)
        try:
            await writer.drain()
        except OSError:
            pass
        writer.close()

    srv = await asyncio.start_server(handle, host, port)
    log("control up: http://%s:%d/health (modes=%s)"
        % (host, port, modes.mode_names(proto)))
    return srv


async def run(args):
    proto = args.proto
    mod = _PROTO_MODULES[proto]

    # h2/h3 are stubs: they register no modes. Refuse to start rather than come
    # up as an empty server that would make the client's chaos phase no-op
    # silently (the control port is not even needed in this branch).
    if not modes.mode_names(proto):
        sys.stderr.write(
            "chaos_server: %s chaos not implemented yet "
            "(only --proto h1 is supported in this phase)\n" % proto)
        return 2

    servers = await mod.start(
        args.host, args.base_port, args.band, args.cert, args.key, log)
    control = await start_control(
        args.host, args.base_port + args.band + 99, proto)
    servers.append(control)

    log("chaos sidecar ready: proto=%s base=%d band=%d"
        % (proto, args.base_port, args.band))
    try:
        await asyncio.gather(*(s.serve_forever() for s in servers))
    except asyncio.CancelledError:
        pass
    finally:
        for s in servers:
            s.close()
    return 0


def main():
    ap = argparse.ArgumentParser(description="navi stress chaos sidecar")
    ap.add_argument("--proto", required=True, choices=["h1", "h2", "h3"])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--base-port", type=int, required=True)
    ap.add_argument("--band", type=int, default=2000)
    ap.add_argument("--cert", required=True)
    ap.add_argument("--key", required=True)
    args = ap.parse_args()
    try:
        rc = asyncio.run(run(args))
    except KeyboardInterrupt:
        rc = 0
    sys.exit(rc)


if __name__ == "__main__":
    main()
