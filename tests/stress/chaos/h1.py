"""HTTP/1.1 chaos handlers.

Raw `asyncio.start_server` + a TLS context whose ALPN is restricted to
`http/1.1` (so the sidecar offers only the cell's pinned protocol and a client
that dials h2 there gets an ALPN mismatch, never a silent downgrade). The
request parser is deliberately minimal -- request line + headers, then drain the
body per Content-Length -- because past the parse the whole point is to put
*arbitrary* bytes on the wire, protocol-legal or not.

Every data-port connection serves exactly one misbehavior (selected by the
request's `/chaos/<mode>` target) and then dies; there is no keep-alive here.
The accept-time modes (`vanish-on-accept`, `stall-on-accept`) never read a
request at all -- they are wired straight to their own listeners by the server
core, one port each.

All modes are a pure function of the request per the issue: no randomness lives
here. The client's seeded schedule sends the coins (e.g. `?prefix=slow`) as
query params.
"""

import asyncio
import ssl

import modes
from modes import qint, rst_close, drip


# --- TLS --------------------------------------------------------------------

def make_ssl_context(certfile, keyfile):
    """Server TLS context pinned to ALPN http/1.1 with the harness cert/key. The
    ALPN restriction is load-bearing: it is how the sidecar refuses to be an h2
    server on an h1 cell, so a chaos response can never masquerade as the wrong
    protocol and trip the client's version pin (that would be a canary failure,
    not a chaos tally)."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
    ctx.set_alpn_protocols(["http/1.1"])
    return ctx


# --- request parser ---------------------------------------------------------

async def read_request(reader):
    """Read the request line + headers, then drain any Content-Length body.

    Returns (method, target, headers) or None on a malformed/short request. The
    body is drained (not returned) so the socket is at a clean boundary before we
    start misbehaving -- otherwise unread request bytes can wedge the client's
    write side and confuse which side actually stalled.
    """
    try:
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=10)
    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError,
            asyncio.TimeoutError):
        return None
    lines = head.split(b"\r\n")
    if not lines or not lines[0]:
        return None
    try:
        method, target, _ = lines[0].decode("latin1").split(" ", 2)
    except ValueError:
        return None
    headers = {}
    for ln in lines[1:]:
        if not ln:
            continue
        k, _, v = ln.partition(b":")
        headers[k.decode("latin1").strip().lower()] = v.decode("latin1").strip()
    clen = 0
    try:
        clen = int(headers.get("content-length", "0"))
    except ValueError:
        clen = 0
    if clen > 0:
        try:
            await asyncio.wait_for(reader.readexactly(clen), timeout=10)
        except (asyncio.IncompleteReadError, asyncio.TimeoutError):
            pass
    return method, target, headers


# --- response helpers -------------------------------------------------------

def _ok_headers(length):
    return (
        "HTTP/1.1 200 OK\r\n"
        "content-type: application/octet-stream\r\n"
        "content-length: %d\r\n"
        "\r\n" % length
    ).encode("latin1")


async def _write(writer, data):
    writer.write(data)
    await writer.drain()


# --- modes ------------------------------------------------------------------

async def m_stall(writer, params):
    """Read the request (already done), then silence forever. The client's
    read/attempt/total timeouts must fire -> strict TimeoutError. We simply never
    write and never close until the client gives up and drops the connection."""
    try:
        await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass


async def m_slowbody(writer, params):
    """Valid 200 with a large Content-Length, then drip the body at ~rate B/s so
    the client stalls mid-body and its read timeout fires (strict TimeoutError,
    stream cleanly cancelled). Defaults: len=1 MiB, rate=1 KiB/s."""
    length = qint(params, "len", 1048576)
    rate = qint(params, "rate", 1024)
    await _write(writer, _ok_headers(length))
    await drip(writer, b"\x00" * length, rate)


async def m_truncate(writer, params):
    """Body shorter than what we promised, then close cleanly. Two shapes via
    ?case=: `clen` (default) declares a Content-Length and sends fewer bytes;
    `chunked` uses Transfer-Encoding: chunked and cuts off mid-chunk (announces a
    chunk size, sends part of it, then EOF). Either way the client must never
    surface a successful short-body Response."""
    case = params.get("case", "clen")
    if case == "chunked":
        head = (
            "HTTP/1.1 200 OK\r\n"
            "content-type: application/octet-stream\r\n"
            "transfer-encoding: chunked\r\n"
            "\r\n"
        ).encode("latin1")
        # Announce a 1024-byte chunk, deliver only 100 bytes, then EOF: the chunk
        # framing is violated mid-chunk (no terminator, no zero-chunk).
        await _write(writer, head + b"400\r\n" + b"c" * 100)
    else:
        await _write(writer, _ok_headers(4096) + b"c" * 100)
    writer.close()


async def m_garbage(writer, params):
    """Protocol-illegal bytes on the wire, rotated by ?case=:
      status  : malformed status line (not `HTTP/1.1 <code> ...`)
      header  : a colon-less header line
      clen    : `Content-Length: abc` (non-numeric)
      nul     : NUL bytes sprinkled through the head
      doubled : two full responses back-to-back on one request
    A tolerant mode: any of these must yield a typed error, and crucially a
    *subsequent* chaos request on a fresh connection must still succeed (this
    connection dies, but nothing sticky is left behind)."""
    case = params.get("case", "status")
    if case == "header":
        blob = (
            b"HTTP/1.1 200 OK\r\n"
            b"content-type application/octet-stream\r\n"   # no colon
            b"content-length: 2\r\n\r\nhi"
        )
    elif case == "clen":
        blob = (
            b"HTTP/1.1 200 OK\r\n"
            b"content-length: abc\r\n\r\nhi"
        )
    elif case == "nul":
        blob = (
            b"HTTP/1.1 2\x0000 OK\r\n"
            b"content-\x00length: 2\r\n\r\nh\x00"
        )
    elif case == "doubled":
        one = _ok_headers(2) + b"hi"
        blob = one + one
    else:  # status
        blob = b"NOT-A-STATUS-LINE garbage bytes\r\n\r\n"
    await _write(writer, blob)
    writer.close()


async def m_vanish(writer, params):
    """Mid-response abrupt death via SO_LINGER=0 RST. Coins from the schedule:
      ?prefix=slow    : drip a slowbody prefix first, then RST mid-body
      ?at=pre-headers : RST before writing any response (no headers at all),
                        exercising KeepAliveRaceError/UnprocessedError + the
                        bounded retry path
    Default (no coins): send valid headers + partial body, then RST. Tolerant,
    but must resolve within the client's total timeout including retries."""
    at = params.get("at", "")
    if at == "pre-headers":
        rst_close(writer)   # die before a single response byte
        return
    length = qint(params, "after", 8192)
    await _write(writer, _ok_headers(length))
    if params.get("prefix", "") == "slow":
        rate = qint(params, "rate", 1024)
        # Drip a slow prefix of the promised body, then RST partway through.
        await drip(writer, b"s" * (length // 2), rate)
    else:
        await _write(writer, b"s" * (length // 4))   # partial body
    rst_close(writer)


async def m_headerbomb(writer, params):
    """Thousands of 8 KiB header lines before the (empty) body. The client must
    stay bounded in memory -- the heap assertion is the real teeth. Tolerant: a
    typed error (header section too large) or a parsed Response both pass, so long
    as memory stays inside the slack bound. Default: 4000 lines * 8 KiB ~= 32 MiB
    of header bytes on the wire."""
    n = qint(params, "n", 4000)
    size = qint(params, "size", 8192)
    writer.write(b"HTTP/1.1 200 OK\r\n")
    line_val = b"x" * size
    for i in range(n):
        writer.write(b"x-bomb-%d: " % i + line_val + b"\r\n")
        if i % 256 == 0:
            await writer.drain()
    writer.write(b"content-length: 0\r\n\r\n")
    await writer.drain()
    writer.close()


async def m_redirectloop(writer, params):
    """A protocol-valid 302 whose Location points at the next hop in a self-loop
    (`?n` incremented each time). The client must follow up to its maxRedirects
    and then surface a bounded 3xx Response -- no error, within total timeout.
    Strict on the client side; here we just always redirect forward."""
    n = qint(params, "n", 0)
    loc = "/chaos/redirectloop?n=%d" % (n + 1)
    body = (
        "HTTP/1.1 302 Found\r\n"
        "location: %s\r\n"
        "content-length: 0\r\n"
        "\r\n" % loc
    ).encode("latin1")
    await _write(writer, body)
    writer.close()


# Registered under h1. h2/h3 register their own variants of the shared names in
# their modules; the schedule filters by proto so only applicable ones are drawn.
_H1_MODES = {
    "stall": m_stall,
    "slowbody": m_slowbody,
    "truncate": m_truncate,
    "garbage": m_garbage,
    "vanish": m_vanish,
    "headerbomb": m_headerbomb,
    "redirectloop": m_redirectloop,
}

for _name, _fn in _H1_MODES.items():
    modes.register("h1", _name, _fn)


# --- connection dispatch (data port) ----------------------------------------

async def handle_data(reader, writer, log):
    """One data-port connection: parse the request, look up the mode from its
    /chaos/<mode> target, run it. An unknown target gets a benign 404 (so a
    stray probe does not look like a mode). Every connection is mode-tagged in
    the log for the post-mortem."""
    peer = writer.get_extra_info("peername")
    req = await read_request(reader)
    if req is None:
        log("h1 data %s: unparseable request, closing" % (peer,))
        rst_close(writer)
        return
    method, target, _headers = req
    mode, params = modes.parse_target(target)
    handler = modes.handler_for("h1", mode)
    if handler is None:
        log("h1 data %s: %s %s -> no mode, 404" % (peer, method, target))
        await _write(
            writer,
            b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n")
        writer.close()
        return
    log("h1 data %s: mode=%s params=%s" % (peer, mode, params))
    try:
        await handler(writer, params)
    except (ConnectionError, asyncio.CancelledError):
        pass
    except OSError as e:
        log("h1 data %s: mode=%s wire-error %s" % (peer, mode, e))
    finally:
        try:
            writer.close()
        except OSError:
            pass


# --- accept-time listeners --------------------------------------------------

async def handle_vanish_on_accept(reader, writer, log):
    """RST immediately after accept -- before reading anything. Connect-phase
    failure: the client should get a typed connect/reset error."""
    peer = writer.get_extra_info("peername")
    log("h1 vanish-on-accept %s: RST" % (peer,))
    rst_close(writer)


async def handle_stall_on_accept(reader, writer, log):
    """Accept, then never progress: no read, no write, hold the socket open until
    the client's connect/total timeout fires (TimeoutError)."""
    peer = writer.get_extra_info("peername")
    log("h1 stall-on-accept %s: holding open" % (peer,))
    try:
        await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass


# --- server startup ---------------------------------------------------------

async def start(host, base, band, certfile, keyfile, log):
    """Bind the three h1 listeners (data / vanish-on-accept / stall-on-accept),
    each on its own port relative to base+band. Returns the list of asyncio
    servers so the core can serve_forever + close them on shutdown. All TLS with
    ALPN http/1.1; loopback only (host is 127.0.0.1)."""
    ctx = make_ssl_context(certfile, keyfile)

    def _wrap(fn):
        async def _cb(r, w):
            await fn(r, w, log)
        return _cb

    data_srv = await asyncio.start_server(
        _wrap(handle_data), host, base + band, ssl=ctx)
    vanish_srv = await asyncio.start_server(
        _wrap(handle_vanish_on_accept), host, base + band + 1, ssl=ctx)
    stall_srv = await asyncio.start_server(
        _wrap(handle_stall_on_accept), host, base + band + 2, ssl=ctx)
    log("h1 listeners up: data=%d vanish=%d stall=%d"
        % (base + band, base + band + 1, base + band + 2))
    return [data_srv, vanish_srv, stall_srv]
