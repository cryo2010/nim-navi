"""HTTP/2 chaos handlers.

Hybrid design (issue #384): hyper-h2 (`import h2`) is the INBOUND decoder only --
it consumes the client's connection preface + SETTINGS and parses the
RequestReceived/DataReceived events so a handler learns the path/query/body and,
crucially, the client's advertised SETTINGS (so a violation can deliberately
exceed them). Its bundled `hpack.Encoder` builds valid HEADERS payloads whenever
a mode needs a legitimate response prelude (truncate, slowbody, redirectloop,
vanish-mid-body). ALL violation output is hand-rolled raw frames written straight
to the transport via `frame()`: H2Connection is far too well-behaved to emit an
oversized frame, HEADERS on stream 0, corrupt HPACK, or an illegal SETTINGS
value, and we do not want it silently repairing our misbehavior. Going off-script
means abandoning its internal state -- fine, because every data-port connection
serves exactly one misbehavior and then dies.

Like h1.py, the ALPN restriction (`h2` only) is load-bearing: it is how the
sidecar refuses to be an h1 server on an h2 cell, so a chaos response can never
masquerade as the wrong protocol and trip the client's version pin.

All modes are a pure function of the request per the issue: no randomness lives
here. The client's seeded schedule sends the coins (e.g. `?prefix=slow`,
`?case=bigframe`) as query params.
"""

import asyncio
import contextlib
import os
import ssl
import struct
import sys


@contextlib.contextmanager
def _unshadowed_path():
    """Import the REAL hyper-h2 package despite this file being named h2.py.

    chaos_server.py loads this file by path under the module name
    `chaos_proto_h2`, so `sys.modules['h2']` stays free for the installed
    package. But this file's own directory is still on sys.path (the script dir),
    so a bare `import h2` here would find the sibling file, not the library, and
    hyper-h2's internal `import h2.<submodule>` statements would follow it into a
    crash. Stripping our directory from sys.path for the duration of the library
    import resolves `h2` to site-packages; the loaded package is then cached in
    sys.modules, so everything after (including hpack, a real dependency of
    hyper-h2) imports normally."""
    here = os.path.dirname(os.path.abspath(__file__))
    saved = sys.path[:]
    sys.path = [p for p in sys.path
                if os.path.abspath(p or os.getcwd()) != here]
    try:
        yield
    finally:
        sys.path = saved


with _unshadowed_path():
    import h2.config
    import h2.connection
    import h2.events
    from hpack import Encoder

import modes
from modes import qint, rst_close


# --- frame types / flags (RFC 9113 6) ---------------------------------------

FRAME_DATA = 0x0
FRAME_HEADERS = 0x1
FRAME_RST_STREAM = 0x3
FRAME_SETTINGS = 0x4
FRAME_WINDOW_UPDATE = 0x8
FRAME_CONTINUATION = 0x9

FLAG_END_STREAM = 0x1
FLAG_ACK = 0x1
FLAG_END_HEADERS = 0x4

# RST_STREAM / GOAWAY error codes we use.
ERR_CANCEL = 0x8

# SETTINGS identifiers (RFC 9113 6.5.2). The client's MAX_FRAME_SIZE is read off
# H2Connection.remote_settings rather than by id (see Inbound.max_frame_size).
SETTINGS_INITIAL_WINDOW_SIZE = 0x4


# --- TLS --------------------------------------------------------------------

def make_ssl_context(certfile, keyfile):
    """Server TLS context pinned to ALPN `h2` with the harness cert/key. The ALPN
    restriction is load-bearing: it is how the sidecar refuses to be an h1 server
    on an h2 cell, so a chaos response can never masquerade as the wrong protocol
    and trip the client's version pin (that would be a canary failure, not a chaos
    tally)."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
    ctx.set_alpn_protocols(["h2"])
    return ctx


# --- raw frame writer -------------------------------------------------------

def frame(ftype, flags, stream_id, payload):
    """Pack one HTTP/2 frame: the 9-byte header (24-bit length, 8-bit type, 8-bit
    flags, 1 reserved bit + 31-bit stream id) followed by the payload. This is the
    hand-rolled path every violation goes through -- we build the exact bytes on
    the wire, including lengths and stream ids H2Connection would refuse to
    produce. `length` is taken from the payload we pass, so a caller can lie about
    it (see `bigframe`) by pre-building a header with a different length."""
    hdr = struct.pack(">I", len(payload))[1:]        # 24-bit length
    hdr += struct.pack(">BB", ftype, flags)
    hdr += struct.pack(">I", stream_id & 0x7FFFFFFF)  # R bit clear
    return hdr + payload


def frame_hdr(ftype, flags, stream_id, length):
    """A bare 9-byte frame header with an arbitrary declared length, for the cases
    that must lie about the payload size (bigframe declares > client MAX_FRAME_SIZE
    but need not actually send that many bytes -- the length field alone is the
    violation)."""
    hdr = struct.pack(">I", length)[1:]
    hdr += struct.pack(">BB", ftype, flags)
    hdr += struct.pack(">I", stream_id & 0x7FFFFFFF)
    return hdr


# --- inbound decode: preface + SETTINGS + request ---------------------------

class Inbound:
    """Wraps an H2Connection used purely to decode the client side: preface,
    SETTINGS exchange, and the request HEADERS. It answers the handshake enough
    for the client to send its request, then hands us (method, target, body) plus
    the client's advertised SETTINGS. Past that point handlers write raw frames
    and never touch this again."""

    def __init__(self):
        cfg = h2.config.H2Configuration(
            client_side=False, header_encoding="utf-8")
        self.conn = h2.connection.H2Connection(config=cfg)
        self.stream_id = None
        self.method = None
        self.target = None
        self.body = bytearray()

    def max_frame_size(self):
        """The client's advertised SETTINGS_MAX_FRAME_SIZE (default 16384). Read so
        a violation can deliberately exceed it (bigframe = this + 1)."""
        return self.conn.remote_settings.max_frame_size


async def _read_request(reader, writer):
    """Decode the h2 preface + SETTINGS + the request via H2Connection, flushing
    whatever handshake bytes it wants to send (server SETTINGS + ACK, HEADERS ack
    is our job later). Returns an Inbound once RequestReceived + StreamEnded (or a
    body's end) is seen, or None on a broken/short handshake. The point is only to
    get the request and the client's SETTINGS on the table; we do not respond
    through H2Connection."""
    ib = Inbound()
    ib.conn.initiate_connection()
    writer.write(ib.conn.data_to_send())
    try:
        await writer.drain()
    except OSError:
        return None
    got_request = False
    ended = False
    while not (got_request and ended):
        try:
            data = await asyncio.wait_for(reader.read(65536), timeout=10)
        except (asyncio.TimeoutError, OSError):
            return None
        if not data:
            return None
        try:
            events = ib.conn.receive_data(data)
        except Exception:
            # A protocol error decoding the client is not our concern here (the
            # client is the canary and should be well-behaved); bail cleanly.
            return None
        for ev in events:
            if isinstance(ev, h2.events.RequestReceived):
                ib.stream_id = ev.stream_id
                for k, v in ev.headers:
                    if k == ":method":
                        ib.method = v
                    elif k == ":path":
                        ib.target = v
                got_request = True
            elif isinstance(ev, h2.events.DataReceived):
                ib.body += ev.data.encode("latin1") if isinstance(
                    ev.data, str) else ev.data
                ib.conn.acknowledge_received_data(
                    ev.flow_controlled_length, ev.stream_id)
            elif isinstance(ev, h2.events.StreamEnded):
                ended = True
        out = ib.conn.data_to_send()
        if out:
            writer.write(out)
            try:
                await writer.drain()
            except OSError:
                return None
    return ib


# --- HPACK response preludes (valid HEADERS via the bundled encoder) --------

def _headers_block(enc, header_list):
    """HPACK-encode a header list into a HEADERS payload with the bundled encoder.
    Used by the modes that need a legitimate response prelude before they
    misbehave (truncate/slowbody/vanish-mid-body/redirectloop)."""
    return enc.encode(header_list)


def _write_ok_headers(writer, enc, stream_id, length):
    """A valid `200` HEADERS frame promising `length` body bytes (END_HEADERS, not
    END_STREAM). The client then waits for DATA -- which the mode drips, truncates,
    or never sends."""
    block = _headers_block(enc, [
        (":status", "200"),
        ("content-type", "application/octet-stream"),
        ("content-length", str(length)),
    ])
    writer.write(frame(FRAME_HEADERS, FLAG_END_HEADERS, stream_id, block))


async def _drain(writer):
    try:
        await writer.drain()
    except OSError:
        pass


async def _drip_data(writer, stream_id, length, rate, gap=3.0):
    """Drip a DATA body in ~rate-byte frames, gap seconds apart, flushing each.
    Mirrors modes.drip but wraps each chunk in a DATA frame on `stream_id`. The
    default gap (3s) is longer than the chaos client's per-read stall timeout (2s),
    so slowbody reliably trips it -> a fast, clean TimeoutError on every backend."""
    chunk = max(1, rate)
    off = 0
    while off < length:
        n = min(chunk, length - off)
        writer.write(frame(FRAME_DATA, 0, stream_id, b"\x00" * n))
        await _drain(writer)
        off += n
        await asyncio.sleep(gap)


# --- modes (each takes (writer, ib, params); ib carries stream id + settings) --

async def m_stall(writer, ib, params):
    """Read the request (already done), then silence forever: no HEADERS, no DATA,
    no close. The client's read/attempt/total timeouts must fire -> strict
    TimeoutError. Same semantics as h1's stall, just no bytes after the
    handshake."""
    try:
        await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass


async def m_slowbody(writer, ib, params):
    """Valid 200 + large content-length HEADERS, then drip DATA at ~rate B/s so the
    client stalls mid-body and its read timeout fires (strict TimeoutError, stream
    cleanly cancelled). Defaults: len=1 MiB, rate=1 KiB/s."""
    length = qint(params, "len", 1048576)
    rate = qint(params, "rate", 1024)
    enc = Encoder()
    _write_ok_headers(writer, enc, ib.stream_id, length)
    await _drain(writer)
    await _drip_data(writer, ib.stream_id, length, rate)


async def m_truncate(writer, ib, params):
    """Valid HEADERS + partial DATA, then cut the stream short. Two shapes via
    ?case=: `rst` (default) sends some DATA then RST_STREAM(CANCEL); `close` sends
    some DATA then an abrupt socket close. Either way the client must never surface
    a successful short-body Response (it promised more via content-length)."""
    case = params.get("case", "rst")
    enc = Encoder()
    _write_ok_headers(writer, enc, ib.stream_id, 4096)
    writer.write(frame(FRAME_DATA, 0, ib.stream_id, b"c" * 100))
    await _drain(writer)
    if case == "close":
        # Abrupt socket close mid-body (no END_STREAM, no RST) -> the client sees a
        # connection-level EOF partway through a promised body.
        rst_close(writer)
    else:
        # RST_STREAM(CANCEL) mid-body: the stream is torn down after 100 of 4096
        # promised bytes.
        writer.write(frame(FRAME_RST_STREAM, 0, ib.stream_id,
                           struct.pack(">I", ERR_CANCEL)))
        await _drain(writer)
        writer.close()


async def m_garbage(writer, ib, params):
    """Protocol-illegal frame content on the wire, rotated by ?case=:
      hpack    : a HEADERS frame whose payload is not a valid HPACK block
      unknown  : an unknown/reserved frame type with a deterministic payload
      data0    : a DATA frame on stream 0 (connection error, DATA needs a stream)
    A tolerant mode: any of these must yield a typed error, and a *subsequent*
    chaos request on a fresh connection must still succeed (this connection dies,
    nothing sticky is left behind)."""
    case = params.get("case", "hpack")
    if case == "unknown":
        # Frame type 0x1f (reserved/unknown). A deterministic, random-looking
        # payload: no server-side dice, everything from the byte pattern.
        payload = bytes((i * 37 + 11) & 0xFF for i in range(64))
        writer.write(frame(0x1F, 0, ib.stream_id, payload))
    elif case == "data0":
        # DATA on stream 0 is a connection error (RFC 9113 6.1).
        writer.write(frame(FRAME_DATA, 0, 0, b"\x00" * 32))
    else:  # hpack
        # A HEADERS frame carrying bytes that are not a decodable HPACK block.
        payload = bytes((i * 53 + 7) & 0xFF for i in range(48))
        writer.write(frame(FRAME_HEADERS, FLAG_END_HEADERS, ib.stream_id, payload))
    await _drain(writer)
    writer.close()


async def m_vanish(writer, ib, params):
    """Mid-response abrupt death via SO_LINGER=0 RST (reuse rst_close). Coins from
    the schedule:
      ?prefix=slow    : drip a slowbody prefix (valid HEADERS + slow DATA) first,
                        then RST mid-body
      ?at=pre-headers : RST before writing any HEADERS (exercises KeepAliveRace/
                        Unprocessed + the bounded retry path)
    Default: valid HEADERS + partial DATA, then RST. Tolerant, but must resolve
    within the client's total timeout including retries."""
    at = params.get("at", "")
    if at == "pre-headers":
        rst_close(writer)   # die before a single response frame
        return
    length = qint(params, "after", 8192)
    enc = Encoder()
    _write_ok_headers(writer, enc, ib.stream_id, length)
    await _drain(writer)
    if params.get("prefix", "") == "slow":
        rate = qint(params, "rate", 1024)
        # Drip a slow prefix of the promised body, then RST partway through.
        await _drip_data(writer, ib.stream_id, length // 2, rate)
    else:
        writer.write(frame(FRAME_DATA, 0, ib.stream_id, b"s" * (length // 4)))
        await _drain(writer)
    rst_close(writer)


HEADERBOMB_CAP = 1024 * 1024
  # On-wire cap for the CONTINUATION flood. navi aborts an assembled header block at
  # ~128 KiB (its CVE-2024-27316 defense) and tears the whole connection down, so the
  # flood only has to comfortably EXCEED that cap to exercise the bound -- 1 MiB is 8x
  # it. Actually building/sending the client-requested n*size (~32 MiB by default)
  # instead pins this single-threaded sidecar's CPU encoding HPACK, which starves its
  # OWN accept/handshake loop enough that unrelated chaos connects time out (observed
  # as a redirectloop KeepAliveRace under the full-mode mix). Capping keeps the client
  # outcome and the heap assertion's teeth identical while the sidecar stays
  # responsive for the rest of the schedule.


async def m_headerbomb(writer, ib, params):
    """A giant HPACK block split over HEADERS + CONTINUATION frames, bounded on the
    wire at HEADERBOMB_CAP (see above). Two shapes:
      ?case=finite (default): a real HPACK block sent over HEADERS + CONTINUATION,
        END_HEADERS on the last frame -- navi aborts at its 128 KiB cap first, so it
        never actually decodes to the end; the point is that it stays bounded.
      ?case=endless: HEADERS + CONTINUATION with END_HEADERS never set, filler piled
        on up to the cap, then RST -- the client must not follow it forever.
    Tolerant: a typed error (header list too large) or a parsed Response both pass so
    long as memory stays inside the slack bound (the heap assertion is the real
    teeth). h1 rotates via n/size; we honor them but clamp the total to the cap."""
    n = qint(params, "n", 4000)
    size = qint(params, "size", 8192)
    case = params.get("case", "finite")
    mfs = 16384
    sid = ib.stream_id
    # Build a real (bounded) HPACK block: enough header lines to reach the cap, no
    # more. A valid leading pseudo-header so the block starts decodable. Each line
    # encodes to roughly (name + value + ~16) bytes, so divide the target by that to
    # pick the count (dividing by `size` alone overshoots wildly when size is tiny
    # and the name overhead dominates). Slice to the cap as a hard belt-and-braces.
    target = min(max(n * size, 256 * 1024), HEADERBOMB_CAP)
    count = max(1, target // (max(size, 1) + 16))
    enc = Encoder()
    big = _headers_block(enc, [(":status", "200")] +
                         [("x-bomb-%d" % i, "x" * size) for i in range(count)])
    if len(big) > HEADERBOMB_CAP:
        big = big[:HEADERBOMB_CAP]
    # First fragment in a HEADERS frame (no END_HEADERS: more coming).
    writer.write(frame(FRAME_HEADERS, 0, sid, big[:mfs]))
    await _drain(writer)
    off = mfs
    if case == "endless":
        # Never send END_HEADERS: emit the block then filler CONTINUATION up to the
        # cap, then die. The client must bound its buffering rather than follow us.
        while off < len(big):
            writer.write(frame(FRAME_CONTINUATION, 0, sid, big[off:off + mfs]))
            await _drain(writer)
            off += mfs
        filler = b"\x00" * mfs
        while off < HEADERBOMB_CAP:
            writer.write(frame(FRAME_CONTINUATION, 0, sid, filler))
            await _drain(writer)
            off += mfs
        rst_close(writer)
        return
    # finite: the rest of the real block as CONTINUATION frames, END_HEADERS last.
    while off < len(big):
        chunk = big[off:off + mfs]
        off += mfs
        flags = FLAG_END_HEADERS if off >= len(big) else 0
        writer.write(frame(FRAME_CONTINUATION, flags, sid, chunk))
        await _drain(writer)
    writer.close()


async def m_redirectloop(reader, writer, ib):
    """A protocol-VALID 302 self-loop whose Location bumps `?n` each hop; the client
    follows up to its maxRedirects and must surface a bounded 3xx Response (no error,
    within total timeout). This is the ONE fully well-behaved mode, so -- unlike the
    violation modes -- serve it through H2Connection across MANY streams on ONE
    connection: send a valid 302 for each request the client makes and keep the
    connection alive for the next hop. Reusing a single h2 connection (rather than a
    fresh TLS handshake per redirect) is what keeps all ~20 hops fast enough to
    finish inside the client's 10s total timeout even when a heavy streaming workload
    is starving the shared event loop -- closing per hop instead makes redirectloop
    time out / keep-alive-race under load, a strict-mode failure that is really load,
    not a client bug. Special-cased in handle_data because it needs `reader` to pull
    the successive hops."""
    def respond(stream_id, target):
        _mode, params = modes.parse_target(target or "")
        n = qint(params, "n", 0)
        ib.conn.send_headers(stream_id, [
            (":status", "302"),
            ("location", "/chaos/redirectloop?n=%d" % (n + 1)),
            ("content-length", "0"),
        ], end_stream=True)
        writer.write(ib.conn.data_to_send())

    respond(ib.stream_id, ib.target)          # the first hop, already decoded into ib
    await _drain(writer)
    # Successive hops arrive as fresh streams on the same connection. The chaos client
    # shares ONE h2 connection per worker across ALL modes, so once this chain ends
    # the worker's NEXT (different-mode) request lands here too: serve 302 ONLY for
    # /chaos/redirectloop requests, and on any foreign request stop and let the
    # connection close so the client retries it fresh where it gets the RIGHT mode
    # (else we would hijack e.g. a zerowindow request with a 302). A large cap bounds
    # a pathological client.
    hops = 1
    while hops < 4096:
        try:
            data = await asyncio.wait_for(reader.read(65536), timeout=10)
        except (asyncio.TimeoutError, OSError):
            return
        if not data:
            return
        try:
            events = ib.conn.receive_data(data)
        except Exception:
            return
        for ev in events:
            if isinstance(ev, h2.events.RequestReceived):
                target = None
                for k, v in ev.headers:
                    if k == ":path":
                        target = v
                mode, _params = modes.parse_target(target or "")
                if mode != "redirectloop":
                    return       # chain over, foreign mode: close; client retries fresh
                respond(ev.stream_id, target)
                hops += 1
            elif isinstance(ev, h2.events.DataReceived):
                ib.conn.acknowledge_received_data(
                    ev.flow_controlled_length, ev.stream_id)
        out = ib.conn.data_to_send()
        if out:
            writer.write(out)
        await _drain(writer)


async def m_badframes(writer, ib, params):
    """h2-only frame-level protocol violations, rotated by ?case=. Each is a single
    connection-level violation the client must detect and reject (connection
    closed, typed error, no hang). Tolerant.
      bigframe        : a frame whose declared length exceeds the client's
                        SETTINGS_MAX_FRAME_SIZE (RFC 9113 4.2 FRAME_SIZE_ERROR)
      initwin         : SETTINGS with INITIAL_WINDOW_SIZE = 2^31 (6.5.2
                        FLOW_CONTROL_ERROR)
      winupdate0      : WINDOW_UPDATE with a 0 increment (6.9 PROTOCOL_ERROR)
      headers-stream0 : HEADERS on stream 0 (6.2 PROTOCOL_ERROR)
    """
    case = params.get("case", "bigframe")
    sid = ib.stream_id or 1
    if case == "initwin":
        # SETTINGS_INITIAL_WINDOW_SIZE = 2^31 is above the max (2^31 - 1) ->
        # FLOW_CONTROL_ERROR. One 6-byte SETTINGS entry.
        payload = struct.pack(">HI", SETTINGS_INITIAL_WINDOW_SIZE, 0x80000000)
        writer.write(frame(FRAME_SETTINGS, 0, 0, payload))
    elif case == "winupdate0":
        # WINDOW_UPDATE with increment 0 is a PROTOCOL_ERROR (connection scope).
        writer.write(frame(FRAME_WINDOW_UPDATE, 0, 0, struct.pack(">I", 0)))
    elif case == "headers-stream0":
        # HEADERS must be on a non-zero stream; on stream 0 it is a PROTOCOL_ERROR.
        enc = Encoder()
        block = _headers_block(enc, [(":status", "200")])
        writer.write(frame(FRAME_HEADERS, FLAG_END_HEADERS, 0, block))
    else:  # bigframe
        # Declare a length one byte past the client's advertised MAX_FRAME_SIZE.
        # The length field alone is the violation; we send only a small real body
        # (the client rejects on the oversized header before it ever arrives).
        oversized = ib.max_frame_size() + 1
        writer.write(frame_hdr(FRAME_DATA, 0, sid, oversized))
        writer.write(b"\x00" * 16)   # a token payload; the length lie is the point
    await _drain(writer)
    writer.close()


async def m_zerowindow(writer, ib, params):
    """Flow-control starvation on the response side. We send a valid 200 HEADERS
    promising a body, then never send DATA and never a WINDOW_UPDATE: the client
    waits on a response body that never arrives and must hit its timeout family
    (strict). The ?body= param names the size the schedule associates with the
    mode; the phase-1 client issues a GET (no request body to flush), so the
    never-arriving response body is the starvation lever. Modelling the client
    send-window with INITIAL_WINDOW_SIZE=0 would also stall a POST body -- the
    faithful reading of the mode name -- but the phase-1 GET-only client does not
    exercise that leg, so the response stall is what enforces the strict class."""
    length = qint(params, "body", 65536)
    enc = Encoder()
    _write_ok_headers(writer, enc, ib.stream_id, max(length, 1))
    await _drain(writer)
    # Drain-and-discard forever; never grant window, never send DATA.
    try:
        await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass


# Registered under h2. The schedule filters by proto so only applicable ones are
# drawn; the shared-name modes mirror h1's paths, plus badframes + zerowindow.
_H2_MODES = {
    "stall": m_stall,
    "slowbody": m_slowbody,
    "truncate": m_truncate,
    "garbage": m_garbage,
    "vanish": m_vanish,
    "headerbomb": m_headerbomb,
    "redirectloop": m_redirectloop,
    "badframes": m_badframes,
    "zerowindow": m_zerowindow,
}

for _name, _fn in _H2_MODES.items():
    modes.register("h2", _name, _fn)


# --- connection dispatch (data port) ----------------------------------------

async def handle_data(reader, writer, log):
    """One data-port connection: decode the request via hyper-h2 (preface +
    SETTINGS + HEADERS), look up the mode from its /chaos/<mode> target, run it. An
    unknown target gets a benign 404 (so a stray probe does not look like a mode).
    Every connection is mode-tagged in the log for the post-mortem. Every mode then
    writes raw frames (or nothing, for stall) directly to the transport; past the
    request decode we never touch H2Connection again -- it is too well-behaved to
    emit the violations we want, and each connection serves one misbehavior and
    dies, so abandoning its state is fine."""
    peer = writer.get_extra_info("peername")
    ib = await _read_request(reader, writer)
    if ib is None or ib.stream_id is None:
        log("h2 data %s: unparseable request, closing" % (peer,))
        rst_close(writer)
        return
    mode, params = modes.parse_target(ib.target or "")
    handler = modes.handler_for("h2", mode)
    if handler is None:
        log("h2 data %s: %s %s -> no mode, 404" % (peer, ib.method, ib.target))
        enc = Encoder()
        block = enc.encode([(":status", "404"), ("content-length", "0")])
        writer.write(frame(FRAME_HEADERS, FLAG_END_HEADERS | FLAG_END_STREAM,
                           ib.stream_id, block))
        await _drain(writer)
        writer.close()
        return
    log("h2 data %s: mode=%s params=%s stream=%d"
        % (peer, mode, params, ib.stream_id))
    try:
        if mode == "redirectloop":
            # The one protocol-VALID mode, served across many streams on this single
            # connection (see m_redirectloop) -- so it takes `reader` to pull the
            # successive redirect hops, not the (writer, ib, params) violation shape.
            await m_redirectloop(reader, writer, ib)
        else:
            await handler(writer, ib, params)
    except (ConnectionError, asyncio.CancelledError):
        pass
    except OSError as e:
        log("h2 data %s: mode=%s wire-error %s" % (peer, mode, e))
    finally:
        try:
            writer.close()
        except OSError:
            pass


# --- accept-time listeners --------------------------------------------------

async def handle_vanish_on_accept(reader, writer, log):
    """RST immediately after accept -- before the h2 preface. Connect-phase failure:
    the client should get a typed connect/reset error."""
    peer = writer.get_extra_info("peername")
    log("h2 vanish-on-accept %s: RST" % (peer,))
    rst_close(writer)


async def handle_stall_on_accept(reader, writer, log):
    """Accept, then never progress: never send the server preface/SETTINGS, hold
    the socket open until the client's connect/total timeout fires
    (TimeoutError)."""
    peer = writer.get_extra_info("peername")
    log("h2 stall-on-accept %s: holding open" % (peer,))
    try:
        await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass


# --- server startup ---------------------------------------------------------

async def start(host, base, band, certfile, keyfile, log):
    """Bind the three h2 listeners (data / vanish-on-accept / stall-on-accept),
    each on its own port relative to base+band, mirroring h1.start. Returns the
    list of asyncio servers so the core can serve_forever + close them on
    shutdown. All TLS with ALPN pinned to `h2`; loopback only (host is
    127.0.0.1)."""
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
    log("h2 listeners up: data=%d vanish=%d stall=%d"
        % (base + band, base + band + 1, base + band + 2))
    return [data_srv, vanish_srv, stall_srv]
