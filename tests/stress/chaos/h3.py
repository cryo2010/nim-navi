"""HTTP/3 chaos handlers via aioquic (issue #384, phase 3).

Structural sibling of h2.py, but the transport is QUIC over UDP, so the shape is
different: instead of one `asyncio.start_server` TCP connection per misbehavior,
aioquic multiplexes every stream of a QUIC connection through ONE
`QuicConnectionProtocol` instance. The chaos client shares a single h3 connection
per worker across many modes and streams (phase-2 lesson), so this protocol must
handle each request stream independently and keep serving the connection.

Hybrid design mirroring h2.py: an `H3Connection` is the INBOUND decoder and the
source of well-formed response preludes. It parses the client's request
(HeadersReceived/DataReceived) so a handler learns the path/query, and its
`send_headers`/`send_data` build legitimate 200/302 responses for the modes that
need a valid prelude (slowbody, truncate, redirectloop, vanish-mid-body,
headerbomb). ALL protocol violations bypass H3Connection and write raw bytes with
`quic.send_stream_data` -- H3Connection is far too well-behaved to emit garbage
frame bytes, bogus frame types, or a duplicate SETTINGS on a second control
stream, and we do not want it repairing our misbehavior.

Teardown discipline (hard-won): aioquic's serve() multiplexes EVERY connection
through ONE shared UDP datagram socket, so a mode must never close that transport
to "vanish" -- doing so kills the whole data port and every subsequent connect
fails with "connection refused" (it broke the strict redirectloop mode outright).
All abrupt-death shapes stay per-connection (`quic.close()` -> a CONNECTION_CLOSE
frame for just this connection) or per-stream (`reset_stream`); the listener stays
up for the next worker's requests.

Reachability (hard-won): navi has NO direct-dial h3. Like every browser, it
reaches h3 exclusively via Alt-Svc discovery -- the first request to an origin
goes over TCP h1/h2 (`wantsH2` forces that bootstrap for a `{H3}` pin, and
`protocolAllowed` exempts it from the pin), the response's `Alt-Svc: h3=...`
header is cached, and only subsequent requests ride QUIC. The verified soak gets
this leg from Caddy's TCP side; a UDP-only chaos port is therefore UNREACHABLE
by construction: every request dies with TCP connection-refused, the tolerant
modes launder that into their expected-error tallies, and strict redirectloop
exposes the vacuity (this exact failure was observed before the leg was added).
So each chaos band port pairs its QUIC listener with a TCP+TLS DISCOVERY leg
(ALPN `http/1.1`) that serves exactly one thing: a protocol-valid 302 redirect to
the SAME target with a `d=1` marker appended, carrying `Alt-Svc: h3=":<port>"`.
navi follows the redirect; by the next hop the endpoint is cached and the request
lands on QUIC where the real mode runs. A TCP request that already carries `d=1`
is a post-QUIC-failure fallback (navi falls back to h2/h1 on any QuicError): it
gets an abrupt RST, so the h3 failure surfaces to the classifier as a typed error
instead of being re-served -- and, critically, so a maxRedirects-exhausted h1 302
can never surface as the FINAL response and trip the version pin. NO chaos mode
response ever flows over TCP: the discovery leg serves only internal 302 hops
(pin-exempt, never surfaced) or an RST. That honors the issue's "no TCP fallback"
intent -- no chaos response can masquerade as the wrong protocol -- while making
the QUIC listener reachable at all.

Feasibility limits from the issue (do NOT attempt without patching aioquic):
flow-control starvation (aioquic auto-grants credit in transmit()) and surgical
QPACK corruption beyond raw garbage bytes. So `zerowindow` stays h2-only; the h3
schedule never draws it.

All modes are a pure function of the request per the issue: no randomness lives
here. The client's seeded schedule sends the coins (?prefix=slow, ?case=..., ...)
as query params.
"""

import asyncio
import ssl

from aioquic.asyncio import serve
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import ConnectionTerminated, ProtocolNegotiated
from aioquic.h3.connection import H3Connection
from aioquic.h3.events import DataReceived, HeadersReceived

import modes
from modes import qint, rst_close


# --- H3 wire helpers (varint frame framing, hand-rolled) --------------------
# We deliberately do NOT import aioquic's private FrameType/Setting/encode_uint_var
# (their location has moved across aioquic releases and the repo pins no version):
# a tiny self-contained varint encoder keeps the garbage/badframes modes stable
# whatever aioquic version the image ships.

def encode_varint(value):
    """QUIC/HTTP-3 variable-length integer (RFC 9000 16). 1/2/4/8-byte forms by
    magnitude, the two high bits of the first byte selecting the length."""
    if value < 0:
        raise ValueError("varint must be non-negative")
    if value < 0x40:
        return bytes([value])
    if value < 0x4000:
        return bytes([0x40 | (value >> 8), value & 0xFF])
    if value < 0x40000000:
        return bytes([0x80 | (value >> 24), (value >> 16) & 0xFF,
                      (value >> 8) & 0xFF, value & 0xFF])
    return bytes([0xC0 | ((value >> 56) & 0xFF)]) + bytes(
        (value >> (8 * i)) & 0xFF for i in range(6, -1, -1))


def h3_frame(ftype, payload):
    """One HTTP/3 frame: varint type + varint length + payload (RFC 9114 7.1).
    The hand-rolled path every h3 violation writes through, straight to the QUIC
    stream via send_stream_data, so we control the exact bytes on the wire."""
    return encode_varint(ftype) + encode_varint(len(payload)) + payload


# HTTP/3 frame + stream type constants (RFC 9114). Defined locally, not imported.
H3_FRAME_DATA = 0x0
H3_FRAME_HEADERS = 0x1
H3_FRAME_SETTINGS = 0x4
H3_STREAM_TYPE_CONTROL = 0x0        # unidirectional control stream type
H3_SETTING_MAX_FIELD_SECTION = 0x6  # SETTINGS_MAX_FIELD_SECTION_SIZE

# Application error codes for reset/close. H3_REQUEST_CANCELLED = 0x10c (RFC 9114
# 8.1); the exact value is not load-bearing (tolerant modes accept any typed
# error), it just needs to be a plausible non-zero H3 error.
H3_REQUEST_CANCELLED = 0x10C
H3_INTERNAL_ERROR = 0x102

HEADERBOMB_CAP = 1024 * 1024
  # On-wire cap for the QPACK header-block flood. navi aborts an assembled header
  # block well under 1 MiB and tears the connection down, so the flood only has to
  # comfortably exceed navi's bound to exercise it. The phase-2 lesson applies with
  # extra force on QUIC: this sidecar is single-threaded asyncio and QPACK-encoding
  # a multi-megabyte block pins its CPU, starving its OWN handshake/transmit loop so
  # unrelated chaos connects time out. Capping keeps the client outcome and the heap
  # assertion's teeth identical while the sidecar stays responsive.


# --- TLS / QUIC configuration -----------------------------------------------

def make_quic_config(certfile, keyfile):
    """Server QuicConfiguration pinned to ALPN `h3` with the harness cert/key.
    UDP/QUIC only; there is no TCP listener on the data port at all, so a client
    that is not speaking h3 simply gets no handshake -- it can never be silently
    downgraded to the wrong protocol and trip the version pin."""
    cfg = QuicConfiguration(is_client=False, alpn_protocols=["h3"])
    cfg.load_cert_chain(certfile=certfile, keyfile=keyfile)
    # A generous idle timeout: several chaos modes deliberately go silent for
    # seconds (stall/slowbody). The client's own tight timeouts (2s read / 4s
    # attempt / 10s total) are what resolve those, so the server just must not
    # tear the connection down first and turn a strict TimeoutError into a
    # connection-level abort.
    cfg.idle_timeout = 120.0
    return cfg


# --- per-request mode execution ---------------------------------------------

async def _drip_data(proto, stream_id, length, rate, gap=3.0):
    """Drip an H3 DATA body in ~rate-byte chunks, gap seconds apart, transmitting
    each. Uses H3Connection.send_data (valid framing) so the client sees a proper
    body that simply arrives too slowly. The default gap (3s) is longer than the
    chaos client's per-read stall timeout (2s), so slowbody reliably trips it ->
    a fast, clean TimeoutError. Stops early if the stream/connection went away."""
    chunk = max(1, rate)
    off = 0
    while off < length:
        n = min(chunk, length - off)
        try:
            proto._http.send_data(stream_id, b"\x00" * n, end_stream=False)
            proto.transmit()
        except Exception:
            return
        off += n
        await asyncio.sleep(gap)


async def m_stall(proto, stream_id, params):
    """Parse the request (already done), then never respond: no HEADERS, no DATA,
    no reset. The client's read/attempt/total timeouts must fire -> strict
    TimeoutError. Just return; the connection stays open and silent on this
    stream."""
    return


async def m_slowbody(proto, stream_id, params):
    """Valid 200 + large content-length HEADERS, then drip DATA at ~rate B/s so the
    client stalls mid-body and its read timeout fires (strict TimeoutError, stream
    cleanly cancelled). Defaults: len=1 MiB, rate=1 KiB/s."""
    length = qint(params, "len", 1048576)
    rate = qint(params, "rate", 1024)
    proto._http.send_headers(stream_id, [
        (b":status", b"200"),
        (b"content-type", b"application/octet-stream"),
        (b"content-length", str(length).encode()),
    ], end_stream=False)
    proto.transmit()
    await _drip_data(proto, stream_id, length, rate)


async def m_truncate(proto, stream_id, params):
    """Valid HEADERS + partial DATA, then RESET_STREAM mid-body (RFC 9114): the
    stream is cancelled after 100 of the 4096 promised bytes. The client must never
    surface a successful short-body Response. The schedule sends h1's case names
    (`clen`/`chunked`) which do not apply to h3 framing, so they all resolve to the
    same stream-scoped reset. It stays STREAM-scoped (never a connection-level
    close) so a concurrent stream on the shared h3 connection is not
    collateral-damaged into a spurious error."""
    proto._http.send_headers(stream_id, [
        (b":status", b"200"),
        (b"content-type", b"application/octet-stream"),
        (b"content-length", b"4096"),
    ], end_stream=False)
    proto._http.send_data(stream_id, b"c" * 100, end_stream=False)
    proto.transmit()
    # RESET_STREAM mid-body: the stream is torn down after 100 of 4096 promised
    # bytes; the rest of the connection stays usable for the client's next request.
    try:
        proto._quic.reset_stream(stream_id, H3_REQUEST_CANCELLED)
        proto.transmit()
    except Exception:
        pass


async def m_garbage(proto, stream_id, params):
    """Raw, non-H3-conformant bytes on the RESPONSE stream, bypassing H3Connection
    (`quic.send_stream_data`), rotated by ?case=:
      frametype : a bogus/reserved frame type carrying a wrong-shaped payload
      rawbytes  : arbitrary non-frame bytes (not even a valid varint frame header)
      badhdr    : a HEADERS frame whose payload is not a decodable QPACK block
    Tolerant: any of these must yield a typed error, and a subsequent chaos request
    on a fresh stream/connection must still succeed (nothing sticky is left)."""
    case = params.get("case", "frametype")
    if case == "rawbytes":
        # Not even a valid frame header: a deterministic random-looking blob.
        raw = bytes((i * 37 + 11) & 0xFF for i in range(64))
    elif case == "badhdr":
        # A HEADERS frame whose payload is not a valid QPACK-encoded field section.
        raw = h3_frame(H3_FRAME_HEADERS,
                       bytes((i * 53 + 7) & 0xFF for i in range(48)))
    else:  # frametype
        # A reserved/unknown frame type (0x21) with a deterministic payload on a
        # request stream. Unknown frame types are skippable per RFC 9114 9, but the
        # wrong-shaped payload + no valid HEADERS first is the violation the client
        # must reject rather than hang on.
        raw = h3_frame(0x21, bytes((i * 29 + 3) & 0xFF for i in range(64)))
    try:
        proto._quic.send_stream_data(stream_id, raw, end_stream=True)
        proto.transmit()
    except Exception:
        pass


async def m_vanish(proto, stream_id, params):
    """Mid-response abrupt death. Coins from the schedule:
      ?at=pre-headers : die before any response frame -- reset the request stream
                        immediately (exercises the client's Unprocessed/retry path)
      ?prefix=slow    : drip a slowbody prefix (valid HEADERS + slow DATA) first,
                        then a graceful QUIC CONNECTION_CLOSE mid-body (this is the
                        one shape that legitimately closes the connection: it is a
                        per-connection QUIC close, NOT the shared UDP listener)
    Default: valid HEADERS + partial DATA, then RESET_STREAM mid-body -- the stream
    dies before the promised body completes while the connection stays alive.
    Tolerant, but must resolve within the client's total timeout including retries.

    Critically, NONE of these touch the shared UDP transport: aioquic's serve()
    multiplexes every connection through one datagram socket, so closing that socket
    (an earlier "silent UDP-endpoint drop" attempt) killed the whole data port and
    made every later connect -- notably the strict redirectloop -- fail with
    "connection refused". quic.close() and reset_stream() are per-connection /
    per-stream and leave the listener up for the next worker's requests."""
    at = params.get("at", "")
    if at == "pre-headers":
        # No response frame at all: cancel the stream so the client can retry.
        try:
            proto._quic.reset_stream(stream_id, H3_REQUEST_CANCELLED)
            proto.transmit()
        except Exception:
            pass
        return
    length = qint(params, "after", 8192)
    proto._http.send_headers(stream_id, [
        (b":status", b"200"),
        (b"content-type", b"application/octet-stream"),
        (b"content-length", str(length).encode()),
    ], end_stream=False)
    proto.transmit()
    if params.get("prefix", "") == "slow":
        rate = qint(params, "rate", 1024)
        # Drip a slow prefix of the promised body, then a graceful per-connection
        # QUIC close (CONNECTION_CLOSE frame) mid-body. This closes ONLY this
        # connection, not the shared listener.
        await _drip_data(proto, stream_id, length // 2, rate)
        try:
            proto._quic.close(error_code=H3_INTERNAL_ERROR)
            proto.transmit()
        except Exception:
            pass
    else:
        # Partial body, then RESET_STREAM mid-body: the stream is abruptly torn down
        # before the promised content-length completes; the connection stays usable.
        proto._http.send_data(stream_id, b"s" * (length // 4), end_stream=False)
        proto.transmit()
        try:
            proto._quic.reset_stream(stream_id, H3_REQUEST_CANCELLED)
            proto.transmit()
        except Exception:
            pass


async def m_headerbomb(proto, stream_id, params):
    """One giant QPACK-encoded header block, bounded on the wire at HEADERBOMB_CAP.
    We let H3Connection QPACK-encode a valid-but-enormous field section (many
    x-bomb-N: xxxx... lines) -- if the encoded block would exceed the cap we fall
    back to a raw HEADERS frame padded to the cap so the wire size is bounded
    either way. Tolerant: a typed error (field section too large) or a parsed
    Response both pass so long as memory stays inside the slack bound (the heap
    assertion is the real teeth). h1/h2 pass n/size; we honor them but clamp to the
    cap so the single-threaded sidecar is not pinned encoding tens of MiB."""
    n = qint(params, "n", 4000)
    size = qint(params, "size", 8192)
    # Pick a header-line count that targets ~ the cap without wildly overshooting
    # when size is small (the name overhead dominates then). Clamp the product.
    target = min(max(n * size, 256 * 1024), HEADERBOMB_CAP)
    count = max(1, target // (max(size, 1) + 24))
    headers = [(b":status", b"200")]
    val = b"x" * size
    for i in range(count):
        headers.append((b"x-bomb-%d" % i, val))
    try:
        # H3Connection QPACK-encodes and frames this as a valid HEADERS block. Even
        # bounded to the cap it comfortably exceeds navi's assembled-block limit, so
        # navi rejects it; the point is that navi stays bounded, not that it decodes.
        proto._http.send_headers(stream_id, headers, end_stream=True)
        proto.transmit()
    except Exception:
        # If the encoder itself balks, fall back to a raw HEADERS frame padded to
        # the cap so the flood still reaches the client bounded.
        try:
            proto._quic.send_stream_data(
                stream_id,
                h3_frame(H3_FRAME_HEADERS, b"\x00" * HEADERBOMB_CAP),
                end_stream=True)
            proto.transmit()
        except Exception:
            pass


async def m_badframes(proto, stream_id, params):
    """h3 frame-level protocol violations on the CONTROL streams (not the request
    stream), so H3Connection's own control stream is not the one misbehaving. Two
    shapes rotated by ?case= (h3 ignores the h2 case names the schedule also sends;
    we map any unknown case onto `garbage`):
      garbage (default, incl. h2's bigframe/winupdate0/headers-stream0): open a NEW
        unidirectional control stream, write the control stream-type varint, then
        raw non-frame garbage bytes -- a malformed control stream the client must
        reject (connection error), not hang on.
      initwin / second-settings: open a SECOND control stream carrying a valid
        SETTINGS frame -- RFC 9114 6.2.1 forbids more than one control stream per
        peer, so a duplicate is a connection error.
    Tolerant: violation detected, connection closed, no hang."""
    case = params.get("case", "")
    # h2's case names arrive here (bigframe/initwin/winupdate0/headers-stream0). Map
    # the ~half that name a SETTINGS/window concept onto the duplicate-SETTINGS
    # variant and the rest onto control-stream garbage, so both h3 shapes get
    # exercised deterministically as the schedule rotates the h2 case values.
    dup_settings = case in ("initwin", "second-settings", "winupdate0")
    try:
        uni = proto._quic.get_next_available_stream_id(is_unidirectional=True)
        if dup_settings:
            # A well-formed SETTINGS frame on a NEW control stream. H3Connection
            # already opened the legitimate control stream at handshake, so this is
            # a forbidden second one (RFC 9114 6.2.1 -> H3_STREAM_CREATION_ERROR).
            settings = (encode_varint(H3_SETTING_MAX_FIELD_SECTION) +
                        encode_varint(65536))
            payload = (encode_varint(H3_STREAM_TYPE_CONTROL) +
                       h3_frame(H3_FRAME_SETTINGS, settings))
        else:
            # A control stream (correct stream type) then raw garbage where a
            # SETTINGS frame is required first (RFC 9114 6.2.1 / 9 -> a connection
            # error): the client must reject the malformed control stream.
            payload = (encode_varint(H3_STREAM_TYPE_CONTROL) +
                       bytes((i * 41 + 5) & 0xFF for i in range(48)))
        proto._quic.send_stream_data(uni, payload, end_stream=False)
        proto.transmit()
    except Exception:
        pass
    # Leave the request stream unanswered; the connection-level violation is the
    # point. The client detects it and tears the connection down.


async def m_redirectloop(proto, stream_id, params, target):
    """A protocol-VALID 302 self-loop whose Location bumps ?n each hop; the client
    follows to maxRedirects and must surface a bounded 3xx Response (no error,
    within total timeout). The ONE fully well-behaved mode: served through
    H3Connection. Because the chaos client shares ONE h3 connection per worker
    across ALL modes, each hop arrives as a fresh request stream on this same
    connection -- handle_request already routes each stream here independently, so
    all ~20 hops stay fast (no per-hop handshake) and finish inside the client's
    10s total timeout even under a heavy streaming workload."""
    n = qint(params, "n", 0)
    proto._http.send_headers(stream_id, [
        (b":status", b"302"),
        (b"location", ("/chaos/redirectloop?n=%d" % (n + 1)).encode()),
        (b"content-length", b"0"),
    ], end_stream=True)
    proto.transmit()


# Registered under h3. The schedule filters by proto so only applicable ones are
# drawn; everything except zerowindow (h2-only flow-control starvation) applies.
_H3_MODES = {
    "stall": m_stall,
    "slowbody": m_slowbody,
    "truncate": m_truncate,
    "garbage": m_garbage,
    "vanish": m_vanish,
    "headerbomb": m_headerbomb,
    "redirectloop": m_redirectloop,
    "badframes": m_badframes,
}

for _name, _fn in _H3_MODES.items():
    modes.register("h3", _name, _fn)


# --- data-port protocol (QUIC) ----------------------------------------------

class ChaosH3Protocol(QuicConnectionProtocol):
    """One QUIC connection on the data port. aioquic multiplexes all of its request
    streams through this single instance, so -- unlike h1/h2's one-connection-per-
    misbehavior -- we dispatch each request stream to its mode independently and
    keep the connection alive (the chaos client reuses one h3 connection per worker
    across many modes/streams). H3Connection is the inbound decoder + prelude
    source; violation modes bypass it via self._quic.send_stream_data."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self._http = None
        self._targets = {}   # stream_id -> request target (path?query)
        self._dispatched = set()
        self._log = _current_log

    def quic_event_received(self, event):
        if isinstance(event, ProtocolNegotiated):
            self._http = H3Connection(self._quic)
        elif isinstance(event, ConnectionTerminated):
            return
        if self._http is None:
            return
        try:
            h3_events = self._http.handle_event(event)
        except Exception:
            return
        for e in h3_events:
            self._on_h3_event(e)

    def _on_h3_event(self, e):
        if isinstance(e, HeadersReceived):
            target = None
            for k, v in e.headers:
                if k == b":path":
                    target = v.decode("latin1") if isinstance(v, bytes) else v
            self._targets[e.stream_id] = target or ""
            self._maybe_dispatch(e.stream_id, e.stream_ended)
        elif isinstance(e, DataReceived):
            # Request bodies are drained implicitly by H3Connection; dispatch once
            # the request stream ends (GET requests end immediately, so this is
            # mostly a no-op, but a POST body's end lands here).
            self._maybe_dispatch(e.stream_id, e.stream_ended)

    def _maybe_dispatch(self, stream_id, ended):
        if stream_id in self._dispatched:
            return
        # Dispatch on the first HeadersReceived (GETs); if a body is coming we still
        # dispatch on headers since every chaos request is a GET (no request body).
        self._dispatched.add(stream_id)
        target = self._targets.get(stream_id, "")
        mode, params = modes.parse_target(target)
        handler = modes.handler_for("h3", mode)
        # A stable-ish connection tag for the post-mortem log without depending on
        # aioquic's private network-path internals (which have moved across
        # versions): the QUIC connection object's id is unique per connection.
        peer = "conn-%x" % (id(self._quic) & 0xFFFFFF)
        if handler is None:
            self._log("h3 data %s: %s -> no mode, 404 (stream=%d)"
                      % (peer, target, stream_id))
            try:
                self._http.send_headers(stream_id, [
                    (b":status", b"404"), (b"content-length", b"0")],
                    end_stream=True)
                self.transmit()
            except Exception:
                pass
            return
        self._log("h3 data %s: mode=%s params=%s stream=%d"
                  % (peer, mode, params, stream_id))
        if mode == "redirectloop":
            coro = handler(self, stream_id, params, target)
        else:
            coro = handler(self, stream_id, params)
        # Run the mode as a task so a long-lived mode (slowbody drip, stall) does
        # not block the event loop's packet servicing for other streams.
        task = asyncio.ensure_future(coro)
        task.add_done_callback(self._mode_done)

    def _mode_done(self, task):
        try:
            task.result()
        except (asyncio.CancelledError, Exception):
            pass


# --- accept-time protocols --------------------------------------------------

class VanishOnAcceptProtocol(QuicConnectionProtocol):
    """Close the connection right after the QUIC handshake completes (base+band+1).
    Connect-phase failure: the client gets a typed connect/QUIC error."""

    def quic_event_received(self, event):
        if isinstance(event, ProtocolNegotiated):
            _current_log("h3 vanish-on-accept: closing post-handshake")
            try:
                self._quic.close(error_code=H3_INTERNAL_ERROR)
                self.transmit()
            except Exception:
                pass


class StallOnAcceptProtocol(QuicConnectionProtocol):
    """Complete the QUIC handshake, then total silence (base+band+2): never open a
    control stream, never respond to any request stream. The client's connect/total
    timeout fires (TimeoutError)."""

    def quic_event_received(self, event):
        if isinstance(event, ProtocolNegotiated):
            _current_log("h3 stall-on-accept: handshake done, holding silent")
        # Deliberately ignore all further events.


# --- module-level log shim --------------------------------------------------
# aioquic's create_protocol takes a zero-extra-arg factory (the protocol class),
# so we cannot thread `log` through the constructor like h1/h2's _wrap closures.
# Stash it at module scope for the protocol instances to read; the sidecar is one
# process per cell, so a module global is safe and deterministic.
_current_log = print


# --- TCP Alt-Svc discovery leg ------------------------------------------------
# See the module docstring: navi reaches h3 only via an Alt-Svc advertisement on a
# TCP h1/h2 response, so every chaos band port pairs its QUIC listener with this
# minimal TCP leg. It is a pure function of the request: no `d=1` in the query ->
# a 302 to the same target plus `d=1`, carrying the Alt-Svc pointer at this same
# port; `d=1` present -> abrupt RST (that request is a post-QUIC-failure fallback
# and must surface as an error, not get re-served). It never serves a mode.

DISCOVERY_MARKER = "d=1"


def make_discovery_ssl_context(certfile, keyfile):
    """TLS for the discovery leg, ALPN pinned to http/1.1. navi's h3 bootstrap
    offers ["h2", "http/1.1"]; pinning the server to http/1.1 keeps this leg on
    the simplest possible protocol (plain asyncio, no hyper-h2 needed) and the
    h1/h2 bootstrap is equally pin-exempt either way."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
    ctx.set_alpn_protocols(["http/1.1"])
    return ctx


async def handle_discovery(reader, writer, port, log):
    """One TCP discovery connection: read the request head, then either advertise
    (302 + Alt-Svc when the `d=1` marker is absent) or RST (marker present -> this is
    navi falling back to TCP after a QUIC failure, which must surface as a typed
    error rather than be re-served, and must never loop discovery until maxRedirects
    is spent and leaves a pin-tripping h1 302 as the final response). One request per
    connection, then close; the Alt-Svc points at this same port's QUIC listener."""
    peer = writer.get_extra_info("peername")
    try:
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=10)
    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError,
            asyncio.TimeoutError, OSError):
        rst_close(writer)
        return
    try:
        line0 = head.split(b"\r\n", 1)[0].decode("latin1")
        _method, target, _ver = line0.split(" ", 2)
    except (ValueError, UnicodeDecodeError):
        rst_close(writer)
        return
    if DISCOVERY_MARKER in target:
        log("h3 discovery %s: %s (post-QUIC-failure fallback) -> RST"
            % (peer, target))
        rst_close(writer)
        return
    sep = "&" if "?" in target else "?"
    loc = target + sep + DISCOVERY_MARKER
    altsvc = 'h3=":%d"; ma=86400' % port
    resp = (
        "HTTP/1.1 302 Found\r\n"
        "location: %s\r\n"
        "alt-svc: %s\r\n"
        "content-length: 0\r\n"
        "connection: close\r\n"
        "\r\n" % (loc, altsvc)
    ).encode("latin1")
    writer.write(resp)
    try:
        await writer.drain()
    except OSError:
        pass
    log("h3 discovery %s: %s -> 302 %s (alt-svc %s)" % (peer, target, loc, altsvc))
    try:
        writer.close()
    except OSError:
        pass


# --- server startup ---------------------------------------------------------

async def start(host, base, band, certfile, keyfile, log):
    """Bind the three band ports (data / vanish-on-accept / stall-on-accept) on
    base+band {+0,+1,+2}, mirroring h1/h2.start. Each port gets BOTH a UDP/QUIC
    listener (where the real modes run) and a TCP+TLS discovery leg on the same port
    number -- the Alt-Svc bootstrap that makes the QUIC port reachable at all, since
    navi has no direct-dial h3. TCP and UDP bind the same port number independently.

    Returns objects with an awaitable serve_forever() + close() so the chaos_server
    core can gather + shut them down: asyncio.Server (the discovery legs) has both
    natively; aioquic's QuicServer has neither, so its instances are wrapped in
    _QuicServerShim whose serve_forever() parks and whose close() closes the real
    QuicServer (which the core already invokes in its finally block)."""
    global _current_log
    _current_log = log

    disc_ctx = make_discovery_ssl_context(certfile, keyfile)

    def _disc_cb(advertise_port):
        async def _cb(r, w):
            await handle_discovery(r, w, advertise_port, log)
        return _cb

    servers = []
    for offset, proto_cls in ((0, ChaosH3Protocol),
                              (1, VanishOnAcceptProtocol),
                              (2, StallOnAcceptProtocol)):
        port = base + band + offset
        quic_srv = await serve(
            host, port, configuration=make_quic_config(certfile, keyfile),
            create_protocol=proto_cls)
        servers.append(_QuicServerShim(quic_srv))
        disc_srv = await asyncio.start_server(_disc_cb(port), host, port, ssl=disc_ctx)
        servers.append(disc_srv)

    log("h3 listeners up: data=%d vanish=%d stall=%d "
        "(each QUIC/UDP + TCP discovery on the same port)"
        % (base + band, base + band + 1, base + band + 2))
    return servers


class _QuicServerShim:
    """Adapts aioquic's QuicServer to the interface chaos_server.py expects of an
    asyncio.Server: an awaitable serve_forever() and a close(). aioquic's server
    starts accepting the moment serve() returns and offers no serve_forever(), so
    ours just parks; close() closes the underlying QuicServer's UDP transport."""

    def __init__(self, server):
        self._server = server

    async def serve_forever(self):
        await asyncio.Future()   # park until cancelled by the core's shutdown

    def close(self):
        try:
            self._server.close()
        except Exception:
            pass
