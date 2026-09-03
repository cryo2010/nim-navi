# HTTP/3 server (aioquic) that advertises SETTINGS_ENABLE_CONNECT_PROTOCOL and
# accepts a WebSocket Extended CONNECT (RFC 9220), echoing WebSocket frames back.
# Client frames are masked; server frames are unmasked (RFC 6455). aioquic handles
# QUIC/h3; we do RFC 6455 framing over the CONNECT stream's DATA.
import asyncio, os, struct
from aioquic.asyncio import serve
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import ProtocolNegotiated
from aioquic.h3.connection import H3Connection
from aioquic.h3.events import HeadersReceived, DataReceived


def parse_frames(buf):
    frames, off = [], 0
    while True:
        if len(buf) - off < 2:
            break
        b0, b1 = buf[off], buf[off + 1]
        opcode, masked, ln, p = b0 & 0x0F, (b1 & 0x80) != 0, b1 & 0x7F, off + 2
        if ln == 126:
            if len(buf) - off < 4:
                break
            ln, p = struct.unpack_from("!H", buf, off + 2)[0], off + 4
        elif ln == 127:
            if len(buf) - off < 10:
                break
            ln, p = struct.unpack_from("!Q", buf, off + 2)[0], off + 10
        mask = None
        if masked:
            if len(buf) < p + 4:
                break
            mask, p = buf[p:p + 4], p + 4
        if len(buf) < p + ln:
            break
        payload = bytearray(buf[p:p + ln])
        if masked:
            for i in range(ln):
                payload[i] ^= mask[i & 3]
        frames.append((opcode, bytes(payload)))
        off = p + ln
    return frames, buf[off:]


def encode_frame(opcode, payload):
    n = len(payload)
    if n < 126:
        h = bytes([0x80 | opcode, n])
    elif n <= 0xFFFF:
        h = bytes([0x80 | opcode, 126]) + struct.pack("!H", n)
    else:
        h = bytes([0x80 | opcode, 127]) + struct.pack("!Q", n)
    return h + payload


class WsH3Protocol(QuicConnectionProtocol):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self._http = None
        self._bufs = {}

    def quic_event_received(self, event):
        if isinstance(event, ProtocolNegotiated):
            # enable_webtransport advertises SETTINGS_ENABLE_CONNECT_PROTOCOL=1,
            # which is what an Extended CONNECT (WebSocket) client requires.
            self._http = H3Connection(self._quic, enable_webtransport=True)
        if self._http is not None:
            for e in self._http.handle_event(event):
                self._h3_event(e)

    def _h3_event(self, e):
        if isinstance(e, HeadersReceived):
            hd = dict(e.headers)
            if hd.get(b":method") == b"CONNECT" and hd.get(b":protocol") == b"websocket":
                self._http.send_headers(e.stream_id, [(b":status", b"200")])
                self._bufs[e.stream_id] = b""
            else:
                self._http.send_headers(e.stream_id, [(b":status", b"404")], end_stream=True)
            self.transmit()
        elif isinstance(e, DataReceived) and e.stream_id in self._bufs:
            self._bufs[e.stream_id] += e.data
            frames, self._bufs[e.stream_id] = parse_frames(self._bufs[e.stream_id])
            for opcode, payload in frames:
                self._http.send_data(e.stream_id, encode_frame(opcode, payload), end_stream=False)
            self.transmit()


async def main():
    port = int(os.environ.get("WS_PORT", "4433"))
    cfg = QuicConfiguration(is_client=False, alpn_protocols=["h3"])
    # enable_webtransport (which makes H3Connection advertise
    # SETTINGS_ENABLE_CONNECT_PROTOCOL=1) requires a datagram frame size.
    cfg.max_datagram_frame_size = 65536
    cfg.load_cert_chain(os.environ["WS_CERT"], os.environ["WS_KEY"])
    await serve("127.0.0.1", port, configuration=cfg, create_protocol=WsH3Protocol)
    print("WS_H3_SERVER_READY", flush=True)
    await asyncio.Future()


asyncio.run(main())
