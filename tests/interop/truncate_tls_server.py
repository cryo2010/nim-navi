#!/usr/bin/env python3
"""A TLS server that ends a response in each of the ways a body can end, so the
client's handling of an unauthenticated close can be tested (issue #426).

Used by tests/interop/tls_truncate.sh. The request path picks the ending:

    /clean      a body delimited only by the close, then unwrap() -> close_notify
    /rst        the same body cut short, then a RST with no close_notify
    /fin        the same body cut short, then a bare FIN with no close_notify
    /cl-short   a Content-Length body cut short, closed cleanly
    /keepalive  an ordinary delimited response, connection kept open

    truncate_tls_server.py <cert> <key> <port>

The cut-short endings pause before closing so the client has taken the partial
body off the wire: a RST discards whatever is still queued in the kernel, and
the point of the test is a client that parsed the headers and then lost the
rest, not one that never saw a response at all.
"""
import socket
import ssl
import struct
import sys
import threading
import time

cert, key, port = sys.argv[1], sys.argv[2], int(sys.argv[3])

ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.load_cert_chain(cert, key)

BODY = b"the body the server meant to send in full"
PART = b"the body the server"          # all a truncated read ever gets

HEAD_UNTIL_CLOSE = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n"


def read_request(tls):
    """Read one request head; returns its path, or None at end of stream."""
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = tls.recv(4096)
        if not chunk:
            return None
        data += chunk
    return data.split(b" ")[1].decode()


def reset(tls):
    """Close with a RST: SO_LINGER with a zero timeout skips the FIN handshake.
    The send queue is already empty (the body was written and acknowledged
    before the pause), so nothing the client still needs is discarded."""
    tls.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    tls.close()


def bare_fin(tls):
    """Close with a plain FIN and no close_notify: detach() drops the TLS layer
    without shutting the session down, leaving an ordinary socket to close."""
    fd = tls.detach()
    socket.socket(fileno=fd).close()


def serve(raw):
    try:
        tls = ctx.wrap_socket(raw, server_side=True)
    except OSError:
        raw.close()
        return
    tls.settimeout(20)
    try:
        while True:
            path = read_request(tls)
            if path is None:
                break
            if path == "/keepalive":
                tls.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                continue
            if path == "/clean":
                tls.sendall(HEAD_UNTIL_CLOSE + BODY)
                tls.unwrap()          # close_notify: the body provably ended here
                break
            if path == "/cl-short":
                tls.sendall(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: "
                            + str(len(BODY)).encode() + b"\r\n\r\n" + PART)
                tls.unwrap()          # a clean close, but the framing says more
                break
            tls.sendall(HEAD_UNTIL_CLOSE + PART)   # /rst and /fin
            time.sleep(0.4)
            if path == "/fin":
                bare_fin(tls)
            else:
                reset(tls)
            return
    except OSError:
        pass
    try:
        tls.close()
    except OSError:
        pass


srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", port))
srv.listen(16)
print("ready", flush=True)

while True:
    conn, _ = srv.accept()
    threading.Thread(target=serve, args=(conn,), daemon=True).start()
