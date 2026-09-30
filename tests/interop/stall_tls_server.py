#!/usr/bin/env python3
"""A TLS server that stalls its client in a chosen place.

Used by tests/interop/tls_stall.sh (cancellation through the chronos TLS pump) and
tests/interop/tls_budget.sh (the sync backend's establishment and read budgets).

    stall_tls_server.py <cert> <key> <port> <acceptlog> [mode] [delayms]

Appends one line to <acceptlog> per accepted connection, so a test can tell how
many times a request was actually put on the wire (a replay shows up as a second
line).

Modes:
  silent   (default) complete the handshake, read the request, then never answer.
           Connections are kept open and referenced: closing them would hand the
           client an EOF, which is exactly what the test must not see.
  partial  complete the handshake, read the request, wait <delayms>, then write a
           bare 5-byte TLS record header (application_data announcing 64 bytes
           that never arrive) straight onto the socket, under the TLS layer, and
           go silent. The client's readiness wait fires late in its read budget on
           bytes that carry no application data, so SSL_read consumes them and has
           to recv again: a per-syscall receive timeout re-armed with the whole
           budget would double the stall (issue #442). Session tickets are turned
           off so nothing else wakes that wait early.
  hsdeaf   accept the TCP connection and never speak TLS at all: the client's
           ClientHello is never answered and the handshake has to be stopped by
           the client's own establishment budget. No certificate is used.
  answer   complete the handshake, read the request and reply 200 at once: the
           control case, so a bounded and an unbounded handshake are both shown
           to still work against a healthy peer.
"""
import os
import socket
import ssl
import sys
import threading
import time

cert, key, port, acceptlog = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
mode = sys.argv[5] if len(sys.argv) > 5 else "silent"
delay_ms = int(sys.argv[6]) if len(sys.argv) > 6 else 0

ctx = None
if mode != "hsdeaf":
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert, key)
    if mode == "partial":
        # No NewSessionTicket: the only bytes the client ever sees must be the
        # partial record below, so the wait it interrupts is the one being measured.
        ctx.options |= ssl.OP_NO_TICKET
        try:
            ctx.num_tickets = 0          # TLS 1.3 (Python 3.8+)
        except (AttributeError, ValueError):
            pass

srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", port))
srv.listen(16)
print("ready", flush=True)

held = []
lock = threading.Lock()


def note_accept():
    with open(acceptlog, "a") as fh:
        fh.write("accept\n")


def serve(raw):
    """One connection, per the mode. Never closes it: the stall is the client's to
    break, and an EOF would let it call the read a clean end of stream."""
    if mode == "hsdeaf":
        note_accept()
        with lock:
            held.append(raw)             # hold the fd open, say nothing ever
        return
    try:
        tls = ctx.wrap_socket(raw, server_side=True)
    except OSError:
        try:
            raw.close()
        except OSError:
            pass
        return
    note_accept()
    try:
        tls.recv(65536)                  # take the request
        if mode == "answer":
            body = b"ok"
            tls.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n"
                        b"Connection: close\r\n\r\n%s" % (len(body), body))
        elif mode == "partial":
            time.sleep(delay_ms / 1000.0)
            # Under the TLS layer on purpose: os.write bypasses OpenSSL, so these
            # five bytes are a record header whose payload never comes.
            os.write(tls.fileno(), b"\x17\x03\x03\x00\x40")
    except OSError:
        pass
    with lock:
        held.append(tls)


while True:
    conn, _ = srv.accept()
    threading.Thread(target=serve, args=(conn,), daemon=True).start()
