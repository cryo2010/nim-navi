#!/usr/bin/env python3
"""A TLS server that completes the handshake, reads the request and then never
answers, so the client is left parked inside its TLS read pump.

Used by tests/interop/tls_stall.sh to exercise the cancellation paths: a read
timeout, a total-request deadline and a CancelToken all have to reach a client
whose OpenSSL pump is blocked on the transport.

    stall_tls_server.py <cert> <key> <port> <acceptlog>

Appends one line to <acceptlog> per accepted connection, so a test can tell how
many times a request was actually put on the wire (a replay shows up as a second
line). Connections are kept open and referenced: closing them would hand the
client an EOF, which is exactly what the test must not see.
"""
import socket
import ssl
import sys

cert, key, port, acceptlog = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]

ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.load_cert_chain(cert, key)

srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", port))
srv.listen(16)
print("ready", flush=True)

held = []
while True:
    raw, _ = srv.accept()
    try:
        tls = ctx.wrap_socket(raw, server_side=True)
        with open(acceptlog, "a") as fh:
            fh.write("accept\n")
        tls.recv(65536)          # take the request, then answer nothing, ever
        held.append(tls)         # keep it open so the client really does stall
    except OSError:
        try:
            raw.close()
        except OSError:
            pass
