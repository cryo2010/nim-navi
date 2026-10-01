#!/usr/bin/env python3
"""A TCP listener that accepts and then never sends a byte, so a TLS ClientHello
is never answered and the client is left parked inside its handshake.

Used by tests/interop/connect_abandon.sh, which runs one instance per loopback
family on the SAME port so that `localhost` becomes a two-address Happy-Eyeballs
pool whose every member stalls.

    deaf_tcp_server.py <4|6> <port> <acceptlog>

Appends one line to <acceptlog> per accepted connection, so the driver can count
how many times navi actually put a connection on the wire: a connect that was
abandoned on a timeout must not add a second one. Connections are kept open and
referenced; closing them would hand the client an EOF and end the stall.
"""
import socket
import sys

which, port, acceptlog = sys.argv[1], int(sys.argv[2]), sys.argv[3]
family = socket.AF_INET6 if which == "6" else socket.AF_INET
address = "::1" if which == "6" else "127.0.0.1"

srv = socket.socket(family, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind((address, port))
srv.listen(16)
print("ready", flush=True)

held = []
while True:
    conn, _ = srv.accept()
    with open(acceptlog, "a") as fh:
        fh.write("accept %s\n" % which)
    held.append(conn)
