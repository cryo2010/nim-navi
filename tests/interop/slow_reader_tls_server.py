#!/usr/bin/env python3
"""A TLS server that finishes the handshake, sends one small message, and then
never reads from the connection again.

Used by tests/interop/tls_read_during_write.sh: the client's outbound write fills
the socket buffers and parks, and the small message has to reach the client's
read pump anyway (issue #444). The listening socket carries a tiny SO_RCVBUF so
the receive window stays small and a few MiB of ciphertext is guaranteed to block
the client's write rather than disappear into kernel buffers.

    slow_reader_tls_server.py <cert> <key> <port> [greeting]

Connections are kept open and referenced: closing one would hand the client an
EOF, which would make the test pass for the wrong reason.
"""
import socket
import ssl
import sys

cert, key, port = sys.argv[1], sys.argv[2], int(sys.argv[3])
greeting = (sys.argv[4] if len(sys.argv) > 4 else "PING").encode()

ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.load_cert_chain(cert, key)

srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
# Inherited by every accepted socket, so the advertised window stays tiny.
srv.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
srv.bind(("127.0.0.1", port))
srv.listen(16)
print("ready", flush=True)

held = []
while True:
    raw, _ = srv.accept()
    try:
        tls = ctx.wrap_socket(raw, server_side=True)
        tls.sendall(greeting)    # one record, then never read again
        held.append(tls)
    except OSError:
        try:
            raw.close()
        except OSError:
            pass
