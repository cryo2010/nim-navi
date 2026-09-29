#!/usr/bin/env python3
"""A tiny AF_UNIX HTTP/1.1 server for the navi Unix-socket interop test.

Answers 200 on every request with a body equal to the request's Host header, so
the client can assert both a round trip and that the Host header carries the URL
host (not the socket path). Prints "ready" once listening.

With a cert and key it serves TLS over the same AF_UNIX socket instead. The interop
uses a self-signed cert the client does not trust, so every handshake fails: that is
the teardown path issue #427 is about.

Usage: uds_server.py <socket-path> [cert key]
"""
import os
import socket
import ssl
import sys
import threading


def handle(conn):
    try:
        data = conn.recv(65536).decode("latin1")
        host = ""
        for line in data.split("\r\n"):
            if line.lower().startswith("host:"):
                host = line.split(":", 1)[1].strip()
        body = host.encode()
        conn.sendall(
            b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s"
            % (len(body), body)
        )
    except Exception:
        pass
    finally:
        conn.close()


def handle_tls(conn, ctx):
    """Offer the (untrusted) cert and drop the connection however it ends."""
    try:
        with ctx.wrap_socket(conn, server_side=True) as tls:
            handle(tls)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass


def main():
    path = sys.argv[1]
    ctx = None
    if len(sys.argv) > 3:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(sys.argv[2], sys.argv[3])
    try:
        os.unlink(path)
    except OSError:
        pass
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(path)
    srv.listen(16)
    print("ready", flush=True)
    while True:
        conn, _ = srv.accept()
        if ctx is None:
            threading.Thread(target=handle, args=(conn,), daemon=True).start()
        else:
            threading.Thread(target=handle_tls, args=(conn, ctx), daemon=True).start()


if __name__ == "__main__":
    main()
