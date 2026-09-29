#!/usr/bin/env python3
"""A TLS server that completes the handshake and then never reads.

Used by tls_write_close.sh: with the peer never draining, the client's kernel send
buffer fills and SSL_write parks on WANT_WRITE, which is the state issue #421 is
about. Connections are held open (never closed) so the stall is the client's to
break. Prints "ready" once listening. Usage: tls_deaf_server.py <port> <cert> <key>
"""
import socket
import ssl
import sys
import threading


def main():
    port, cert, key = int(sys.argv[1]), sys.argv[2], sys.argv[3]
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert, key)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(16)
    print("ready", flush=True)
    held = []
    lock = threading.Lock()

    def accept(conn):
        try:
            tls = ctx.wrap_socket(conn, server_side=True)
        except Exception:
            conn.close()
            return
        with lock:
            held.append(tls)      # keep it open and unread until the process exits

    while True:
        conn, _ = srv.accept()
        threading.Thread(target=accept, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
