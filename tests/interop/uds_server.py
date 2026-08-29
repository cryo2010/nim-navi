#!/usr/bin/env python3
"""A tiny AF_UNIX HTTP/1.1 server for the navi Unix-socket interop test.

Answers 200 on every request with a body equal to the request's Host header, so
the client can assert both a round trip and that the Host header carries the URL
host (not the socket path). Prints "ready" once listening. Usage: uds_server.py <socket-path>
"""
import os
import socket
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


def main():
    path = sys.argv[1]
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
        threading.Thread(target=handle, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
