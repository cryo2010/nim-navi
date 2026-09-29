#!/usr/bin/env python3
"""A minimal HTTP CONNECT proxy for the navi interop test (RFC 9110 9.3.6).

Replies in one of three deliberately awkward shapes, chosen with the MODE env
var, to exercise navi's CONNECT reply reader:

  split  the "200 Connection established" status line and the headers go out in
         two writes with a sleep between, so they land in separate TCP segments
  big    a single 200 reply padded past 1 KiB with filler headers
  deny   407 Proxy Authentication Required, connection closed

Not production code -- just enough to exercise navi's CONNECT client. Prints
"ready" once it is listening. Usage: MODE=split http_connect_proxy.py <port>
"""
import os
import select
import socket
import sys
import threading
import time

MODE = os.environ.get("MODE", "split")


def read_head(sock):
    """Read the request head up to and including CRLFCRLF."""
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("client closed")
        buf += chunk
        if len(buf) > 65536:
            raise ConnectionError("request head too large")
    return buf


def relay(client, upstream):
    peers = [client, upstream]
    while True:
        ready, _, _ = select.select(peers, [], [])
        for s in ready:
            data = s.recv(65536)
            if not data:
                return
            (upstream if s is client else client).sendall(data)


def handle(client):
    upstream = None
    try:
        head = read_head(client)
        request = head.split(b"\r\n", 1)[0].decode("latin-1")
        verb, target = request.split(" ")[0], request.split(" ")[1]
        if verb != "CONNECT":
            client.sendall(b"HTTP/1.1 405 Method Not Allowed\r\n"
                           b"Content-Length: 0\r\n\r\n")
            return

        if MODE == "deny":
            client.sendall(b"HTTP/1.1 407 Proxy Authentication Required\r\n"
                           b'Proxy-Authenticate: Basic realm="navi-interop"\r\n'
                           b"Content-Length: 0\r\n\r\n")
            return

        host, port = target.rsplit(":", 1)
        try:
            upstream = socket.create_connection((host, int(port)))
        except OSError:
            client.sendall(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
            return

        if MODE == "split":
            # Status line first, then a pause long enough that the headers are a
            # separate segment: a one-recv reader sees only "HTTP/1.1 200 ...".
            client.sendall(b"HTTP/1.1 200 Connection established\r\n")
            time.sleep(0.2)
            client.sendall(b"Proxy-Agent: navi-interop/1.0\r\n"
                           b"Via: 1.1 navi-interop\r\n\r\n")
        else:  # "big": one 200 reply padded well past a 1 KiB read
            reply = [b"HTTP/1.1 200 Connection established\r\n",
                     b"Proxy-Agent: navi-interop/1.0\r\n"]
            for i in range(40):
                reply.append(b"X-Navi-Filler-%02d: %s\r\n" % (i, b"p" * 48))
            reply.append(b"\r\n")
            blob = b"".join(reply)
            assert len(blob) > 1024, "big reply must exceed one read"
            client.sendall(blob)

        relay(client, upstream)
    except Exception:
        pass
    finally:
        if upstream is not None:
            upstream.close()
        client.close()


def main():
    port = int(sys.argv[1])
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(64)
    print("ready", flush=True)
    while True:
        client, _ = srv.accept()
        threading.Thread(target=handle, args=(client,), daemon=True).start()


if __name__ == "__main__":
    main()
