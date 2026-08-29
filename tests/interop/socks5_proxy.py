#!/usr/bin/env python3
"""A minimal SOCKS5 proxy for the navi interop test (RFC 1928 + RFC 1929).

Supports the no-auth and username/password methods (selected by the SOCKS_USER /
SOCKS_PASS env vars), CONNECT to a domain or IPv4/IPv6 target, and byte relay.
Not production code -- just enough to exercise navi's SOCKS5 client. Prints
"ready" once it is listening. Usage: socks5_proxy.py <port>
"""
import os
import select
import socket
import sys
import threading

USER = os.environ.get("SOCKS_USER", "")
PASS = os.environ.get("SOCKS_PASS", "")


def recvn(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("client closed")
        buf += chunk
    return buf


def handle(client):
    try:
        recvn(client, 1)                       # VER
        nmethods = recvn(client, 1)[0]
        methods = recvn(client, nmethods)
        if USER:
            if 2 not in methods:
                client.sendall(b"\x05\xff")
                return
            client.sendall(b"\x05\x02")
            recvn(client, 1)                   # auth VER
            ulen = recvn(client, 1)[0]
            user = recvn(client, ulen).decode()
            plen = recvn(client, 1)[0]
            pw = recvn(client, plen).decode()
            if user != USER or pw != PASS:
                client.sendall(b"\x01\x01")
                return
            client.sendall(b"\x01\x00")
        else:
            client.sendall(b"\x05\x00")

        _, _, _, atyp = recvn(client, 4)       # VER, CMD, RSV, ATYP
        if atyp == 3:
            host = recvn(client, recvn(client, 1)[0]).decode()
        elif atyp == 1:
            host = socket.inet_ntoa(recvn(client, 4))
        else:
            host = socket.inet_ntop(socket.AF_INET6, recvn(client, 16))
        port = int.from_bytes(recvn(client, 2), "big")

        try:
            upstream = socket.create_connection((host, port))
        except OSError:
            client.sendall(b"\x05\x04\x00\x01\x00\x00\x00\x00\x00\x00")  # host unreachable
            return
        client.sendall(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")     # success

        peers = [client, upstream]
        while True:
            ready, _, _ = select.select(peers, [], [])
            for s in ready:
                data = s.recv(65536)
                if not data:
                    return
                (upstream if s is client else client).sendall(data)
    except Exception:
        pass
    finally:
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
