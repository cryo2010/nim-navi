#!/bin/sh
# Generate a throwaway self-signed cert, then serve FastAPI over HTTP/2 + TLS with
# Hypercorn (which advertises h2 via ALPN when TLS is on). The client trusts it by
# disabling verification -- the point of the demo is body integrity, not the cert.
set -e
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout /tmp/key.pem -out /tmp/cert.pem \
  -subj "/CN=server" -addext "subjectAltName=DNS:server,DNS:localhost" >/dev/null 2>&1
exec hypercorn server:app --bind 0.0.0.0:8443 \
  --certfile /tmp/cert.pem --keyfile /tmp/key.pem
