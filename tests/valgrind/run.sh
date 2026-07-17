#!/usr/bin/env bash
# Run navi's HTTPS request loop under Valgrind memcheck against a local nghttpd
# (TLS + HTTP/2), failing on any definite/indirect leak. Meant to run inside the
# Docker image (tests/valgrind/Dockerfile); Valgrind is Linux-only.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
work="$(mktemp -d)"
srv=""
cleanup() { [ -n "$srv" ] && kill "$srv" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT
cd "$work"

port=9443

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout key.pem -out cert.pem \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" >/dev/null 2>&1
mkdir -p htdocs
printf 'hello from nghttpd\n' > htdocs/small.txt

nghttpd -d htdocs "$port" key.pem cert.pem >"$work/nghttpd.log" 2>&1 &
srv=$!
for _ in $(seq 1 50); do
  echo | openssl s_client -alpn h2 -connect "localhost:$port" 2>/dev/null \
    | grep -q "ALPN protocol: h2" && break
  sleep 0.2
done

# -g/--debugger:native for symbolized stacks; -d:useMalloc so Nim allocations go
# through malloc where Valgrind can see them; --mm:orc so scope-exit runs =destroy.
nim c -d:ssl -d:useMalloc --mm:orc -g --debugger:native --hints:off \
  -o:"$work/vg" "$root/tests/valgrind/leak_valgrind.nim"

export NAVI_VG_URL="https://localhost:$port/small.txt"
export NAVI_VG_CERT="$work/cert.pem"
export NAVI_VG_ITERS="${NAVI_VG_ITERS:-50}"

valgrind \
  --leak-check=full \
  --show-leak-kinds=definite,indirect \
  --errors-for-leak-kinds=definite,indirect \
  --error-exitcode=1 \
  --num-callers=25 \
  --suppressions="$root/tests/valgrind/navi.supp" \
  "$work/vg"
