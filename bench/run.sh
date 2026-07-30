#!/usr/bin/env bash
# Build the clients, start the TLS server, run each client against it, and print
# a comparison table. Iteration count overridable via NAVI_BENCH_ITERS.
set -euo pipefail

root=/app
bench="$root/bench"
work="$(mktemp -d)"
export NAVI_BENCH_URL="https://127.0.0.1:8443"
# Modest default so the whole matrix finishes quickly even though std/httpclient
# re-handshakes TLS every request (no connection reuse) and is ~90x slower than
# the pooling clients. Bump it for steadier numbers on the fast clients.
export NAVI_BENCH_ITERS="${NAVI_BENCH_ITERS:-500}"
# Per-client wall-clock cap so one slow/stuck client can't wedge the run.
NAVI_BENCH_TIMEOUT="${NAVI_BENCH_TIMEOUT:-180}"
export NAVI_BENCH_CERT="$work/cert.pem" NAVI_BENCH_KEY="$work/key.pem"

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$work/key.pem" -out "$work/cert.pem" -subj "/CN=127.0.0.1" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" >/dev/null 2>&1

echo "building server + clients..."
(cd "$bench/server" && go build -o "$work/server" .)
(cd "$bench/clients/go_client" && go build -o "$work/go_client" .)
for c in navi_sync navi_async std_sync std_async; do
  nim c -d:release -d:ssl --hints:off --path:"$root/src" -o:"$work/$c" \
    "$bench/clients/$c.nim" >/dev/null 2>&1
done
rust_bin="$bench/clients/rust_client/target/release/rust_client"

"$work/server" & srv=$!
trap 'kill "$srv" 2>/dev/null || true; rm -rf "$work"' EXIT
ready=""
for _ in $(seq 1 60); do
  if echo | openssl s_client -connect 127.0.0.1:8443 2>/dev/null | grep -q "BEGIN CERT"; then
    ready=1; break
  fi
  sleep 0.2
done
[ -n "$ready" ] || { echo "server did not come up"; exit 1; }

echo ""
echo "=== HTTP client benchmark: TLS (HTTP/1.1) + gzip + all methods ==="
echo "    $NAVI_BENCH_ITERS iterations x 7 methods = $((NAVI_BENCH_ITERS * 7)) requests/client, pooled connection"
echo ""

tmp="$work/results"; : > "$tmp"
run() { # name binary
  local out
  if out="$(timeout "$NAVI_BENCH_TIMEOUT" "$2" 2>/dev/null)" && echo "$out" | grep -q '^RESULT'; then
    echo "$out" | grep '^RESULT' >> "$tmp"
    echo "  $1: done"
  else
    echo "  $1: FAILED or exceeded ${NAVI_BENCH_TIMEOUT}s"
  fi
}
run navi-sync   "$work/navi_sync"
run navi-async  "$work/navi_async"
run std-sync    "$work/std_sync"
run std-async   "$work/std_async"
run go          "$work/go_client"
run rust        "$rust_bin"

echo ""
max="$(sort -t$'\t' -k5 -nr "$tmp" | head -1 | cut -f5)"
printf "%-14s %10s %9s %12s %8s\n" CLIENT REQUESTS "TIME(s)" "REQ/S" REL
printf -- "------------------------------------------------------------\n"
sort -t$'\t' -k5 -nr "$tmp" | while IFS=$'\t' read -r _ name req sec rps; do
  rel="$(awk -v r="$rps" -v m="$max" 'BEGIN{printf "%.0f", r / m * 100}')"
  printf "%-14s %10s %9s %12s %7s%%\n" "$name" "$req" "$sec" "$rps" "$rel"
done
