#!/usr/bin/env bash
# Build the clients, start the TLS server, run each client against it, and print
# a comparison table. Iteration count overridable via NAVI_BENCH_ITERS.
set -euo pipefail

# The h3 libs (OpenSSL 3.5 in /opt/ossl) must NOT be on the global library path,
# or the system openssl and other clients linked against system OpenSSL 3.0 load
# the wrong libs and crash. navi_proto finds them via an rpath baked in at link.
unset LD_LIBRARY_PATH || true

root=/app
bench="$root/bench"
work="$(mktemp -d)"
export NAVI_BENCH_URL="https://127.0.0.1:8443"
# Modest default so the whole matrix finishes quickly even though std/httpclient
# re-handshakes TLS every request (no connection reuse) and is ~90x slower than
# the pooling clients. Bump it for steadier numbers on the fast clients.
export NAVI_BENCH_ITERS="${NAVI_BENCH_ITERS:-500}"
# Cold phase (fresh connection per request) is far slower per request -- a full
# TCP+TLS handshake every time -- so it runs fewer iterations by default.
NAVI_BENCH_COLD_ITERS="${NAVI_BENCH_COLD_ITERS:-200}"
# The protocol matrix runs after run_phase, which mutates NAVI_BENCH_ITERS; keep
# the original pooled count for it.
PROTO_ITERS="$NAVI_BENCH_ITERS"
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
# navi across protocols (h1/h2/h3), built with the HTTP/3 backend enabled. An
# rpath lets the binary find the /opt h3 libs at runtime without LD_LIBRARY_PATH.
nim c -d:release -d:ssl -d:naviHttp3 --hints:off --path:"$root/src" \
  --passL:"-Wl,-rpath,/opt/ossl/lib -Wl,-rpath,/opt/nghttp3/lib -Wl,-rpath,/opt/ngtcp2/lib" \
  -o:"$work/navi_proto" "$bench/clients/navi_proto.nim" >/dev/null 2>&1 \
  || echo "note: navi_proto (h3) build failed; the protocol matrix will be skipped"
# Interpreted clients: tiny launchers so the run() helper can exec them uniformly.
printf '#!/usr/bin/env bash\nexec node "%s"\n' "$bench/clients/node_client.js" > "$work/node_client"
printf '#!/usr/bin/env bash\nexec python3 "%s"\n' "$bench/clients/python_client.py" > "$work/python_client"
chmod +x "$work/node_client" "$work/python_client"
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

# Run the whole client matrix once and print a ranked table. Args: results file,
# cold flag (0/1), iterations. The clients read NAVI_BENCH_COLD / NAVI_BENCH_ITERS
# from the environment, so export them here for the child processes.
run_phase() {
  local tmp="$1"
  export NAVI_BENCH_COLD="$2" NAVI_BENCH_ITERS="$3"
  : > "$tmp"
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
  run node        "$work/node_client"
  run python      "$work/python_client"

  echo ""
  local max
  max="$(sort -t$'\t' -k5 -nr "$tmp" | head -1 | cut -f5)"
  printf "%-14s %10s %9s %12s %8s\n" CLIENT REQUESTS "TIME(s)" "REQ/S" REL
  printf -- "------------------------------------------------------------\n"
  sort -t$'\t' -k5 -nr "$tmp" | while IFS=$'\t' read -r _ name req sec rps; do
    local rel
    rel="$(awk -v r="$rps" -v m="$max" 'BEGIN{printf "%.0f", r / m * 100}')"
    printf "%-14s %10s %9s %12s %7s%%\n" "$name" "$req" "$sec" "$rps" "$rel"
  done
}

echo ""
echo "=== HTTP client benchmark: TLS (HTTP/1.1) + gzip + all methods ==="
echo ""
echo "-- pooled: one kept-alive connection reused across requests --"
echo "   $NAVI_BENCH_ITERS iterations x 7 methods = $((NAVI_BENCH_ITERS * 7)) requests/client"
echo ""
run_phase "$work/pooled" 0 "$NAVI_BENCH_ITERS"

echo ""
echo "-- cold: a fresh TCP+TLS connection per request (connection setup) --"
echo "   $NAVI_BENCH_COLD_ITERS iterations x 7 methods = $((NAVI_BENCH_COLD_ITERS * 7)) requests/client"
echo ""
run_phase "$work/cold" 1 "$NAVI_BENCH_COLD_ITERS"

# --- navi protocol matrix: each protocol (h1/h2/h3), cold vs pooled ---
# Runs navi against a Caddy origin that speaks all three protocols, so h1/h2/h3
# are compared on the same server. GET-only (connection/protocol overhead, not
# the 7-method body workload above). Skipped if the h3 build/toolchain is absent.
proto_matrix() {  # one 6-cell table; reads NAVI_BENCH_CONC etc. from the env
  printf "%-10s %-8s %10s %9s %12s\n" PROTOCOL MODE REQUESTS "TIME(s)" "REQ/S"
  printf -- "------------------------------------------------------------\n"
  for proto in h1 h2 h3; do
    for mode in pooled cold; do
      local_cold=0; its="$PROTO_ITERS"
      [ "$mode" = cold ] && { local_cold=1; its="$NAVI_BENCH_COLD_ITERS"; }
      out="$(NAVI_BENCH_PROTO="$proto" NAVI_BENCH_COLD="$local_cold" NAVI_BENCH_ITERS="$its" \
             timeout "$NAVI_BENCH_TIMEOUT" "$work/navi_proto" 2>/dev/null | grep '^RESULT' || true)"
      if [ -n "$out" ]; then
        echo "$out" | awk -F'\t' -v p="$proto" -v m="$mode" \
          '{printf "%-10s %-8s %10s %9s %12s\n", p, m, $3, $4, $5}'
      else
        printf "%-10s %-8s %10s\n" "$proto" "$mode" "FAILED"
      fi
    done
  done
}

if [ -x "$work/navi_proto" ] && command -v caddy >/dev/null 2>&1; then
  netem_conc="${NAVI_BENCH_CONC:-16}"     # concurrency for the netem regime
  export NAVI_BENCH_URL="https://localhost:4433/"
  caddy start --config "$bench/Caddyfile" --adapter caddyfile >/tmp/caddy.log 2>&1 || true
  cready=""
  for _ in $(seq 1 40); do
    if curl -sk --max-time 2 https://localhost:4433/ >/dev/null 2>&1; then cready=1; break; fi
    sleep 0.25
  done
  if [ -z "$cready" ]; then
    echo ""; echo "   (Caddy h3 origin did not come up; skipping the protocol matrix)"
  else
    echo ""
    echo "=== navi protocol matrix: HTTP/1.1 / HTTP/2 / HTTP/3, cold vs pooled (GET, TLS) ==="
    echo "   clean loopback, one request at a time -- raw protocol/connection overhead"
    echo "   pooled: $PROTO_ITERS iters reused   cold: $NAVI_BENCH_COLD_ITERS iters, fresh conn each"
    echo "   (h3 cold also pays an h1/h2 Alt-Svc discovery round trip per request)"
    echo ""
    export NAVI_BENCH_CONC=1
    proto_matrix

    # Optional: the same matrix under emulated latency + loss with concurrent
    # requests -- the regime where h3 pulls ahead of h2 (h2's single connection
    # suffers TCP head-of-line blocking across streams; h3's are independent).
    # Needs `tc` (iproute2) and NET_ADMIN; set NAVI_BENCH_NETEM=1 to enable.
    if [ "${NAVI_BENCH_NETEM:-0}" = 1 ]; then
      delay="${NAVI_BENCH_NETEM_DELAY:-25ms}"; loss="${NAVI_BENCH_NETEM_LOSS:-1.5%}"
      if command -v tc >/dev/null 2>&1 && \
         tc qdisc add dev lo root netem delay "$delay" loss "$loss" 2>/dev/null; then
        export NAVI_BENCH_CONC="$netem_conc"
        echo ""
        echo "=== the same matrix under netem: $delay each-way delay + $loss loss, $netem_conc concurrent ==="
        echo "   emulates a lossy/high-latency link; here h3 avoids the head-of-line"
        echo "   blocking h2 suffers, so h3 pooled should beat h2 pooled"
        echo ""
        proto_matrix
        tc qdisc del dev lo root 2>/dev/null || true
      else
        echo ""
        echo "   (NAVI_BENCH_NETEM set but netem is unavailable -- needs iproute2 and"
        echo "    --cap-add=NET_ADMIN; skipping the netem regime)"
      fi
    fi
  fi
  caddy stop 2>/dev/null || true
fi
