#!/usr/bin/env bash
# Per-workload benchmark harness. Stands up N fast Go TLS servers (a Caddy front for
# h3), then for each protocol builds+runs every applicable client -- navi's four
# backends plus the cross-language reference clients (Go/Rust/Node/Python/std) -- and
# prints one ranked throughput+latency table per (workload, protocol) cell. Clients
# are time-boxed (NAVI_SECONDS) and record per-request latency; the streaming cells
# also verify a SHA-1 and fail hard on mismatch.
#
# Driven by `nimble bench<Workload>` (Dockerized). Config via NAVI_* env.
set -uo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
here="$root/tests/bench"

workload="${NAVI_WORKLOAD:-requests}"
proto="${NAVI_PROTO:-h2}"
backend="${NAVI_BACKEND:-all}"      # navi backends: sync|asyncdispatch|chronos|js|all
langs="${NAVI_LANGS:-all}"          # reference langs: all|navi|go|rust|node|python|std (csv ok)
# navi's async backends are single-threaded (one event loop = one core), like Node;
# you scale them across cores by running one process per core (as you would in
# production), NOT by threading the client. So navi's native backends run NAVI_PROCS
# processes in parallel with the total concurrency split across them, and the harness
# sums their throughput -- an apples-to-apples multi-core comparison with Go/Rust
# (which use all cores within one process). Default: the machine's core count.
procs="${NAVI_PROCS:-$(nproc 2>/dev/null || echo 1)}"
servers="${NAVI_SERVERS:-5}"
host="${NAVI_HOST:-127.0.0.1}"
base_port="${NAVI_BASE_PORT:-9443}"

command -v openssl >/dev/null || { echo "openssl required"; exit 127; }
command -v go >/dev/null || { echo "go required"; exit 127; }

work="$(mktemp -d)"
cert="$work/cert.pem"; key="$work/key.pem"
pids=()
cleanup() { for p in "${pids[@]:-}"; do kill -9 -- -"$p" 2>/dev/null || true; done; rm -rf "$work"; }
trap cleanup EXIT

# Self-signed cert (DNS:127.0.0.1 SAN so chronos's dNSName match accepts the loopback
# IP). env -u LD_LIBRARY_PATH: the h3 image points it at the custom OpenSSL 3.5, which
# breaks the system openssl CLI's config lookup.
env -u LD_LIBRARY_PATH openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$key" -out "$cert" -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,DNS:127.0.0.1,IP:127.0.0.1" >/dev/null 2>&1 \
  || { echo "cert generation failed"; exit 1; }

echo "building the Go bench server..."
(cd "$here/server" && go build -o "$work/server" .) || { echo "server build failed"; exit 1; }

# --- start N servers for a given protocol ------------------------------------
start_servers() {
  local p="$1" i port bport
  if [ "$p" = "h3" ]; then
    command -v caddy >/dev/null || { echo "caddy required for h3 (use the h3 image)"; return 1; }
    local caddyfile="$work/Caddyfile"
    cat >"$caddyfile" <<-EOF
	{
		auto_https off
		servers {
			protocols h1 h2 h3
		}
	}
	EOF
    for ((i=0; i<servers; i++)); do
      port=$((base_port + i)); bport=$((base_port + 1000 + i))
      setsid env NAVI_BENCH_ADDR="127.0.0.1:$bport" NAVI_BENCH_CERT="$cert" NAVI_BENCH_KEY="$key" \
        "$work/server" >"$work/srv-$i.log" 2>&1 &
      pids+=($!)
      cat >>"$caddyfile" <<-EOF
	https://$host:$port {
		tls $cert $key
		header Alt-Svc \`h3=":$port"; ma=86400\`
		reverse_proxy https://127.0.0.1:$bport {
			transport http {
				tls_insecure_skip_verify
			}
		}
	}
	EOF
    done
    setsid caddy run --config "$caddyfile" --adapter caddyfile >"$work/caddy.log" 2>&1 &
    pids+=($!)
  else
    for ((i=0; i<servers; i++)); do
      port=$((base_port + i))
      setsid env NAVI_BENCH_ADDR="$host:$port" NAVI_BENCH_CERT="$cert" NAVI_BENCH_KEY="$key" \
        "$work/server" >"$work/srv-$i.log" 2>&1 &
      pids+=($!)
    done
  fi
  # Wait until each public port serves a real 200 (for h3 that's the whole Caddy+Go path).
  for ((i=0; i<servers; i++)); do
    port=$((base_port + i)); local ok=""
    for _ in $(seq 1 150); do
      if [ "$(curl -sk -o /dev/null -w '%{http_code}' --max-time 2 \
              "https://$host:$port/echo" 2>/dev/null)" = "200" ]; then ok=1; break; fi
      sleep 0.2
    done
    [ -n "$ok" ] || { echo "server on :$port did not start"; cat "$work"/srv-*.log 2>/dev/null
      [ "$p" = "h3" ] && { echo "--- caddy.log ---"; cat "$work/caddy.log" 2>/dev/null; }; return 1; }
  done
}

ports_free() {
  python3 - "$host" "$base_port" "$servers" <<'PY' 2>/dev/null
import socket, sys
host, base, n = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
for i in range(n):
    for p in (base + i, base + 1000 + i):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try: s.bind((host, p))
        except OSError: sys.exit(1)
        finally: s.close()
PY
}

stop_servers() {
  local p
  for p in "${pids[@]:-}"; do kill -9 -"$p" 2>/dev/null || true; done
  for p in "${pids[@]:-}"; do wait "$p" 2>/dev/null || true; done
  pids=()
  pkill -9 -f "$work/server" 2>/dev/null || true
  pkill -9 -f 'caddy run' 2>/dev/null || true
  for _ in $(seq 1 100); do ports_free && break; sleep 0.1; done
}

# --- workload -> client source ------------------------------------------------
case "$workload" in
  requests)        src="requests";        js_src="requests_js" ;;
  ws)              src="ws";              js_src="ws_js" ;;
  sse)             src="sse";             js_src="sse_js" ;;
  streamUpload)    src="stream_upload";   js_src="" ;;
  streamDownload)  src="stream_download"; js_src="stream_download_js" ;;
  *) echo "unknown NAVI_WORKLOAD: $workload"; exit 2 ;;
esac

export NAVI_CERT="$cert" NAVI_HOST="$host" NAVI_BASE_PORT="$base_port"
export NAVI_WORKLOAD="$workload" NAVI_SERVERS="$servers"

common="--path:$root/src -d:ssl -d:release --hints:off"
{ [ "$proto" = "h3" ] || [ "$proto" = "all" ]; } && common="$common -d:naviHttp3"

case "$proto" in all) protos=(h1 h2 h3) ;; *) protos=("$proto") ;; esac
# WebSocket is an h1 upgrade (reference WS libs are h1-only), so ws runs h1 only.
[ "$workload" = ws ] && protos=(h1)
case "$backend" in all) navi_backends=(sync asyncdispatch chronos js) ;; *) navi_backends=("$backend") ;; esac

want_lang() { case ",$langs," in *,all,*|*",$1,"*) return 0 ;; *) return 1 ;; esac; }

# --- build the clients once (each picks proto/mode at runtime via env) --------
declare -A NAVI_BIN
for be in "${navi_backends[@]}"; do
  case "$be" in
    sync)          [ -f "$here/clients/${src}_sync.nim" ] && { nim c $common -o:"$work/${src}_sync" "$here/clients/${src}_sync.nim" && NAVI_BIN[sync]="$work/${src}_sync"; } ;;
    asyncdispatch) nim c $common -o:"$work/${src}_ad" "$here/clients/${src}.nim" && NAVI_BIN[asyncdispatch]="$work/${src}_ad" ;;
    chronos)       nim c $common -d:useChronos -o:"$work/${src}_ch" "$here/clients/${src}.nim" && NAVI_BIN[chronos]="$work/${src}_ch" ;;
    js)            [ -n "$js_src" ] && [ -f "$here/clients/${js_src}.nim" ] && { nim js --path:"$root/src" -d:release --hints:off -o:"$work/${js_src}.js" "$here/clients/${js_src}.nim" && NAVI_BIN[js]="node"; } ;;
  esac
done

# Reference clients (built if their source is present and the lang is wanted).
GO_BIN=""; RUST_BIN=""; STD_SYNC=""; STD_ASYNC=""
if want_lang go && [ -f "$here/clients/go_client/main.go" ]; then
  (cd "$here/clients/go_client" && go build -o "$work/go_client" .) && GO_BIN="$work/go_client"
fi
if want_lang rust; then
  RUST_BIN="$here/clients/rust_client/target/release/rust_client"
  [ -x "$RUST_BIN" ] || RUST_BIN=""
fi
if want_lang std && [ -f "$here/clients/std_sync.nim" ]; then
  nim c $common -o:"$work/std_sync" "$here/clients/std_sync.nim" && STD_SYNC="$work/std_sync"
  nim c $common -o:"$work/std_async" "$here/clients/std_async.nim" && STD_ASYNC="$work/std_async"
fi

# run_client <displayname> <cellfile> <cmd...>: run one client, collect its RESULT
# line (or note a SKIP / crash). A crash without RESULT sets the global fail flag.
fail=0
run_client() {
  local name="$1" cell="$2"; shift 2
  local out
  if out="$("$@" 2>&1)"; then
    if echo "$out" | grep -q '^RESULT'; then
      echo "$out" | grep '^RESULT' >> "$cell"
    elif echo "$out" | grep -q '^SKIP'; then
      echo "  $name: $(echo "$out" | grep '^SKIP' | cut -f3-)"
    else
      echo "  $name: no RESULT"; echo "$out" | tail -3; fail=1
    fi
  else
    echo "  $name: FAILED"; echo "$out" | tail -5; fail=1
  fi
}

print_table() {   # <cellfile> <workload> <proto>
  local cell="$1" wl="$2" pr="$3"
  [ -s "$cell" ] || { echo "  (no results)"; return; }
  echo ""
  echo "== bench: $wl | $pr | ${servers} servers =="
  local max; max="$(sort -t$'\t' -k5 -nr "$cell" | head -1 | cut -f5)"
  printf "%-14s %11s %8s %11s %9s %9s %9s %9s %6s\n" \
    CLIENT REQUESTS "TIME(s)" "REQ/S" p50ms p99ms p999ms "MB/s" REL
  printf -- "--------------------------------------------------------------------------------------------\n"
  sort -t$'\t' -k5 -nr "$cell" | while IFS=$'\t' read -r _ name req sec rps p50 p99 p999 mbps; do
    local rel; rel="$(awk -v r="$rps" -v m="$max" 'BEGIN{printf "%.0f", (m>0? r/m*100 : 0)}')"
    printf "%-14s %11s %8s %11s %9s %9s %9s %9s %5s%%\n" \
      "$name" "$req" "$sec" "$rps" "$p50" "$p99" "$p999" "$mbps" "$rel"
  done
}

# Run a navi native backend across `procs` parallel processes (one event loop per
# core) with the total concurrency split across them, then aggregate into one row:
# sum ops/req-per-s/MB-per-s (true multi-core throughput), average the per-stream
# latency percentiles. p=1 is handled by the caller (plain run_client).
run_navi_multi() {   # <displayname> <cellfile> <bin>
  local name="$1" cell="$2" bin="$3"
  local total=$(( ${NAVI_CLIENTS:-3} * ${NAVI_CONCURRENCY:-8} )); [ "$total" -lt 1 ] && total=1
  local p=$procs; [ "$p" -gt "$total" ] && p=$total; [ "$p" -lt 1 ] && p=1
  local per=$(( (total + p - 1) / p ))          # per-process concurrency (ceil)
  local base="$work/mp.$name"; local kids=(); local k
  for ((k=0; k<p; k++)); do
    ( NAVI_CLIENTS=1 NAVI_CONCURRENCY="$per" "$bin" >"$base.$k.out" 2>"$base.$k.err" ) &
    kids+=($!)
  done
  for ((k=0; k<p; k++)); do wait "${kids[k]}" || true; done
  local got="$base.all"; : > "$got"; local n=0
  for ((k=0; k<p; k++)); do
    if grep -q '^RESULT' "$base.$k.out" 2>/dev/null; then
      grep '^RESULT' "$base.$k.out" >> "$got"; n=$((n+1))
    else
      echo "  $name[proc $k]: no RESULT"; tail -3 "$base.$k.out" "$base.$k.err" 2>/dev/null; fail=1
    fi
  done
  [ "$n" -gt 0 ] || { fail=1; return; }
  awk -F'\t' -v name="$name" '
    { ops+=$3; if($4>secs)secs=$4; rps+=$5; p50+=$6; p99+=$7; p999+=$8; mbps+=$9; c++ }
    END { printf "RESULT\t%s\t%d\t%.3f\t%d\t%.3f\t%.3f\t%.3f\t%.1f\n",
          name, ops, secs, rps, p50/c, p99/c, p999/c, mbps }' "$got" >> "$cell"
}

run_cell() {   # <proto>: run every applicable client for this protocol, print table
  local pr="$1" cell="$work/cell.$pr"; : > "$cell"
  export NAVI_PROTO="$pr"
  # navi backends
  for be in "${navi_backends[@]}"; do
    [ -n "${NAVI_BIN[$be]:-}" ] || continue
    export NAVI_BACKEND="$be"
    if [ "$be" = js ]; then
      [ -n "$js_src" ] || { echo "  [navi-js]: skip (no js client for $workload)"; continue; }
      [ "$pr" = h3 ] && { echo "  [navi-js $pr]: skip js/undici has no HTTP/3"; continue; }
      run_client "navi-js" "$cell" env NODE_EXTRA_CA_CERTS="$cert" node "$work/${js_src}.js"
    else
      local dn="navi-$be"; [ "$be" = asyncdispatch ] && dn="navi-async"  # match clientName
      if [ "$procs" -le 1 ]; then run_client "$dn" "$cell" "${NAVI_BIN[$be]}"
      else run_navi_multi "$dn" "$cell" "${NAVI_BIN[$be]}"; fi
    fi
  done
  # reference clients (h3 is navi-only). Each reads NAVI_WORKLOAD/NAVI_PROTO from the
  # env and self-skips (prints a SKIP line) any workload/protocol it does not support.
  if [ "$pr" != h3 ]; then
    [ -n "$GO_BIN" ]    && run_client "go"     "$cell" "$GO_BIN"
    [ -n "$RUST_BIN" ]  && run_client "rust"   "$cell" "$RUST_BIN"
    want_lang node   && [ -f "$here/clients/node_client.js" ]  && run_client "node"   "$cell" env NODE_EXTRA_CA_CERTS="$cert" node "$here/clients/node_client.js"
    want_lang python && [ -f "$here/clients/python_client.py" ] && run_client "python" "$cell" python3 "$here/clients/python_client.py"
    [ -n "$STD_SYNC" ]  && run_client "std-sync"  "$cell" "$STD_SYNC"
    [ -n "$STD_ASYNC" ] && run_client "std-async" "$cell" "$STD_ASYNC"
  fi
  print_table "$cell" "$workload" "$pr"
}

run_matrix() {
  for pr in "${protos[@]}"; do
    start_servers "$pr" || { stop_servers; fail=1; continue; }
    run_cell "$pr"
    stop_servers
  done
}

echo ""
echo "=== navi bench: $workload | protos: ${protos[*]} | langs: $langs | ${servers} servers | navi procs: $procs ==="
run_matrix

# Optional lossy-link regime (h3 vs h2 head-of-line blocking). Needs iproute2 + NET_ADMIN.
if [ "${NAVI_NETEM:-0}" = 1 ]; then
  delay="${NAVI_NETEM_DELAY:-25ms}"; loss="${NAVI_NETEM_LOSS:-1.5%}"
  if command -v tc >/dev/null 2>&1 && tc qdisc add dev lo root netem delay "$delay" loss "$loss" 2>/dev/null; then
    echo ""
    echo "=== netem regime: $delay each-way delay + $loss loss ==="
    run_matrix
    tc qdisc del dev lo root 2>/dev/null || true
  else
    echo "  (NAVI_NETEM set but netem unavailable -- needs iproute2 + --cap-add=NET_ADMIN)"
  fi
fi

[ "$fail" -eq 0 ] && echo "== $workload: all cells ran ==" || { echo "== $workload: FAILURES =="; exit 1; }
