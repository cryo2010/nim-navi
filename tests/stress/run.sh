#!/usr/bin/env bash
# Per-workload stress harness. Stands up N TLS servers (FastAPI via hypercorn for
# h1/h2; a Caddy front for h3), then builds and runs the workload client for each
# backend x protocol cell, distributing requests across the servers. Every cell
# prints a status+RSS report each interval; the streaming cells verify a 1 GiB
# checksum and fail hard on mismatch.
#
# Driven by `nimble stress<Workload>` (Dockerized). Config via NAVI_* env.
set -uo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
here="$root/tests/stress"

workload="${NAVI_WORKLOAD:-requests}"
proto="${NAVI_PROTO:-h2}"
backend="${NAVI_BACKEND:-all}"
servers="${NAVI_SERVERS:-5}"
host="${NAVI_HOST:-127.0.0.1}"
base_port="${NAVI_BASE_PORT:-9443}"

command -v openssl >/dev/null || { echo "openssl required"; exit 127; }
command -v hypercorn >/dev/null || { echo "hypercorn required (pip install -r server/requirements.txt)"; exit 127; }

work="$(mktemp -d)"
cert="$work/cert.pem"; key="$work/key.pem"
pids=()
cleanup() { for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$work"; }
trap cleanup EXIT

# Self-signed cert. DNS:127.0.0.1 (not just the IP SAN) so chronos's TLS, which
# matches the connect host against dNSName SANs, accepts the loopback IP.
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$key" -out "$cert" -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,DNS:127.0.0.1,IP:127.0.0.1" >/dev/null 2>&1

# --- start N servers for a given protocol -----------------------------------
start_servers() {
  local p="$1" i port
  if [ "$p" = "h3" ]; then
    command -v caddy >/dev/null || { echo "caddy required for h3 (use the h3 image)"; return 1; }
    local caddyfile="$work/Caddyfile"; : >"$caddyfile"
    printf '{\n\tauto_https off\n\tservers { protocols h1 h2 h3 }\n}\n' >>"$caddyfile"
    for ((i=0; i<servers; i++)); do
      port=$((base_port + i))
      local bport=$((base_port + 1000 + i))
      hypercorn "app:app" --bind "127.0.0.1:$bport" >"$work/srv-$i.log" 2>&1 &
      pids+=($!)
      printf 'https://%s:%s {\n\ttls %s %s\n\theader Alt-Svc `h3=":%s"; ma=86400`\n\treverse_proxy 127.0.0.1:%s\n}\n' \
        "$host" "$port" "$cert" "$key" "$port" "$bport" >>"$caddyfile"
    done
    caddy run --config "$caddyfile" --adapter caddyfile >"$work/caddy.log" 2>&1 &
    pids+=($!)
  else
    for ((i=0; i<servers; i++)); do
      port=$((base_port + i))
      hypercorn "app:app" --bind "$host:$port" --certfile "$cert" --keyfile "$key" \
        >"$work/srv-$i.log" 2>&1 &
      pids+=($!)
    done
  fi
  # Wait for each public TLS listener to accept.
  for ((i=0; i<servers; i++)); do
    port=$((base_port + i)); local ok=""
    for _ in $(seq 1 100); do
      if openssl s_client -connect "$host:$port" </dev/null >/dev/null 2>&1; then ok=1; break; fi
      sleep 0.2
    done
    [ -n "$ok" ] || { echo "server on :$port did not start"; cat "$work"/srv-*.log; return 1; }
  done
}

# Kill the servers AND wait for them to fully exit, so their listening ports are
# released before the next cell binds the same ones (otherwise: Address in use).
stop_servers() {
  local p
  for p in "${pids[@]:-}"; do kill -9 "$p" 2>/dev/null || true; done
  for p in "${pids[@]:-}"; do wait "$p" 2>/dev/null || true; done
  pids=()
}

# --- workload -> client source ----------------------------------------------
case "$workload" in
  requests)        src="requests";        js_src="requests_js" ;;
  ws)              src="ws";              js_src="ws_js" ;;
  sse)             src="sse";             js_src="sse_js" ;;
  streamUpload)    src="stream_upload";   js_src="" ;;               # js can't stream uploads
  streamDownload)  src="stream_download"; js_src="stream_download_js" ;;
  *) echo "unknown NAVI_WORKLOAD: $workload"; exit 2 ;;
esac

export NAVI_CERT="$cert" NAVI_HOST="$host" NAVI_BASE_PORT="$base_port"
export NAVI_WORKLOAD="$workload" NAVI_SERVERS="$servers"
export PYTHONPATH="$here/server"          # so hypercorn finds app.py as `app`
cd "$here/server"

common="--path:$root/src -d:ssl -d:release --hints:off"
[ "$proto" = "h3" ] && common="$common -d:naviHttp3"

# Which backends to run (skip those without a client source for this workload).
case "$backend" in all) backends=(sync asyncdispatch chronos js) ;; *) backends=("$backend") ;; esac
case "$proto"   in all) protos=(h1 h2) ;; *) protos=("$proto") ;; esac   # h3 opt-in via PROTO=h3

fail=0
for be in "${backends[@]}"; do
  # locate & build this backend's client binary
  bin=""
  case "$be" in
    sync)          [ -f "$here/clients/${src}_sync.nim" ] && { bin="$work/${src}_sync"; nim c $common -o:"$bin" "$here/clients/${src}_sync.nim" || fail=1; } ;;
    asyncdispatch) bin="$work/${src}_ad"; nim c $common -o:"$bin" "$here/clients/${src}.nim" || fail=1 ;;
    chronos)       bin="$work/${src}_ch"; nim c $common -d:useChronos -o:"$bin" "$here/clients/${src}.nim" || fail=1 ;;
    js)            [ -n "$js_src" ] && [ -f "$here/clients/${js_src}.nim" ] && { bin="$work/${js_src}.js"; nim js --path:"$root/src" -d:release --hints:off -o:"$bin" "$here/clients/${js_src}.nim" || fail=1; } ;;
  esac
  [ -z "$bin" ] && { echo "[$workload $be] skip: no client for this backend/workload"; continue; }

  for pr in "${protos[@]}"; do
    # start fresh servers per protocol (h1/h2 vs h3 differ), run the cell, stop them
    start_servers "$pr" || { fail=1; continue; }
    export NAVI_BACKEND="$be" NAVI_PROTO="$pr"
    echo "== stress: $workload | $be | $pr | ${servers} servers =="
    if [[ "$bin" == *.js ]]; then NODE_EXTRA_CA_CERTS="$cert" node "$bin" || fail=1
    else "$bin" || fail=1; fi
    stop_servers
  done
done

[ "$fail" -eq 0 ] && echo "== $workload: all cells passed ==" || { echo "== $workload: FAILURES =="; exit 1; }
