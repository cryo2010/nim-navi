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
cleanup() { for p in "${pids[@]:-}"; do kill -- -"$p" 2>/dev/null || true; done; rm -rf "$work"; }
trap cleanup EXIT

# Self-signed cert. DNS:127.0.0.1 (not just the IP SAN) so chronos's TLS, which
# matches the connect host against dNSName SANs, accepts the loopback IP.
# `env -u LD_LIBRARY_PATH`: the h3 image points LD_LIBRARY_PATH at the custom
# OpenSSL 3.5 (for the ngtcp2 client), which makes the system `openssl` binary
# load those libs and hunt for a config at /opt/ossl/ssl/openssl.cnf that does not
# exist -- so cert generation fails and Caddy later can't find the cert. The CLI
# only needs the stock system OpenSSL to mint a self-signed cert. `|| exit 1` so a
# failure is loud, not a silently-missing cert.
env -u LD_LIBRARY_PATH openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$key" -out "$cert" -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,DNS:127.0.0.1,IP:127.0.0.1" >/dev/null 2>&1 \
  || { echo "cert generation failed"; exit 1; }

# --- start N servers for a given protocol -----------------------------------
start_servers() {
  local p="$1" i port
  # hypercorn's connection-lifecycle defaults are too aggressive for a loopback soak
  # and manufacture spurious transport errors a real keep-alive server would not:
  #  - keep_alive_max_requests (default 1000): closes each h2 connection after 1000
  #    requests -> constant churn under load; an in-flight non-idempotent request at
  #    the recycle then fails un-retryably.
  #  - keep_alive_timeout (default 5s): closes a connection idle for 5s; a >5s stream
  #    (a 1 GiB transfer) or an idle pooled connection between transfers is dropped
  #    mid-flight, which without a client retry crashes the transfer.
  # Raise both (override with NAVI_KEEPALIVE_MAX / NAVI_KEEPALIVE_TIMEOUT).
  local hcfg="$work/hypercorn.toml"
  {
    printf 'keep_alive_max_requests = %s\n' "${NAVI_KEEPALIVE_MAX:-1000000000}"
    printf 'keep_alive_timeout = %s\n' "${NAVI_KEEPALIVE_TIMEOUT:-3600}"
  } >"$hcfg"
  if [ "$p" = "h3" ]; then
    command -v caddy >/dev/null || { echo "caddy required for h3 (use the h3 image)"; return 1; }
    local caddyfile="$work/Caddyfile"
    # Global options block. The `servers` block MUST be multi-line -- an inline
    # `servers { protocols ... }` is a Caddyfile parse error (Caddy exits, nothing
    # binds). Mirrors the known-good tests/interop/http3/Caddyfile.
    cat >"$caddyfile" <<-EOF
	{
		auto_https off
		servers {
			protocols h1 h2 h3
		}
	}
	EOF
    for ((i=0; i<servers; i++)); do
      port=$((base_port + i))
      local bport=$((base_port + 1000 + i))
      setsid hypercorn "app:app" --bind "127.0.0.1:$bport" --config "$hcfg" >"$work/srv-$i.log" 2>&1 &
      pids+=($!)
      cat >>"$caddyfile" <<-EOF
	https://$host:$port {
		tls $cert $key
		header Alt-Svc \`h3=":$port"; ma=86400\`
		reverse_proxy 127.0.0.1:$bport
	}
	EOF
    done
    setsid caddy run --config "$caddyfile" --adapter caddyfile >"$work/caddy.log" 2>&1 &
    pids+=($!)
  else
    for ((i=0; i<servers; i++)); do
      port=$((base_port + i))
      setsid hypercorn "app:app" --bind "$host:$port" --certfile "$cert" --keyfile "$key" \
        --config "$hcfg" >"$work/srv-$i.log" 2>&1 &
      pids+=($!)
    done
  fi
  # Wait until each public port actually serves a 200 -- not just accepts TLS. For
  # h3 the public port is Caddy; a bare TLS-accept check passes as soon as Caddy is
  # up, before the hypercorn backend behind it is ready, so navi's first request
  # gets a 502. Curling for a real 200 waits for the whole path (Caddy + backend).
  for ((i=0; i<servers; i++)); do
    port=$((base_port + i)); local ok=""
    for _ in $(seq 1 150); do
      if [ "$(curl -sk -o /dev/null -w '%{http_code}' --max-time 2 \
              "https://$host:$port/echo" 2>/dev/null)" = "200" ]; then ok=1; break; fi
      sleep 0.2
    done
    [ -n "$ok" ] || {
      echo "server on :$port did not start"
      cat "$work"/srv-*.log 2>/dev/null
      [ "$p" = "h3" ] && { echo "--- caddy.log ---"; cat "$work/caddy.log" 2>/dev/null; }
      return 1
    }
  done
}

# True once every port a cell uses (public base_port+i, and the h3 backend
# base_port+1000+i) can be bound again -- i.e. no live listener is left. Uses
# SO_REUSEADDR like the servers do, so a port merely in TIME_WAIT counts as free.
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

# Kill the servers AND wait for their ports to actually free up before the next
# cell binds the same ones (otherwise: Address already in use). Each server is a
# `setsid` process-group leader, so kill the whole group (negative pid): that reaps
# hypercorn's multiprocessing worker subprocesses too, whose command line has no
# "hypercorn" for pkill to match and which otherwise stay orphaned holding the
# listening sockets -- shadowing the next cell's server (notably an h3 Caddy, so navi
# never sees Alt-Svc and every request downgrades to h2). Then poll until bindable.
stop_servers() {
  local p
  for p in "${pids[@]:-}"; do kill -9 -"$p" 2>/dev/null || true; done  # -pid = process group
  for p in "${pids[@]:-}"; do wait "$p" 2>/dev/null || true; done
  pids=()
  pkill -9 -f hypercorn 2>/dev/null || true      # belt-and-suspenders for any stray
  pkill -9 -f 'caddy run' 2>/dev/null || true
  for _ in $(seq 1 100); do ports_free && break; sleep 0.1; done
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
# The single per-backend binary picks its protocol at runtime, so build it with h3
# support whenever the run includes an h3 cell (h3 or all); h1/h2 cells just don't
# use the h3 code. Needs the h3 image's toolchain (nimble stressX selects it).
{ [ "$proto" = "h3" ] || [ "$proto" = "all" ]; } && common="$common -d:naviHttp3"

# Which backends to run (skip those without a client source for this workload).
case "$backend" in all) backends=(sync asyncdispatch chronos js) ;; *) backends=("$backend") ;; esac
case "$proto"   in all) protos=(h1 h2 h3) ;; *) protos=("$proto") ;; esac

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
    # js/undici has no HTTP/3 (mirrors config.nim skipReason): without this it would
    # fall back to a slow proxied h1/h2 path against the Caddy h3 front, not real h3.
    if [ "$be" = js ] && [ "$pr" = h3 ]; then
      echo "[$workload $pr $be] skip: js/undici has no HTTP/3"; continue
    fi
    # start fresh servers per protocol (h1/h2 vs h3 differ), run the cell, stop them.
    # On a failed start, stop_servers first so a partially-started cell does not leak
    # its listeners into the next cell's ports.
    start_servers "$pr" || { stop_servers; fail=1; continue; }
    export NAVI_BACKEND="$be" NAVI_PROTO="$pr"
    echo "== stress: $workload | $be | $pr | ${servers} servers =="
    if [[ "$bin" == *.js ]]; then NODE_EXTRA_CA_CERTS="$cert" node "$bin" || fail=1
    else "$bin" || fail=1; fi
    stop_servers
  done
done

[ "$fail" -eq 0 ] && echo "== $workload: all cells passed ==" || { echo "== $workload: FAILURES =="; exit 1; }
