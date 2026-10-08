#!/usr/bin/env bash
# Per-workload stress harness. Stands up N TLS servers, then builds and runs the
# workload client for each client x protocol cell, distributing requests across
# the servers. Every cell prints a status+RSS report each interval; the three
# checksum-verifying cells (streamUpload, streamDownload and the two stream
# slices of mixed) verify the transfer and fail hard on mismatch.
#
# NAVI_SERVER picks what those servers are:
#   hypercorn (default)  FastAPI via hypercorn for h1/h2, a Caddy front for h3,
#                        and aioquic for an h3 WebSocket, which Caddy cannot
#                        bridge. The interop reference: a green run proves navi
#                        talks to widely deployed servers under load.
#   vortex               one Nim process per instance terminating h1, h2 and
#                        (h3 cells) QUIC natively on the SAME port, WebSocket
#                        included over Extended CONNECT -- so no Caddy hop, no
#                        +1000 backend band and no aioquic ws band. Chosen when
#                        the question is navi's own throughput, h3 behaviour or
#                        fairness rather than interop (see README.md).
#
# Driven by `nimble stress<Workload>` (Dockerized). Config via NAVI_* env.
set -uo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
here="$root/tests/stress"

workload="${NAVI_WORKLOAD:-requests}"
proto="${NAVI_PROTO:-h2}"
client="${NAVI_CLIENT:-all}"
servers="${NAVI_SERVER_COUNT:-5}"
host="${NAVI_HOST:-127.0.0.1}"
base_port="${NAVI_BASE_PORT:-9443}"

# Which origin implementation the cells run against. Validated here, loudly, the
# same way an unknown NAVI_WORKLOAD is (exit 2): a typo must never silently fall
# back to the default and report a vortex run that was really hypercorn.
server="${NAVI_SERVER:-hypercorn}"
case "$server" in
  hypercorn|vortex) ;;
  *) echo "unknown NAVI_SERVER: $server (hypercorn|vortex)"; exit 2 ;;
esac

# Opt-in misbehaving-server (chaos) sidecar. `none` (default) => byte-identical
# to a pre-chaos run: no sidecar, no extra ports, no timeout wrapper. When on,
# per cell a Python asyncio sidecar (tests/stress/chaos/) offers the pinned
# protocol's fault modes and the client attacks it alongside the verified soak.
chaos="${NAVI_CHAOS:-none}"
chaos_band="${NAVI_CHAOS_PORTBAND:-2000}"

# WebSocket-over-h3 band. Only the mixed+h3 cell uses it: Caddy's reverse_proxy
# does not bridge an h3 Extended CONNECT, so that cell needs aioquic ws origins
# on their own ports alongside the Caddy front (see start_ws_h3_servers). The ws
# workload's own h3 cell replaces Caddy outright and keeps the base ports.
ws_band="${NAVI_WS_H3_PORTBAND:-3000}"

# The band indexes real listening ports, so validate it once, loudly, here: a
# non-integer silently becomes 0 (or a bash arithmetic error) and a value that
# collides with another band points the ws slice at the wrong origin or makes two
# servers fight for a port. Exit 2 is the same "bad invocation" code an unknown
# NAVI_WORKLOAD uses.
case "$ws_band" in
  ''|*[!0-9]*) echo "NAVI_WS_H3_PORTBAND must be a positive integer (got '$ws_band')"; exit 2 ;;
esac
[ "$ws_band" -gt 0 ] || {
  echo "NAVI_WS_H3_PORTBAND must be a positive integer (got '$ws_band')"; exit 2; }
# Both collision checks below are about ports the HYPERCORN layout actually
# binds: the h3 backend hypercorns at base+1000 .. base+1000+servers and the
# aioquic ws origins on the band (start_servers). A vortex run binds neither --
# one process serves every protocol on the base port -- so the band indexes
# nothing there and a value that would collide is simply unused. The band is
# still validated as an integer above, because the mixed client parses it either
# way.
if [ "$server" = hypercorn ]; then
  if [ "$ws_band" -ge 1000 ] && [ "$ws_band" -le $((1000 + servers)) ]; then
    echo "NAVI_WS_H3_PORTBAND=$ws_band overlaps the h3 backend band (1000..$((1000 + servers)))"
    exit 2
  fi
  if [ "$chaos" != none ] && [ "$ws_band" -eq "$chaos_band" ]; then
    echo "NAVI_WS_H3_PORTBAND=$ws_band collides with NAVI_CHAOS_PORTBAND=$chaos_band"
    exit 2
  fi
fi

# Which vortex the binary was built from, for the cell banner: the image writes
# "<ref> <runtime>" to /opt/vortex/build-id beside the compile. A throughput
# figure is only reproducible if the server build is named with it, and the
# runtime (sync/async/chronos) is baked in at image build time, so the banner is
# the only place a reader of the log can see either. Empty for hypercorn, which
# keeps its banner byte-identical.
vortex_build=""
# Set per cell by start_vortex_servers and printed after the banner: how much of
# NAVI_RECYCLE this protocol actually gets under vortex.
vortex_notice=""

command -v openssl >/dev/null || { echo "openssl required"; exit 127; }
if [ "$server" = vortex ]; then
  # Only the h3 image carries it: vortex serves TLS and HTTP/3 from one build, so
  # there is no TLS-without-QUIC configuration to put in the lighter image (see
  # Dockerfile.h3). navi.nimble already selects that image for NAVI_SERVER=vortex.
  command -v vortex_server >/dev/null || {
    echo "vortex_server required for NAVI_SERVER=vortex (use the h3 image, tests/stress/Dockerfile.h3)"
    exit 127; }
  if [ -r /opt/vortex/build-id ]; then
    read -r vb_ref vb_rt </opt/vortex/build-id || true
    vortex_build="@${vb_ref:0:12}/${vb_rt:-?}"
  fi
else
  command -v hypercorn >/dev/null || { echo "hypercorn required (pip install -r server/requirements.txt)"; exit 127; }
fi

work="$(mktemp -d)"
cert="$work/cert.pem"; key="$work/key.pem"
pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill -- -"$p" 2>/dev/null || true; done
  # Preserve the server logs outside the mktemp dir before it is removed: on a
  # workload failure the servers' view (a GOAWAY reason, a worker crash, a timeout
  # fired) is the only post-mortem evidence, and the EXIT trap otherwise destroys
  # it with the container still running -- copy, then remove.
  mkdir -p /navi/stress-srv-logs 2>/dev/null && cp "$work"/srv-*.log /navi/stress-srv-logs/ 2>/dev/null
  rm -rf "$work"
}
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

# --- aioquic WebSocket-over-h3 origins --------------------------------------
# Launch N aioquic servers that terminate a WebSocket Extended CONNECT (RFC 9220)
# natively, on $1 + i, and wait for each to print WS_H3_SERVER_READY. Caddy's
# reverse_proxy does not bridge an h3 Extended CONNECT to a backend WebSocket, so
# h3 ws has to be served here. Two callers:
#   - workload=ws,    proto=h3: these REPLACE the Caddy front on the base ports.
#   - workload=mixed, proto=h3: these run on the ws band BESIDE the Caddy front,
#     so the other four slices keep their /echo, /events, /upload and /download.
start_ws_h3_servers() {
  local first="$1" i port
  command -v python3 >/dev/null || { echo "python3 required for h3 ws"; return 1; }
  for ((i=0; i<servers; i++)); do
    port=$((first + i))
    WS_HOST="$host" WS_PORT="$port" WS_CERT="$cert" WS_KEY="$key" \
      setsid python3 "$root/tests/interop/ws_h3/server.py" >"$work/srv-ws-$i.log" 2>&1 &
    pids+=($!)
  done
  for ((i=0; i<servers; i++)); do
    local ok=""
    for _ in $(seq 1 150); do
      grep -q WS_H3_SERVER_READY "$work/srv-ws-$i.log" 2>/dev/null && { ok=1; break; }
      sleep 0.2
    done
    [ -n "$ok" ] || { echo "aioquic ws-h3 server $i did not start"; cat "$work/srv-ws-$i.log"; return 1; }
  done
}

# --- vortex origins ---------------------------------------------------------
# One vortex process per instance on base_port + i, serving h1 + h2 over TLS with
# ALPN on that TCP port and, for an h3 cell, h3 over QUIC on the same UDP port
# (settings.http3, which also turns on vortex's own Alt-Svc advertisement -- the
# client discovers h3 exactly as it does through Caddy's). No Caddy, no +1000
# backend band, no aioquic: the ws h3 cell and the mixed h3 ws slice dial the base
# ports and vortex terminates the Extended CONNECT. The binary is compiled once at
# image build time (Dockerfile.h3), never per cell.
start_vortex_servers() {
  local p="$1" i port
  # Same two lifecycle knobs the hypercorn branch sets, mapped onto vortex:
  #  - NAVI_KEEPALIVE_TIMEOUT -> keepAliveTimeout (idle between requests; the h3
  #    spelling is QUIC's idle timeout). Defaulted past the whole soak as there.
  #  - NAVI_KEEPALIVE_MAX     -> maxRequestsPerSocket, which vortex applies to
  #    HTTP/1 keep-alive only. 0 is unlimited.
  local ka_max="${NAVI_KEEPALIVE_MAX:-0}"
  local ka_to="${NAVI_KEEPALIVE_TIMEOUT:-$(( ${NAVI_SECONDS:-600} + 3600 ))}"
  vortex_notice=""
  if [ "${NAVI_RECYCLE:-0}" != "0" ]; then
    ka_max="${NAVI_KEEPALIVE_MAX:-200}"      # recycle each h1 connection after ~200 requests
    ka_to="${NAVI_KEEPALIVE_TIMEOUT:-2}"     # and idle-close after 2s, every protocol
    # State the real coverage per protocol rather than let the knob imply
    # hypercorn's behaviour. Three different answers, and none of them is a
    # skipped cell (the cell runs either way), so these are notices:
    #  - h1: maxRequestsPerSocket is vortex's only per-connection request
    #    counter, and it applies to HTTP/1 keep-alive. Busy connections really
    #    are recycled here.
    #  - h2: keepAliveTimeout is a true IDLE timer (unlike hypercorn's
    #    keep_alive_timeout, which fires on a busy connection too), so only
    #    pooled-and-quiet connections are churned; an h2 idle close is a GOAWAY.
    #    No request cap is reachable from handler code.
    #  - h3: no coverage at all. vortex advertises the idle window and then arms
    #    ngtcp2's keep-alive PING at a third of it, so a live QUIC connection is
    #    never idle-closed, and there is no request cap either.
    case "$p" in
      h1) vortex_notice="[$workload $p server=vortex] notice: maxRequestsPerSocket=$ka_max caps requests per connection; idle close at ${ka_to}s is idle-only" ;;
      h2) vortex_notice="[$workload $p server=vortex] notice: idle-only recycle (keepAliveTimeout=${ka_to}s); no per-connection request cap" ;;
      h3) vortex_notice="[$workload $p server=vortex] notice: no recycle coverage under vortex (QUIC keep-alive PING defeats the idle close; no request cap)" ;;
    esac
  fi
  local want_h3=0; [ "$p" = h3 ] && want_h3=1
  for ((i=0; i<servers; i++)); do
    port=$((base_port + i))
    NAVI_SERVER_PORT="$port" NAVI_KEY="$key" NAVI_HTTP3="$want_h3" \
      NAVI_KEEPALIVE_TIMEOUT="$ka_to" NAVI_KEEPALIVE_MAX="$ka_max" \
      setsid vortex_server >"$work/srv-$i.log" 2>&1 &
    pids+=($!)
  done
}

# --- hypercorn (+ Caddy for h3) origins -------------------------------------
hypercorn_servers() {
  local p="$1" i port
  # hypercorn's connection-lifecycle defaults are too aggressive for a loopback soak
  # and manufacture spurious transport errors a real keep-alive server would not:
  #  - keep_alive_max_requests (default 1000): closes each h2 connection after 1000
  #    requests -> constant churn under load; an in-flight non-idempotent request at
  #    the recycle then fails un-retryably.
  #  - keep_alive_timeout (default 5s): closes a connection idle for 5s; a >5s stream
  #    (a 1 GiB transfer) or an idle pooled connection between transfers is dropped
  #    mid-flight, which without a client retry crashes the transfer. Measured on
  #    hypercorn 0.18 h2: this timer fires at ~the configured value even on a BUSY
  #    connection (a soak at 13k req/s died in the (t-300, t] window at ka_to=3600
  #    twice, and at ka_to=120 within 120s), so a fixed value silently caps every
  #    connection's lifetime. Default it past the whole soak (NAVI_SECONDS + 1h
  #    headroom) so the steady-state soak never hits it.
  # Raise both (override with NAVI_KEEPALIVE_MAX / NAVI_KEEPALIVE_TIMEOUT).
  #
  # Recycling variant: NAVI_RECYCLE=1 instead LOWERS the limits so the server sends
  # GOAWAY / idle-closes pooled connections mid-soak -- exercising navi's connection
  # recycle + retry path (untested by the steady-state defaults). The stream workloads
  # already retry transient recycles; a soak with `NAVI_RECYCLE=1 nimble stress...`
  # points it at every workload. Both knobs still override the recycling defaults.
  local ka_max="${NAVI_KEEPALIVE_MAX:-1000000000}"
  local ka_to="${NAVI_KEEPALIVE_TIMEOUT:-$(( ${NAVI_SECONDS:-600} + 3600 ))}"
  if [ "${NAVI_RECYCLE:-0}" != "0" ]; then
    ka_max="${NAVI_KEEPALIVE_MAX:-200}"      # recycle each connection after ~200 requests
    ka_to="${NAVI_KEEPALIVE_TIMEOUT:-2}"     # and idle-close after 2s
  fi
  local hcfg="$work/hypercorn.toml"
  {
    printf 'keep_alive_max_requests = %s\n' "$ka_max"
    printf 'keep_alive_timeout = %s\n' "$ka_to"
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
}

# --- start N servers for a given protocol -----------------------------------
# Dispatch to the chosen origin, then wait for readiness the same way for both.
start_servers() {
  local p="$1" i port
  if [ "$server" = hypercorn ] && [ "$p" = "h3" ] && [ "$workload" = "ws" ]; then
    # The ws workload's h3 cell is ws-only, so under hypercorn the aioquic
    # origins take the base ports outright: no Caddy, no hypercorn, and no /echo
    # to curl for readiness. (A vortex origin serves /echo on that same port and
    # terminates the ws itself, so it takes the normal path below.)
    start_ws_h3_servers "$base_port" || return 1
    return 0
  fi
  if [ "$server" = vortex ]; then
    start_vortex_servers "$p" || return 1
  else
    hypercorn_servers "$p" || return 1
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
      [ "$p" = "h3" ] && [ "$server" = hypercorn ] && { echo "--- caddy.log ---"; cat "$work/caddy.log" 2>/dev/null; }
      return 1
    }
  done
  # Under hypercorn the mixed h3 cell needs BOTH origins: Caddy on the base ports
  # (above) for /echo, /events, /upload and /download, and the aioquic ws servers
  # on the ws band for the ws slice. navi direct-dials QUIC for an h3 WebSocket,
  # so the band needs no Alt-Svc discovery leg of its own. A vortex origin needs
  # no band at all: it terminates the h3 Extended CONNECT on the base port, so
  # the ws slice finally shares a QUIC connection with the other four.
  if [ "$server" = hypercorn ] && [ "$p" = "h3" ] && [ "$workload" = "mixed" ]; then
    start_ws_h3_servers "$((base_port + ws_band))" || return 1
  fi
}

# --- launch the chaos sidecar for a cell -------------------------------------
# Canary-first ordering (mirroring vortex): only after start_servers succeeds do
# we bring up the misbehaving sidecar, so the verified soak is never racing it.
# The sidecar offers only the pinned protocol ($pr); its control port is a plain
# well-behaved HTTP /health we poll for readiness (the healthcheck for a server
# whose job is to fail healthchecks). On a start/readiness failure the caller
# dumps the log, stops the servers, and fails the cell. Returns non-zero on
# failure. A no-op (returns 0) when chaos is off.
start_chaos() {
  [ "$chaos" = none ] && return 0
  local pr="$1"
  setsid python3 "$here/chaos/chaos_server.py" --proto "$pr" --host "$host" \
    --base-port "$base_port" --band "$chaos_band" --cert "$cert" --key "$key" \
    >"$work/srv-chaos.log" 2>&1 &
  pids+=($!)
  local hport=$((base_port + chaos_band + 99))
  local ok=""
  for _ in $(seq 1 150); do
    # The control port is plain HTTP (no TLS): curl for the readiness flag.
    if curl -s --max-time 2 "http://$host:$hport/health" 2>/dev/null \
         | grep -q '"ready": true'; then ok=1; break; fi
    sleep 0.2
  done
  [ -n "$ok" ] || { echo "chaos sidecar ($pr) did not become ready"; cat "$work/srv-chaos.log" 2>/dev/null; return 1; }
}

# True once every port a cell uses (public base_port+i, and the h3 backend
# base_port+1000+i under hypercorn) can be bound again -- i.e. no live listener
# is left. Uses SO_REUSEADDR like the servers do, so a port merely in TIME_WAIT
# counts as free. When chaos is on, also check the band's TCP ports
# (data/vanish/stall/control) so the next cell does not race a lingering sidecar
# listener.
ports_free() {
  python3 - "$host" "$base_port" "$servers" "$chaos" "$chaos_band" \
           "$workload" "$ws_band" "$server" <<'PY' 2>/dev/null
import socket, sys
host, base, n = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
chaos, band = sys.argv[4], int(sys.argv[5])
workload, ws_band, server = sys.argv[6], int(sys.argv[7]), sys.argv[8]
ports = []
for i in range(n):
    ports.append(base + i)
    # The +1000 backend band only exists in the hypercorn h3 layout (Caddy in
    # front, hypercorn behind). A vortex origin serves every protocol on the
    # base port, so nothing ever binds the band and checking it proves nothing.
    if server == "hypercorn":
        ports.append(base + 1000 + i)
if chaos != "none":
    ports += [base + band, base + band + 1, base + band + 2, base + band + 99]
# A QUIC listener binds UDP only, so a TCP bind on its port would always succeed
# and prove nothing. Check those with a UDP bind, which does see a lingering
# listener -- and without SO_REUSEADDR, since UDP has no TIME_WAIT to forgive and
# the question here is simply whether anyone is still bound. Which ports those
# are depends on the layout:
#  - vortex serves h3 on the BASE ports (same port as h1/h2), for every workload.
#  - hypercorn's aioquic ws origins sit on the ws band for the mixed cell, and
#    take the BASE ports for the ws workload's own h3 cell (where they replace
#    Caddy, so the TCP loop above can only prove them free of a TCP listener).
if server == "vortex":
    udp_ports = [base + i for i in range(n)]
elif workload == "mixed":
    udp_ports = [base + ws_band + i for i in range(n)]
elif workload == "ws":
    udp_ports = [base + i for i in range(n)]
else:
    udp_ports = []
for p in ports:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try: s.bind((host, p))
    except OSError: sys.exit(1)
    finally: s.close()
for p in udp_ports:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
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
  pkill -9 -f vortex_server 2>/dev/null || true  # ditto for a vortex origin
  pkill -9 -f 'caddy run' 2>/dev/null || true
  pkill -9 -f chaos_server.py 2>/dev/null || true   # reap any stray chaos sidecar
  pkill -9 -f 'ws_h3/server.py' 2>/dev/null || true  # reap any stray aioquic ws origin
  for _ in $(seq 1 100); do ports_free && break; sleep 0.1; done
}

# --- workload -> client source ----------------------------------------------
case "$workload" in
  requests)        src="requests";        js_src="requests_js" ;;
  ws)              src="ws";              js_src="ws_js" ;;
  sse)             src="sse";             js_src="sse_js" ;;
  streamUpload)    src="stream_upload";   js_src="" ;;               # js can't stream uploads
  streamDownload)  src="stream_download"; js_src="stream_download_js" ;;
  mixed)           src="mixed";           js_src="mixed_js" ;;         # all five at once (js: four)
  *) echo "unknown NAVI_WORKLOAD: $workload"; exit 2 ;;
esac

export NAVI_CERT="$cert" NAVI_HOST="$host" NAVI_BASE_PORT="$base_port"
export NAVI_WORKLOAD="$workload" NAVI_SERVER_COUNT="$servers"
export NAVI_WS_H3_PORTBAND="$ws_band"     # the mixed client's h3 ws origins
export NAVI_SERVER="$server"              # mixed's h3 ws slice: band vs base port
export PYTHONPATH="$here/server"          # so hypercorn finds app.py as `app`
cd "$here/server"

common="--path:$root/src -d:ssl -d:release --hints:off"
# The single per-client binary picks its protocol at runtime, so build it with h3
# support whenever the run includes an h3 cell (h3 or all); h1/h2 cells just don't
# use the h3 code. Needs the h3 image's toolchain (nimble stressX selects it).
{ [ "$proto" = "h3" ] || [ "$proto" = "all" ]; } && common="$common -d:naviHttp3"

# Which clients to run (skip those without a source for this workload).
case "$client" in all) clients=(sync asyncdispatch chronos js) ;; *) clients=("$client") ;; esac
case "$proto"   in all) protos=(h1 h2 h3) ;; *) protos=("$proto") ;; esac

fail=0
ran=0        # cells actually executed; a run where every cell was skipped is not a pass
for be in "${clients[@]}"; do
  # locate & build this client's binary
  bin=""
  case "$be" in
    sync)          [ -f "$here/clients/${src}_sync.nim" ] && { bin="$work/${src}_sync"; nim c $common -d:naviStressSync -o:"$bin" "$here/clients/${src}_sync.nim" || fail=1; } ;;
    asyncdispatch) bin="$work/${src}_ad"; nim c $common -o:"$bin" "$here/clients/${src}.nim" || fail=1 ;;
    chronos)       bin="$work/${src}_ch"; nim c $common -d:useChronos -o:"$bin" "$here/clients/${src}.nim" || fail=1 ;;
    js)            [ -n "$js_src" ] && [ -f "$here/clients/${js_src}.nim" ] && { bin="$work/${js_src}.js"; nim js --path:"$root/src" -d:release --hints:off -o:"$bin" "$here/clients/${js_src}.nim" || fail=1; } ;;
  esac
  [ -z "$bin" ] && { echo "[$workload $be] skip: no source for this client/workload"; continue; }

  for pr in "${protos[@]}"; do
    # js/undici has no HTTP/3 (mirrors config.nim skipReason): without this it would
    # fall back to a slow proxied h1/h2 path against the Caddy h3 front, not real h3.
    if [ "$be" = js ] && [ "$pr" = h3 ]; then
      echo "[$workload $pr $be] skip: js/undici has no HTTP/3"; continue
    fi
    # navi/js can't pin undici's HTTP version and can't verify which was negotiated,
    # so h1 and h2 js cells would be identical, unchecked runs. Collapse to one js
    # cell (h1) rather than imply protocol coverage we don't have.
    if [ "$be" = js ] && [ "$pr" != h1 ]; then
      echo "[$workload $pr $be] skip: js/undici protocol not selectable; h1 js is the js coverage"; continue
    fi
    # start fresh servers per protocol (h1/h2 vs h3 differ), run the cell, stop them.
    # On a failed start, stop_servers first so a partially-started cell does not leak
    # its listeners into the next cell's ports.
    start_servers "$pr" || { stop_servers; fail=1; continue; }
    # Then the chaos sidecar (canary-first). A start/readiness failure is a cell
    # failure: dump the log, stop everything, move on.
    start_chaos "$pr" || { stop_servers; fail=1; continue; }
    export NAVI_CLIENT="$be" NAVI_PROTO="$pr"
    chaos_tag=""; [ "$chaos" != none ] && chaos_tag=" | chaos=$chaos"
    # Only named when it is not the default, so a hypercorn log stays
    # byte-identical to a pre-NAVI_SERVER run (same reason as chaos_tag). For
    # vortex the segment also carries the build: server=vortex@<sha12>/<runtime>.
    srv_tag=""; [ "$server" != hypercorn ] && srv_tag=" | server=$server$vortex_build"
    echo "== stress: $workload | $be | $pr | ${servers} servers${srv_tag}${chaos_tag} =="
    # After the banner, not before: a notice belongs to the cell it describes.
    [ -n "$vortex_notice" ] && echo "$vortex_notice"
    # When chaos is on, wrap the cell in coreutils `timeout` as the outermost hang
    # backstop (belt-and-suspenders behind navi's own timeouts and the in-process
    # watchdog, and the sync client's only watchdog): NAVI_SECONDS + 180s slack,
    # SIGKILL 10s after SIGTERM. A timeout expiry is a failure like any other.
    run_cell() {
      if [ "$chaos" != none ]; then
        timeout -k 10 "$(( ${NAVI_SECONDS:-600} + 180 ))" "$@"
      else
        "$@"
      fi
    }
    if [[ "$bin" == *.js ]]; then NODE_EXTRA_CA_CERTS="$cert" run_cell node "$bin" || fail=1
    else run_cell "$bin" || fail=1; fi
    ran=$((ran + 1))
    stop_servers
  done
done

# A matrix that skipped every cell (NAVI_CLIENT=sync with `mixed`, NAVI_CLIENT=js
# with `streamUpload`) used to print "all cells passed" and exit 0: nothing ran,
# nothing failed. Say so instead, and fail, so a typo in NAVI_CLIENT can never
# read as green.
if [ "$ran" -eq 0 ]; then
  echo "== $workload: NO CELLS RAN =="
  exit 1
fi
[ "$fail" -eq 0 ] && echo "== $workload: all cells passed ==" || { echo "== $workload: FAILURES =="; exit 1; }
