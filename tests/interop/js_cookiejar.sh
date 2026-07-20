#!/usr/bin/env bash
# navi/js opt-in cookie jar runtime test: run the navi/js client under Node
# against a small HTTP server that sets a cookie and echoes the Cookie header it
# receives. Verifies the jar replays a cookie across requests on Node (undici,
# which has no cookie store of its own) and that the default does not. Needs Node
# 18+ (global fetch) and a Nim toolchain.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
command -v node >/dev/null || { echo "node not found (need Node 18+ for global fetch)"; exit 127; }

work="$(mktemp -d)"
srv=""
cleanup() { [ -n "$srv" ] && kill "$srv" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT

cat > "$work/server.mjs" <<'EOF'
import http from 'node:http';
const server = http.createServer((req, res) => {
  res.setHeader('Set-Cookie', 'sid=abc123; Path=/');
  res.end('cookie:' + (req.headers.cookie ?? 'none'));
});
server.listen(9521, '127.0.0.1', () => console.log('ready'));
EOF

nim js --hints:off --path:"$root/src" -o:"$work/client.js" \
  "$root/tests/interop/js_cookiejar_client.nim"

node "$work/server.mjs" >"$work/srv.log" 2>&1 &
srv=$!
disown   # avoid bash's "Terminated" notice when the trap kills it
for _ in $(seq 1 50); do
  grep -q ready "$work/srv.log" 2>/dev/null && break
  sleep 0.1
done

node "$work/client.js"
