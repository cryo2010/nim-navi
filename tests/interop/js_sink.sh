#!/usr/bin/env bash
# navi/js response-sink runtime test: run the navi/js client under Node against a
# small HTTP server that streams a chunked body, retries once (503 then 200), and
# serves a 404. Verifies the gated response sink on js: chunked delivery, bool-stop
# truncation, the void form, the delivery rule (retry final body only), and that a
# thrown non-2xx never calls the sink. Needs Node 18+ (global fetch) and a Nim
# toolchain. nim check -b:js is not enough (a prior js bug shipped that way), so
# this runs the compiled JS under Node.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
command -v node >/dev/null || { echo "node not found (need Node 18+ for global fetch)"; exit 127; }

work="$(mktemp -d)"
srv=""
cleanup() { [ -n "$srv" ] && kill "$srv" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT

cat > "$work/server.mjs" <<'EOF'
import http from 'node:http';
let retryHits = 0;
const server = http.createServer((req, res) => {
  if (req.url === '/body') {
    res.writeHead(200, {'Content-Type': 'text/plain', 'Transfer-Encoding': 'chunked'});
    res.write('Hello, ');
    res.write('chunked ');
    res.end('world!');
  } else if (req.url === '/retry') {
    retryHits++;
    if (retryHits === 1) { res.writeHead(503); res.end('unavailable'); }
    else { res.writeHead(200); res.end('recovered'); }
  } else if (req.url === '/notfound') {
    res.writeHead(404); res.end('not found here');
  } else {
    res.writeHead(200); res.end('ok');
  }
});
server.listen(9522, '127.0.0.1', () => console.log('ready'));
EOF

nim js --hints:off --path:"$root/src" -o:"$work/client.js" \
  "$root/tests/interop/js_sink_client.nim"

node "$work/server.mjs" >"$work/srv.log" 2>&1 &
srv=$!
disown   # avoid bash's "Terminated" notice when the trap kills it
for _ in $(seq 1 50); do
  grep -q ready "$work/srv.log" 2>/dev/null && break
  sleep 0.1
done

node "$work/client.js"
