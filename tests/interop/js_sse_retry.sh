#!/usr/bin/env bash
# navi/js SSE reconnect-delay runtime test (#291): run the navi/js SSE client under
# Node against a server that sends `retry: 0` and one that answers 200 and closes
# with no events. Verifies the floor under the reconnect delay and the backoff on
# repeated empty connects. Needs Node 18+ (global fetch) and a Nim toolchain.
# `nim check -b:js` is not enough (a prior js bug shipped that way), so this runs
# the compiled JS under Node.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
command -v node >/dev/null || { echo "node not found (need Node 18+ for global fetch)"; exit 127; }

work="$(mktemp -d)"
srv=""
cleanup() { [ -n "$srv" ] && kill "$srv" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT

cat > "$work/server.mjs" <<'JS'
import http from 'node:http';
let flapHits = 0;
const head = {'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache'};
const server = http.createServer((req, res) => {
  if (req.url === '/floor') {
    // Every connection: a zero reconnect delay plus one event, then close.
    res.writeHead(200, head);
    res.end('retry: 0\ndata: tick\n\n');
  } else if (req.url === '/flap') {
    // The first four connections close with nothing in them.
    flapHits++;
    res.writeHead(200, head);
    res.end(flapHits > 4 ? 'data: done\n\n' : '');
  } else {
    res.writeHead(404); res.end('nope');
  }
});
server.listen(9533, '127.0.0.1', () => console.log('ready'));
JS

nim js --hints:off --path:"$root/src" -o:"$work/client.js" \
  "$root/tests/interop/js_sse_retry_client.nim"

node "$work/server.mjs" >"$work/srv.log" 2>&1 &
srv=$!
disown   # avoid bash's "Terminated" notice when the trap kills it
for _ in $(seq 1 50); do
  grep -q ready "$work/srv.log" 2>/dev/null && break
  sleep 0.1
done

node "$work/client.js"
