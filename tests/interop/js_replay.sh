#!/usr/bin/env bash
# navi/js keep-alive race replay + fetch error cause runtime test: run the navi/js
# client under Node against a server that destroys the socket of a request before
# any response. A keyed POST and a GET must be replayed (RFC 9110 9.2.2); an unkeyed
# POST must surface a KeepAliveRaceError whose message carries undici's `err.cause`;
# a mid-body drop must be a catchable IOError; a refused connect stays a plain
# IOError. Needs Node 18+ (global fetch) and a Nim toolchain. `nim check -b:js` is
# not enough (a prior js bug shipped that way), so this runs the compiled JS under Node.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
command -v node >/dev/null || { echo "node not found (need Node 18+ for global fetch)"; exit 127; }

work="$(mktemp -d)"
srv=""
cleanup() { [ -n "$srv" ] && kill "$srv" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT

cat > "$work/server.mjs" <<'JS'
import http from 'node:http';
const seen = new Map();
const server = http.createServer((req, res) => {
  let body = '';
  req.on('data', (d) => { body += d; });
  req.on('end', () => {
    if (req.url === '/hits') {
      res.writeHead(200); res.end(String(seen.get(req.headers['x-id']) || 0)); return;
    }
    const id = req.headers['x-id'] || '';
    const n = (seen.get(id) || 0) + 1;
    seen.set(id, n);
    if (req.url === '/reset' && n === 1) { req.socket.destroy(); return; }
    if (req.url === '/always') { req.socket.destroy(); return; }
    if (req.url === '/midbody') {
      res.writeHead(200, {'content-length': '100'});
      res.write('partial');
      setTimeout(() => req.socket.destroy(), 20);
      return;
    }
    res.writeHead(200, {'content-type': 'text/plain'});
    res.end('ok ' + n + ' ' + body);
  });
});
server.listen(9534, '127.0.0.1', () => console.log('ready'));
JS

nim js --hints:off --path:"$root/src" -o:"$work/client.js" \
  "$root/tests/interop/js_replay_client.nim"

node "$work/server.mjs" >"$work/srv.log" 2>&1 &
srv=$!
disown   # avoid bash's "Terminated" notice when the trap kills it
for _ in $(seq 1 50); do
  grep -q ready "$work/srv.log" 2>/dev/null && break
  sleep 0.1
done

node "$work/client.js"
