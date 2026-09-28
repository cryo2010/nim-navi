#!/usr/bin/env bash
# navi/js binary body round-trip test: run the navi/js client under Node against a
# small HTTP server that streams a 1 MiB body containing every byte value 0..255,
# in many chunks, and echoes back what it received on uploads. Guards the bulk
# Uint8Array <-> Nim conversions that replaced the per-byte jsffi loops (#412) and
# the Uint8Array request body (#417) -- a conversion that mangles bytes > 127, loses
# a chunk boundary, or UTF-8 transcodes an upload still compiles and still passes
# `nim check`, so it has to be run.
# Needs Node 18+ (global fetch) and a Nim toolchain.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
command -v node >/dev/null || { echo "node not found (need Node 18+ for global fetch)"; exit 127; }

work="$(mktemp -d)"
srv=""
cleanup() { [ -n "$srv" ] && kill "$srv" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT

cat > "$work/server.mjs" <<'JS'
import http from 'node:http';
import zlib from 'node:zlib';
// 1 MiB of the repeating 0..255 pattern, written as 64 x 16 KiB chunks so the
// client sees a genuinely multi-chunk body (and a chunk boundary mid-pattern).
const TOTAL = 256 * 4096, CHUNK = 16384;
const full = Buffer.alloc(TOTAL);
for (let i = 0; i < TOTAL; i++) full[i] = i % 256;
// A gzip stream: a real-world binary upload shape, and its header alone (1f 8b)
// already carries a byte no UTF-8 decode survives.
const GZ = zlib.gzipSync(Buffer.from('navi gzip upload payload '.repeat(40)));
const server = http.createServer((req, res) => {
  if (req.url === '/bytes') {
    res.writeHead(200, {'Content-Type': 'application/octet-stream',
                        'Transfer-Encoding': 'chunked'});
    let off = 0;
    const pump = () => {
      while (off < TOTAL) {
        const end = Math.min(off + CHUNK, TOTAL);
        const more = res.write(full.subarray(off, end));
        off = end;
        if (!more) { res.once('drain', pump); return; }
      }
      res.end();
    };
    pump();
  } else if (req.url === '/empty') {
    res.writeHead(200, {'Content-Type': 'application/octet-stream'});
    res.end();
  } else if (req.url === '/gzip') {
    // An opaque gzip blob (no Content-Encoding, so nothing decompresses it): the
    // client downloads it over the byte-exact stream path and uploads it back.
    res.writeHead(200, {'Content-Type': 'application/octet-stream',
                        'Content-Length': GZ.length});
    res.end(GZ);
  } else if (req.url === '/echo') {
    // Report exactly what arrived: length and hex, in headers, so the assertion
    // does not ride on the response body path. The request's Content-Type comes
    // back too, to catch fetch's implicit text/plain default.
    const parts = [];
    req.on('data', (c) => parts.push(c));
    req.on('end', () => {
      const got = Buffer.concat(parts);
      res.writeHead(200, {'Content-Type': 'text/plain',
                          'x-body-len': String(got.length),
                          'x-body-hex': got.toString('hex'),
                          'x-req-ctype': req.headers['content-type'] ?? ''});
      res.end('ok');
    });
  } else {
    res.writeHead(404); res.end();
  }
});
server.listen(9524, '127.0.0.1', () => console.log('ready'));
JS

nim js --hints:off --path:"$root/src" -o:"$work/client.js" \
  "$root/tests/interop/js_bytes_client.nim"

node "$work/server.mjs" >"$work/srv.log" 2>&1 &
srv=$!
disown   # avoid bash's "Terminated" notice when the trap kills it
for _ in $(seq 1 50); do
  grep -q ready "$work/srv.log" 2>/dev/null && break
  sleep 0.1
done

node "$work/client.js"
