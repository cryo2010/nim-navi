// TLS test server for the backend stress harness (tests/stress). Node.js so the
// fixture's TLS/HTTP/WebSocket handling is production-grade -- the earlier Nim
// servers hit ARC refcount races (thread-per-connection) or asyncnet SSL write
// bugs (async). The thing under test is the navi *clients*, not this server.
//
// - any HTTP method on /echo: 200 echoing the request method and x-stress header
//   (as x-echo-* response headers) and the request body verbatim, reflecting its
//   Content-Type and Content-Length. A Content-Encoding request body is
//   decompressed; if the client sent x-want-encoding the echoed body is
//   (re)compressed with it. HEAD replies headers only with a non-zero
//   Content-Length (to exercise the client's HEAD handling).
// - a WebSocket upgrade on /ws: RFC 6455 handshake, then echoes every frame.
//
// Env: NAVI_STRESS_HOST, NAVI_STRESS_PORT, NAVI_STRESS_CERT, NAVI_STRESS_KEY.
import https from 'node:https';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import zlib from 'node:zlib';

const host = process.env.NAVI_STRESS_HOST ?? '127.0.0.1';
const port = parseInt(process.env.NAVI_STRESS_PORT ?? '9443');
const cert = readFileSync(process.env.NAVI_STRESS_CERT);
const key = readFileSync(process.env.NAVI_STRESS_KEY);

const decode = (buf, enc) =>
  enc === 'gzip' ? zlib.gunzipSync(buf) : enc ? zlib.inflateSync(buf) : buf;
const encode = (buf, enc) =>
  enc === 'gzip' ? zlib.gzipSync(buf) : zlib.deflateSync(buf);   // "deflate" = zlib-wrapped

const server = https.createServer({ cert, key }, (req, res) => {
  const chunks = [];
  req.on('data', (c) => chunks.push(c));
  req.on('end', () => {
    const stress = req.headers['x-stress'] ?? '';
    const ct = req.headers['content-type'];
    const reqEnc = req.headers['content-encoding'];
    const wantEnc = req.headers['x-want-encoding'];
    const headers = { 'x-echo-method': req.method, 'x-echo-stress': stress, 'Connection': 'keep-alive' };
    if (req.method === 'HEAD') {                       // headers only, non-zero length
      res.writeHead(200, { ...headers, 'Content-Type': 'application/octet-stream', 'Content-Length': 24 });
      return res.end();
    }
    const payload = decode(Buffer.concat(chunks), reqEnc);
    let body = payload;
    if (wantEnc && payload.length > 0) { body = encode(payload, wantEnc); headers['Content-Encoding'] = wantEnc; }
    if (ct) headers['Content-Type'] = ct;
    headers['Content-Length'] = body.length;
    res.writeHead(200, headers);
    res.end(body);
  });
});

// --- WebSocket echo (RFC 6455), no dependencies ---
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
server.on('upgrade', (req, socket) => {
  const accept = createHash('sha1').update(req.headers['sec-websocket-key'] + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n' +
               'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  let buf = Buffer.alloc(0);
  socket.on('data', (d) => {
    buf = Buffer.concat([buf, d]);
    for (;;) {
      if (buf.length < 2) return;
      const op = buf[0] & 0x0f, masked = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      const need = off + (masked ? 4 : 0) + len;
      if (buf.length < need) return;
      let payload;
      if (masked) {
        const mask = buf.subarray(off, off + 4);
        payload = Buffer.from(buf.subarray(off + 4, need));
        for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      } else payload = Buffer.from(buf.subarray(off, need));
      buf = buf.subarray(need);
      if (op === 0x8) return socket.end();               // close
      if (op === 0x1 || op === 0x2 || op === 0x9) {       // text/binary/ping -> echo (unmasked)
        const echoOp = op === 0x9 ? 0xA : op;             // ping -> pong
        const head = len < 126 ? Buffer.from([0x80 | echoOp, len])
          : len < 65536 ? Buffer.concat([Buffer.from([0x80 | echoOp, 126]), (() => { const b = Buffer.alloc(2); b.writeUInt16BE(len); return b; })()])
          : Buffer.concat([Buffer.from([0x80 | echoOp, 127]), (() => { const b = Buffer.alloc(8); b.writeBigUInt64BE(BigInt(len)); return b; })()]);
        socket.write(Buffer.concat([head, payload]));
      }
    }
  });
  socket.on('error', () => {});
});

server.listen(port, host, () => console.log(`stress server on https://${host}:${port}/`));
