// HTTP/2 server (TLS) advertising SETTINGS_ENABLE_CONNECT_PROTOCOL (RFC 8441),
// accepting a WebSocket Extended CONNECT and echoing WS frames (client frames are
// masked; server frames must be unmasked).
const http2 = require('http2');
const fs = require('fs');

function parseFrames(buf) {
  const frames = [];
  let off = 0;
  while (true) {
    if (buf.length - off < 2) break;
    const b0 = buf[off], b1 = buf[off + 1];
    const opcode = b0 & 0x0f;
    const masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f;
    let p = off + 2;
    if (len === 126) { if (buf.length - off < 4) break; len = buf.readUInt16BE(off + 2); p = off + 4; }
    else if (len === 127) { if (buf.length - off < 10) break; len = Number(buf.readBigUInt64BE(off + 2)); p = off + 10; }
    let mask = null;
    if (masked) { if (buf.length < p + 4) break; mask = buf.slice(p, p + 4); p += 4; }
    if (buf.length < p + len) break;
    let payload = buf.slice(p, p + len);
    if (masked) { const o = Buffer.alloc(len); for (let i = 0; i < len; i++) o[i] = payload[i] ^ mask[i & 3]; payload = o; }
    frames.push({ opcode, payload });
    off = p + len;
  }
  return [frames, buf.slice(off)];
}
function encodeFrame(opcode, payload) {
  const n = payload.length;
  let h;
  if (n < 126) h = Buffer.from([0x80 | opcode, n]);
  else if (n <= 0xffff) { h = Buffer.alloc(4); h[0] = 0x80 | opcode; h[1] = 126; h.writeUInt16BE(n, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | opcode; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
  return Buffer.concat([h, payload]);
}

const server = http2.createSecureServer({
  key: fs.readFileSync('key.pem'),
  cert: fs.readFileSync('cert.pem'),
  settings: { enableConnectProtocol: true },
  ALPNProtocols: ['h2'],
});
server.on('stream', (stream, headers) => {
  if (headers[':method'] === 'CONNECT' && headers[':protocol'] === 'websocket') {
    stream.respond({ ':status': 200 });
    let buf = Buffer.alloc(0);
    stream.on('data', (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      let frames; [frames, buf] = parseFrames(buf);
      for (const f of frames) {
        if (f.opcode === 0x8) { stream.write(encodeFrame(0x8, f.payload)); stream.end(); }
        else stream.write(encodeFrame(f.opcode, f.payload)); // echo text/binary
      }
    });
    stream.on('error', () => {});
  } else {
    stream.respond({ ':status': 404 });
    stream.end();
  }
});
server.listen(parseInt(process.env.WS_PORT || '8443'), '127.0.0.1', () => {
  console.log('WS_H2_SERVER_READY');
});
