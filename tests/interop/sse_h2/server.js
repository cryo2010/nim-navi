// An HTTP/2 TLS origin for the SSE connection-sharing interop (#466). It counts h2
// SESSIONS (one per TCP/TLS connection) so the client can prove which requests
// landed on which connection, which is the whole point of the issue.
//
//   /plain    200 "ok", at once.
//   /events   text/event-stream: one event at once, then SILENCE for SSE_GAP_MS,
//             then a second event and the end of the stream. The gap is longer than
//             the read timeout the client sets, so a stream that shared a connection
//             with the client's bounded requests would be killed by it.
//   /stall    accepted and never answered. The client's `timeouts.read` has to end
//             it, which on h2 means the whole connection dies with it.
//   /big      1000 bytes, for the response-size-cap checks (a client whose
//             maxResponseBytes is a few bytes must fail this and still read /events).
//   /conns    "<opened> <closed>" h2 sessions so far, for connection accounting.
//
// Needs no dependencies beyond node itself (cert/key come from run.sh).
'use strict';
const http2 = require('http2');
const fs = require('fs');

const port = parseInt(process.env.SSE_PORT || '8443', 10);
const gapMs = parseInt(process.env.SSE_GAP_MS || '3000', 10);

let opened = 0;
let closed = 0;

const server = http2.createSecureServer({
  key: fs.readFileSync('key.pem'),
  cert: fs.readFileSync('cert.pem'),
});

server.on('session', (session) => {
  opened += 1;
  session.on('close', () => { closed += 1; });
});

server.on('stream', (stream, headers) => {
  const path = headers[':path'] || '/';
  stream.on('error', () => {});           // a client RST/close is expected here
  if (path === '/plain') {
    stream.respond({ ':status': 200, 'content-type': 'text/plain' });
    stream.end('ok');
    return;
  }
  if (path === '/big') {
    stream.respond({ ':status': 200, 'content-type': 'text/plain' });
    stream.end('x'.repeat(1000));
    return;
  }
  if (path === '/conns') {
    stream.respond({ ':status': 200, 'content-type': 'text/plain' });
    stream.end(opened + ' ' + closed);
    return;
  }
  if (path === '/stall') {
    return;                               // accepted, never answered, never reset
  }
  if (path === '/events') {
    stream.respond({
      ':status': 200,
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
    });
    stream.write('data: one\nid: 1\n\n');
    const t = setTimeout(() => {
      try { stream.write('data: two\nid: 2\n\n'); stream.end(); } catch (e) { /* gone */ }
    }, gapMs);
    stream.on('close', () => clearTimeout(t));
    return;
  }
  stream.respond({ ':status': 404 });
  stream.end();
});

server.listen(port, '127.0.0.1', () => {
  console.log('sse_h2 server on ' + port + ' (gap ' + gapMs + ' ms)');
});
