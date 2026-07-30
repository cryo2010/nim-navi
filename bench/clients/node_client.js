'use strict';
// Benchmark client: Node.js built-in https + Agent. The Agent pools connections
// (keepAlive) and caches TLS sessions, and we gunzip the body with zlib so the
// decompression work matches the other clients. NAVI_BENCH_COLD=1 turns keep-alive
// off and sends `Connection: close`, so every request opens a fresh connection
// (the Agent still resumes the TLS session, as Node does by default).
const https = require('https');
const zlib = require('zlib');

const url = new URL(process.env.NAVI_BENCH_URL || 'https://127.0.0.1:8443');
const iters = parseInt(process.env.NAVI_BENCH_ITERS || '3000', 10);
const cold = process.env.NAVI_BENCH_COLD === '1';
const warmup = cold ? Math.min(20, iters) : Math.max(100, Math.floor(iters / 10));
const body = 'x'.repeat(256);

const agent = new https.Agent({
  keepAlive: !cold,
  rejectUnauthorized: false, // self-signed target; TLS still exercised
  maxSockets: 8,
});

function req(method, path, data) {
  return new Promise((resolve, reject) => {
    const headers = { 'Accept-Encoding': 'gzip' };
    if (data) headers['Content-Length'] = Buffer.byteLength(data);
    if (cold) headers['Connection'] = 'close';
    const r = https.request(
      { method, hostname: url.hostname, port: url.port, path, agent, headers },
      (res) => {
        const stream =
          res.headers['content-encoding'] === 'gzip' ? res.pipe(zlib.createGunzip()) : res;
        stream.on('data', () => {});
        stream.on('end', resolve);
        stream.on('error', reject);
      },
    );
    r.on('error', reject);
    if (data) r.write(data);
    r.end();
  });
}

async function one() {
  await req('GET', '/get');
  await req('POST', '/post', body);
  await req('PUT', '/put', body);
  await req('PATCH', '/patch', body);
  await req('DELETE', '/delete');
  await req('HEAD', '/get');
  await req('OPTIONS', '/get');
}

(async () => {
  for (let i = 0; i < warmup; i++) await one();
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < iters; i++) await one();
  const secs = Number(process.hrtime.bigint() - t0) / 1e9;
  const reqs = iters * 7;
  process.stdout.write(`RESULT\tnode-https\t${reqs}\t${secs.toFixed(3)}\t${(reqs / secs).toFixed(0)}\n`);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
