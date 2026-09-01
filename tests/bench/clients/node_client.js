// Node.js reference client for the navi HTTP bench harness (requests workload).
//
// Time-boxed buffered GET/POST/PUT throughput + latency against the local TLS
// server pool. An unmeasured warmup prelude precedes the measured window; each
// measured request's wall time lands in a log-bucketed latency histogram whose
// scheme (floor(log2(us)*64)) matches the Go/Nim/Rust/Python peers so the
// p50/p99/p999 columns are comparable. Emits one RESULT line; fails hard on any
// surfaced transport error or protocol downgrade. Built-in modules only.
'use strict';

const https = require('https');
const http2 = require('http2');
const zlib = require('zlib');
const { performance } = require('perf_hooks');

function envStr(k, def) { const v = process.env[k]; return v ? v : def; }
function envInt(k, def) { const v = process.env[k]; const n = v ? parseInt(v, 10) : NaN; return Number.isFinite(n) ? n : def; }
function envFloat(k, def) { const v = process.env[k]; const n = v ? parseFloat(v) : NaN; return Number.isFinite(n) ? n : def; }

function fail(verb, url, err) {
  process.stderr.write(`FAIL: ${verb} ${url} -> ${err && err.stack ? err.stack : err}\n`);
  process.exit(1);
}

// --- latency histogram (must match the peer reference clients) ---
const BPD = 64;
const MAX_BUCKETS = Math.floor(Math.log2(300000000) * 64) + 1;
const counts = new Int32Array(MAX_BUCKETS);
let total = 0;

function record(us) {
  const v = Math.max(1, us);
  let idx = Math.floor(Math.log2(v) * BPD);
  if (idx < 0) idx = 0; else if (idx >= MAX_BUCKETS) idx = MAX_BUCKETS - 1;
  counts[idx]++;
  total++;
}

// percentile(p) -> milliseconds (bucket midpoint).
function percentile(p) {
  if (total === 0) return 0;
  const target = Math.max(1, Math.ceil((p / 100) * total));
  let cum = 0;
  for (let i = 0; i < MAX_BUCKETS; i++) {
    cum += counts[i];
    if (cum >= target) return Math.pow(2, (i + 0.5) / BPD) / 1000;
  }
  return Math.pow(2, (MAX_BUCKETS - 1 + 0.5) / BPD) / 1000;
}

function maybeGunzip(buf, encoding) {
  if (encoding && encoding.toLowerCase() === 'gzip') return zlib.gunzipSync(buf);
  return buf;
}

async function main() {
  const proto = envStr('NAVI_PROTO', 'h2');
  if (proto === 'h3') {
    process.stdout.write('SKIP\tnode\tno reference-client HTTP/3\n');
    process.exit(0);
  }

  const host = envStr('NAVI_HOST', '127.0.0.1');
  const basePort = envInt('NAVI_BASE_PORT', 9443);
  let servers = envInt('NAVI_SERVERS', 5);
  if (servers < 1) servers = 1;
  const seconds = envFloat('NAVI_SECONDS', 20);
  const warmup = envFloat('NAVI_WARMUP_SECONDS', 2);
  const cold = envStr('NAVI_MODE', 'pooled') === 'cold';
  const clients = envInt('NAVI_CLIENTS', 3);
  const concurrency = envInt('NAVI_CONCURRENCY', 8);

  const bases = [];
  for (let i = 0; i < servers; i++) bases.push(`https://${host}:${basePort + i}`);

  const verbs = ['GET', 'POST', 'PUT'];
  const payload = Buffer.from('payload-x');

  const startMs = performance.now();
  const measureStartMs = startMs + warmup * 1000;
  const deadlineMs = measureStartMs + seconds * 1000;

  let baseIdx = 0;      // shared round-robin cursor across the server pool
  let verified = false; // one-time protocol gate

  // --- h1 transport (https module, HTTP/1.1) ---
  const agent = new https.Agent({
    keepAlive: !cold,
    maxSockets: cold ? Infinity : 4096,
    rejectUnauthorized: false,
  });

  function doH1(verb, base) {
    const url = base + '/echo';
    return new Promise((resolve, reject) => {
      const headers = { 'accept-encoding': 'gzip' };
      let bodyBuf = null;
      if (verb === 'POST' || verb === 'PUT') {
        bodyBuf = payload;
        headers['content-type'] = 'text/plain';
        headers['content-length'] = bodyBuf.length;
      }
      const req = https.request(url, { method: verb, agent, headers, rejectUnauthorized: false }, (res) => {
        if (!verified) {
          verified = true;
          if (res.httpVersion !== '1.1') {
            process.stderr.write(`FAIL: wrong protocol -- expected HTTP/1.1, got ${res.httpVersion}\n`);
            process.exit(1);
          }
        }
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          try { maybeGunzip(Buffer.concat(chunks), res.headers['content-encoding']); }
          catch (e) { return reject(e); }
          resolve();
        });
        res.on('error', reject);
      });
      req.on('error', reject);
      if (bodyBuf) req.write(bodyBuf);
      req.end();
    });
  }

  // --- h2 transport (http2 module) ---
  // One ClientHttp2Session per base for pooled; per-request session for cold.
  const sessions = new Array(servers).fill(null);
  function connect(base) {
    const s = http2.connect(base, { rejectUnauthorized: false });
    s.on('error', (e) => fail('h2-session', base, e));
    return s;
  }
  function getSession(i) {
    if (!sessions[i] || sessions[i].closed || sessions[i].destroyed) sessions[i] = connect(bases[i]);
    return sessions[i];
  }

  function doH2(verb, baseIndex) {
    const base = bases[baseIndex];
    const url = base + '/echo';
    return new Promise((resolve, reject) => {
      const session = cold ? connect(base) : getSession(baseIndex);
      const hdrs = { ':method': verb, ':path': '/echo', 'accept-encoding': 'gzip' };
      let bodyBuf = null;
      if (verb === 'POST' || verb === 'PUT') {
        bodyBuf = payload;
        hdrs['content-type'] = 'text/plain';
      }
      const req = session.request(hdrs);
      let encoding;
      req.on('response', (respHeaders) => {
        encoding = respHeaders['content-encoding'];
        if (!verified) {
          verified = true;
          // Response arrived over the http2 session => HTTP/2 confirmed.
          if (!session.alpnProtocol || session.alpnProtocol.indexOf('h2') === -1) {
            process.stderr.write(`FAIL: wrong protocol -- expected HTTP/2, got ${session.alpnProtocol}\n`);
            process.exit(1);
          }
        }
      });
      const chunks = [];
      req.on('data', (c) => chunks.push(c));
      req.on('end', () => {
        try { maybeGunzip(Buffer.concat(chunks), encoding); }
        catch (e) { return reject(e); }
        if (cold) session.close();
        resolve();
      });
      req.on('error', reject);
      if (bodyBuf) req.write(bodyBuf);
      req.end();
    });
  }

  const isH2 = proto === 'h2';

  async function workerLoop(seed) {
    let n = seed;
    while (performance.now() < deadlineMs) {
      const verb = verbs[n % verbs.length];
      n++;
      const bi = (baseIdx++) % bases.length;
      const t0 = performance.now();
      try {
        if (isH2) await doH2(verb, bi);
        else await doH1(verb, bases[bi]);
      } catch (e) {
        fail(verb, bases[bi] + '/echo', e);
      }
      const nowMs = performance.now();
      if (nowMs >= measureStartMs) record(Math.round((nowMs - t0) * 1000));
    }
  }

  const workerCount = clients * concurrency;
  const workers = [];
  for (let i = 0; i < workerCount; i++) workers.push(workerLoop(i));
  await Promise.all(workers);

  for (const s of sessions) { if (s && !s.destroyed) s.close(); }
  agent.destroy();

  const elapsed = (performance.now() - measureStartMs) / 1000;
  const ops = total;
  const rps = elapsed > 0 ? ops / elapsed : 0;
  const p50 = percentile(50);
  const p99 = percentile(99);
  const p999 = percentile(99.9);
  process.stdout.write(
    `RESULT\tnode\t${ops}\t${elapsed.toFixed(3)}\t${Math.round(rps)}\t` +
    `${p50.toFixed(3)}\t${p99.toFixed(3)}\t${p999.toFixed(3)}\t0.0\n`
  );
  process.exit(0);
}

main().catch((e) => { process.stderr.write(`FAIL: ${e && e.stack ? e.stack : e}\n`); process.exit(1); });
