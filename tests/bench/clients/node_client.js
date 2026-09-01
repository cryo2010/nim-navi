// Node.js reference client for the navi HTTP bench harness.
//
// Dispatches on NAVI_WORKLOAD: "requests" (buffered GET/POST/PUT), or the
// streaming pairs "streamDownload"/"streamUpload". All paths are time-boxed
// (unmeasured warmup then a measured window), run NAVI_CLIENTS*NAVI_CONCURRENCY
// async workers, and land each measured unit in a log-bucketed latency
// histogram whose scheme (floor(log2(us)*64)) matches the Go/Nim/Rust/Python
// peers so the p50/p99/p999 columns are comparable. For "requests" the unit is
// one request; for the streaming workloads it is one full transfer, and the
// final RESULT field carries MB/s. Emits one RESULT line; fails hard on any
// surfaced transport error, protocol downgrade, or SHA-1 mismatch. Built-in
// modules only.
'use strict';

const https = require('https');
const http2 = require('http2');
const zlib = require('zlib');
const crypto = require('crypto');
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

  const workload = envStr('NAVI_WORKLOAD', 'requests');
  if (workload !== 'requests' && workload !== 'streamDownload' && workload !== 'streamUpload') {
    process.stdout.write(`SKIP\tnode\t${workload} not implemented\n`);
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
  const streamBytes = envInt('NAVI_STREAM_BYTES', 1073741824);

  const bases = [];
  for (let i = 0; i < servers; i++) bases.push(`https://${host}:${basePort + i}`);

  const verbs = ['GET', 'POST', 'PUT'];
  const payload = Buffer.from('payload-x');
  const isH2 = proto === 'h2';

  const startMs = performance.now();
  const measureStartMs = startMs + warmup * 1000;
  const deadlineMs = measureStartMs + seconds * 1000;

  let baseIdx = 0;         // shared round-robin cursor across the server pool
  let verified = false;    // one-time protocol gate
  let measuredBytes = 0;   // total bytes across measured transfers (streaming)

  function protoFail(got) {
    const want = isH2 ? 'HTTP/2' : 'HTTP/1.1';
    process.stderr.write(`FAIL: wrong protocol -- expected ${want}, got ${got}\n`);
    process.exit(1);
  }
  function gateH1(res) { if (!verified) { verified = true; if (res.httpVersion !== '1.1') protoFail(res.httpVersion); } }
  function gateH2(session) {
    if (!verified) {
      verified = true;
      if (!session.alpnProtocol || session.alpnProtocol.indexOf('h2') === -1) protoFail(session.alpnProtocol);
    }
  }

  // --- h1 transport (https module, HTTP/1.1) ---
  const agent = new https.Agent({
    keepAlive: !cold,
    maxSockets: cold ? Infinity : 4096,
    rejectUnauthorized: false,
  });

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

  // --- workload: requests (buffered GET/POST/PUT against /echo) ---
  function reqH1(verb, base) {
    return new Promise((resolve, reject) => {
      const headers = { 'accept-encoding': 'gzip' };
      let bodyBuf = null;
      if (verb === 'POST' || verb === 'PUT') {
        bodyBuf = payload;
        headers['content-type'] = 'text/plain';
        headers['content-length'] = bodyBuf.length;
      }
      const req = https.request(base + '/echo', { method: verb, agent, headers, rejectUnauthorized: false }, (res) => {
        gateH1(res);
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => {
          try { maybeGunzip(Buffer.concat(chunks), res.headers['content-encoding']); }
          catch (e) { return reject(e); }
          resolve(0);
        });
        res.on('error', reject);
      });
      req.on('error', reject);
      if (bodyBuf) req.write(bodyBuf);
      req.end();
    });
  }

  function reqH2(verb, baseIndex) {
    const base = bases[baseIndex];
    return new Promise((resolve, reject) => {
      const session = cold ? connect(base) : getSession(baseIndex);
      const hdrs = { ':method': verb, ':path': '/echo', 'accept-encoding': 'gzip' };
      let bodyBuf = null;
      if (verb === 'POST' || verb === 'PUT') { bodyBuf = payload; hdrs['content-type'] = 'text/plain'; }
      const req = session.request(hdrs);
      let encoding;
      req.on('response', (respHeaders) => { encoding = respHeaders['content-encoding']; gateH2(session); });
      const chunks = [];
      req.on('data', (c) => chunks.push(c));
      req.on('end', () => {
        try { maybeGunzip(Buffer.concat(chunks), encoding); }
        catch (e) { return reject(e); }
        if (cold) session.close();
        resolve(0);
      });
      req.on('error', reject);
      if (bodyBuf) req.write(bodyBuf);
      req.end();
    });
  }

  // --- workload: streamDownload (GET /download?size=N, verify x-sha1) ---
  // No accept-encoding: the server serves raw octet-stream, so we hash the wire
  // bytes directly and never buffer the body.
  function downH1(base) {
    return new Promise((resolve, reject) => {
      const url = `${base}/download?size=${streamBytes}`;
      const req = https.request(url, { method: 'GET', agent, rejectUnauthorized: false }, (res) => {
        gateH1(res);
        const h = crypto.createHash('sha1');
        let got = 0;
        res.on('data', (c) => { h.update(c); got += c.length; });
        res.on('end', () => {
          if (h.digest('hex') !== res.headers['x-sha1']) {
            process.stderr.write(`FAIL: streamDownload sha1 mismatch on ${url}\n`);
            process.exit(1);
          }
          resolve(got);
        });
        res.on('error', reject);
      });
      req.on('error', reject);
      req.end();
    });
  }

  function downH2(baseIndex) {
    const base = bases[baseIndex];
    const path = `/download?size=${streamBytes}`;
    return new Promise((resolve, reject) => {
      const session = cold ? connect(base) : getSession(baseIndex);
      const req = session.request({ ':method': 'GET', ':path': path });
      const h = crypto.createHash('sha1');
      let got = 0;
      let sha1;
      req.on('response', (respHeaders) => { sha1 = respHeaders['x-sha1']; gateH2(session); });
      req.on('data', (c) => { h.update(c); got += c.length; });
      req.on('end', () => {
        if (h.digest('hex') !== sha1) {
          process.stderr.write(`FAIL: streamDownload sha1 mismatch on ${base}${path}\n`);
          process.exit(1);
        }
        if (cold) session.close();
        resolve(got);
      });
      req.on('error', reject);
      req.end();
    });
  }

  // --- workload: streamUpload (POST /upload of N bytes, verify JSON echo) ---
  // Stream a reused 1 MiB block repeatedly, hashing the same bytes we send and
  // honouring backpressure. Server replies with {"sha1","size"}.
  const BLOCK = Buffer.alloc(1024 * 1024, 0x61);
  function writeBody(stream) {
    return new Promise((resolve, reject) => {
      const h = crypto.createHash('sha1');
      let remaining = streamBytes;
      const pump = () => {
        while (remaining > 0) {
          const n = Math.min(remaining, BLOCK.length);
          const buf = n === BLOCK.length ? BLOCK : BLOCK.subarray(0, n);
          h.update(buf);
          remaining -= n;
          const ok = stream.write(buf);
          if (!ok && remaining > 0) { stream.once('drain', pump); return; }
        }
        stream.end();
        resolve(h.digest('hex'));
      };
      stream.on('error', reject);
      pump();
    });
  }

  function collect(stream) {
    return new Promise((resolve, reject) => {
      const chunks = [];
      stream.on('data', (c) => chunks.push(c));
      stream.on('end', () => resolve(Buffer.concat(chunks)));
      stream.on('error', reject);
    });
  }

  function checkUpload(url, sentSha1, bodyBuf) {
    let parsed;
    try { parsed = JSON.parse(bodyBuf.toString()); }
    catch (e) { process.stderr.write(`FAIL: streamUpload bad JSON on ${url}: ${e}\n`); process.exit(1); }
    if (parsed.sha1 !== sentSha1 || Number(parsed.size) !== streamBytes) {
      process.stderr.write(`FAIL: streamUpload mismatch on ${url} -- got sha1=${parsed.sha1} size=${parsed.size}\n`);
      process.exit(1);
    }
  }

  function upH1(base) {
    const url = base + '/upload';
    return new Promise((resolve, reject) => {
      const headers = { 'content-type': 'application/octet-stream', 'content-length': streamBytes };
      const req = https.request(url, { method: 'POST', agent, headers, rejectUnauthorized: false }, (res) => {
        gateH1(res);
        collect(res).then((body) => { checkUpload(url, sentSha1, body); resolve(streamBytes); }, reject);
      });
      req.on('error', reject);
      let sentSha1;
      writeBody(req).then((d) => { sentSha1 = d; }, reject);
    });
  }

  function upH2(baseIndex) {
    const base = bases[baseIndex];
    const url = base + '/upload';
    return new Promise((resolve, reject) => {
      const session = cold ? connect(base) : getSession(baseIndex);
      const req = session.request({ ':method': 'POST', ':path': '/upload', 'content-type': 'application/octet-stream' });
      req.on('response', () => gateH2(session));
      let sentSha1;
      writeBody(req).then((d) => { sentSha1 = d; }, reject);
      collect(req).then((body) => {
        checkUpload(url, sentSha1, body);
        if (cold) session.close();
        resolve(streamBytes);
      }, reject);
    });
  }

  // Pick the per-unit function for the requested workload + protocol.
  let unit;
  if (workload === 'requests') {
    unit = (verb, bi) => (isH2 ? reqH2(verb, bi) : reqH1(verb, bases[bi]));
  } else if (workload === 'streamDownload') {
    unit = (_verb, bi) => (isH2 ? downH2(bi) : downH1(bases[bi]));
  } else {
    unit = (_verb, bi) => (isH2 ? upH2(bi) : upH1(bases[bi]));
  }

  async function workerLoop(seed) {
    let n = seed;
    while (performance.now() < deadlineMs) {
      const verb = verbs[n % verbs.length];
      n++;
      const bi = (baseIdx++) % bases.length;
      const t0 = performance.now();
      let bytes = 0;
      try {
        bytes = await unit(verb, bi);
      } catch (e) {
        fail(verb, bases[bi], e);
      }
      const nowMs = performance.now();
      if (nowMs >= measureStartMs) { record(Math.round((nowMs - t0) * 1000)); measuredBytes += bytes; }
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
  const mbps = elapsed > 0 ? measuredBytes / elapsed / 1e6 : 0;
  const p50 = percentile(50);
  const p99 = percentile(99);
  const p999 = percentile(99.9);
  process.stdout.write(
    `RESULT\tnode\t${ops}\t${elapsed.toFixed(3)}\t${Math.round(rps)}\t` +
    `${p50.toFixed(3)}\t${p99.toFixed(3)}\t${p999.toFixed(3)}\t${mbps.toFixed(1)}\n`
  );
  process.exit(0);
}

main().catch((e) => { process.stderr.write(`FAIL: ${e && e.stack ? e.stack : e}\n`); process.exit(1); });
