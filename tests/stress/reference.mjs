// Reference stress client: the same workload as the navi backends, but driven by
// Node's raw fetch + WebSocket (no navi). It's a baseline -- compare its req/s to
// the navi backends, especially navi/js (which is fetch underneath), to see the
// cost navi's layer adds over the bare runtime. Mirrors stress_js.nim: several
// clients concurrently, each firing every HTTP verb concurrently plus a
// persistent WebSocket round trip, until a deadline, over TLS. A "middleware"
// stamps x-stress: 1 (added by the request wrapper). Any bad echo throws ->
// unhandled rejection -> non-zero exit.
//
// Run under Node with NODE_EXTRA_CA_CERTS pointing at the server cert.

const secs = parseFloat(process.env.NAVI_STRESS_SECONDS ?? '20');
const base = process.env.NAVI_STRESS_URL ?? 'https://127.0.0.1:9443';
const clients = parseInt(process.env.NAVI_STRESS_CLIENTS ?? '3');
const wsUrl = 'wss://' + base.slice('https://'.length) + '/ws';
const verbs = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS'];

const ok = (cond, msg) => { if (!cond) throw new Error(msg); };

// A tiny "middleware": every request goes through here, which stamps x-stress.
async function request(method) {
  const sentBody = ['POST', 'PUT', 'PATCH'].includes(method) ? 'payload-' + method : '';
  const sentCt = sentBody ? 'text/plain' : '';
  const headers = { 'x-stress': '1' };
  if (sentCt) headers['content-type'] = sentCt;
  const res = await fetch(base + '/echo', {
    method, headers, body: sentBody || undefined,
  });
  ok(res.status === 200, `${method} -> ${res.status}`);
  ok(res.headers.get('x-echo-method') === method, `method echo: ${res.headers.get('x-echo-method')}`);
  ok(res.headers.get('x-echo-stress') === '1', 'x-stress not seen by server');
  const text = await res.text();
  if (method !== 'HEAD') {
    ok(text === sentBody, `body echo: ${text}`);
    ok(res.headers.get('content-length') === String(sentBody.length),
      `content-length: ${res.headers.get('content-length')}`);
    ok(res.headers.get('content-type') === (sentCt || null),
      `content-type: ${res.headers.get('content-type')}`);
  }
}

function openWs() {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(wsUrl);
    ws.binaryType = 'arraybuffer';
    ws.onopen = () => resolve(ws);
    ws.onerror = () => reject(new Error('ws open failed'));
  });
}
const nextMsg = (ws) => new Promise((resolve) => { ws.onmessage = (e) => resolve(e.data); });

async function wsRound(ws) {
  const p1 = nextMsg(ws); ws.send('ping');
  ok((await p1) === 'ping', 'ws text echo');
  const p2 = nextMsg(ws); ws.send(new TextEncoder().encode('bytes'));
  const b = await p2;
  ok(new TextDecoder().decode(b) === 'bytes', 'ws binary echo');
}

async function oneClient(deadline) {
  const ws = await openWs();          // one persistent WS per client
  let ops = 0;
  while (Date.now() < deadline) {
    await Promise.all(verbs.map(request));   // every verb concurrently
    await wsRound(ws);
    ops++;
  }
  ws.close();
  return ops;
}

const deadline = Date.now() + secs * 1000;
const counts = await Promise.all(Array.from({ length: clients }, () => oneClient(deadline)));
const total = counts.reduce((a, b) => a + b, 0);
const rps = Math.round((total * verbs.length) / secs);
console.log(`[node-ref] ${clients} clients, ${total} batches, ${rps} req/s over ${secs}s: OK`);
