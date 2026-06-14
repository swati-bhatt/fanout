// Self-contained end-to-end smoke test + mini latency probe (the seed of the Milestone 2 harness).
// Spawns a server and a producer, then exercises ALL FIVE transports as a client, computing
// end-to-end latency (event emitted -> received) the same way the real harness will. Tears down and
// prints a per-transport table + the server's own /metrics view. Exits non-zero on failure.
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import process from 'node:process';
import WebSocket from 'ws';
import { nowMs } from '../src/clock.js';

const ROOT = fileURLToPath(new URL('..', import.meta.url));
const PORT = process.env.PORT || 3000;
const BASE = `http://127.0.0.1:${PORT}`;
const RATE = 50;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const fmt = (x) => (x == null ? '  n/a' : `${x.toFixed(1)}ms`);
function pct(arr, p) {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
}
const getJson = async (path) => (await fetch(BASE + path)).json();

function startProc(name, args) {
  return new Promise((resolve, reject) => {
    const p = spawn('node', args, { cwd: ROOT, env: process.env });
    p.stderr.on('data', (d) => process.stderr.write(`[${name}] ${d}`));
    p.on('exit', (c) => reject(new Error(`${name} exited early (code ${c})`)));
    if (name === 'server') {
      const to = setTimeout(() => reject(new Error('server did not start within 8s')), 8000);
      p.stdout.on('data', (d) => {
        if (d.toString().includes('listening')) {
          clearTimeout(to);
          resolve(p);
        }
      });
    } else {
      resolve(p);
    }
  });
}

// Generic poll/longpoll probe: start from a FRESH tail so latency reflects only events emitted
// during the probe (not stale backfill).
async function pollProbe(path, { rounds = 12, gapMs = 150 } = {}) {
  const lat = [];
  let missed = 0;
  let cursor = (await getJson('/poll?since=-1')).tailSeq ?? -1;
  for (let i = 0; i < rounds; i++) {
    const j = await getJson(`${path}?since=${cursor}`);
    if (j.tailSeq != null) cursor = j.tailSeq;
    missed += j.missed || 0;
    const t = nowMs();
    for (const ev of j.events) lat.push(t - ev.t_emit);
    if (!path.includes('longpoll')) await sleep(gapMs); // long poll already blocks until data
  }
  return { lat, missed };
}

async function sseProbe({ ms = 2500 } = {}) {
  const lat = [];
  const ctrl = new AbortController();
  const res = await fetch(`${BASE}/sse`, { signal: ctrl.signal, headers: { accept: 'text/event-stream' } });
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  let buf = '';
  const stop = setTimeout(() => ctrl.abort(), ms);
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let idx;
      while ((idx = buf.indexOf('\n\n')) >= 0) {
        const frame = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        const line = frame.split('\n').find((l) => l.startsWith('data:'));
        if (!line) continue;
        try {
          const ev = JSON.parse(line.slice(5).trim());
          if (ev.t_emit) lat.push(nowMs() - ev.t_emit);
        } catch {
          /* heartbeat or partial */
        }
      }
    }
  } catch {
    /* aborted */
  } finally {
    clearTimeout(stop);
  }
  return { lat, missed: 0 };
}

async function wsProbe({ ms = 2500 } = {}) {
  const lat = [];
  let rtt = null;
  const ws = new WebSocket(`${BASE.replace('http', 'ws')}/ws`);
  await new Promise((res, rej) => {
    ws.on('open', res);
    ws.on('error', rej);
  });
  const t0 = nowMs();
  ws.send(JSON.stringify({ type: 'ping', t: t0 })); // axis-2 RTT demo
  ws.on('message', (d) => {
    let m;
    try {
      m = JSON.parse(d.toString());
    } catch {
      return;
    }
    if (m.type === 'pong') {
      rtt = nowMs() - m.t;
      return;
    }
    if (m.t_emit) lat.push(nowMs() - m.t_emit);
  });
  await sleep(ms);
  ws.close();
  return { lat, missed: 0, rtt };
}

(async () => {
  console.log(`[smoke] starting server + producer (rate ${RATE}/s)...`);
  const server = await startProc('server', ['src/server.js']);
  const producer = await startProc('producer', ['src/producer.js', `--rate=${RATE}`]);
  await sleep(1000); // let events flow and the buffer warm

  console.log('[smoke] warm — probing each transport (~10s)...');
  const results = {};
  results.poll = await pollProbe('/poll');
  results.pollDebounced = await pollProbe('/poll-debounced');
  results.longpoll = await pollProbe('/longpoll', { rounds: 10 });
  results.sse = await sseProbe();
  results.ws = await wsProbe();

  const rows = [['transport', 'samples', 'p50', 'p95', 'p99', 'missed']];
  for (const [k, v] of Object.entries(results)) {
    rows.push([k, String(v.lat.length), fmt(pct(v.lat, 50)), fmt(pct(v.lat, 95)), fmt(pct(v.lat, 99)), String(v.missed)]);
  }
  const w = rows[0].map((_, i) => Math.max(...rows.map((r) => r[i].length)));
  console.log('\n=== end-to-end latency (event emitted -> received) ===');
  for (const r of rows) console.log(r.map((c, i) => c.padEnd(w[i])).join('  '));
  if (results.ws.rtt != null) console.log(`\nWS client->server->client RTT (axis-2 demo): ${results.ws.rtt.toFixed(1)}ms`);

  const m = await getJson('/metrics');
  console.log('\n=== /metrics (server view) ===');
  for (const [t, c] of Object.entries(m.transports)) {
    console.log(
      `  ${t.padEnd(14)} conns=${c.connections} reqs=${c.requests} delivered=${c.eventsDelivered} ` +
        `bytesSent=${c.bytesSent}${c.liveWireBytes != null ? ` liveWire=${c.liveWireBytes}` : ''} missed=${c.missed} bp=${c.backpressure}`,
    );
  }
  console.log(
    `  redisEventsReceived=${m.redisEventsReceived} loopDelay.p99=${m.process.eventLoopDelayMs.p99.toFixed(2)}ms ` +
      `rss=${(m.process.memory.rss / 1e6).toFixed(0)}MB`,
  );

  console.log('\n[smoke] OK — tearing down.');
  producer.kill('SIGINT');
  server.kill('SIGINT');
  setTimeout(() => process.exit(0), 300);
})().catch((e) => {
  console.error('[smoke] FAILED:', e.message);
  process.exit(1);
});
