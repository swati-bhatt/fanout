// Orchestrate ONE benchmark scenario end to end:
//   fresh server (own port + own Redis channel => zero cross-run contamination)
//   + fresh producer (separate process => its CPU never pollutes the server measurement)
//   + N clients spread across parallel workers (~one per core so the generator keeps up)
// Phases: connect (ramped) -> warmup (excluded) -> measure (recorded) -> teardown.
// Writes results/<runId>.json and returns the result object (sweep.js builds on this).
//
//   node harness/run.js --transport=ws --clients=500 --rate=20 --duration=20
import { spawn, fork } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { Histogram } from './histogram.js';

const ROOT = fileURLToPath(new URL('..', import.meta.url));
const TRANSPORTS = ['poll', 'poll-debounced', 'longpoll', 'sse', 'ws'];
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function getJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url} -> ${res.status}`);
  return res.json();
}

async function waitHealthy(url, timeoutMs) {
  const t0 = Date.now();
  for (;;) {
    try {
      await getJson(url);
      return;
    } catch {
      if (Date.now() - t0 > timeoutMs) throw new Error(`not healthy after ${timeoutMs}ms: ${url}`);
      await sleep(150);
    }
  }
}

const withTimeout = (p, ms, what) =>
  Promise.race([p, sleep(ms).then(() => Promise.reject(new Error(`timeout: ${what}`)))]);

function splitClients(total, workers) {
  const base = Math.floor(total / workers);
  return Array.from({ length: workers }, (_, i) => base + (i < total % workers ? 1 : 0));
}

export async function runScenario(sc) {
  if (!TRANSPORTS.includes(sc.transport)) throw new Error(`transport must be one of ${TRANSPORTS.join('|')}`);
  const cfg = {
    transport: sc.transport,
    clients: sc.clients,
    rate: sc.rate ?? 20,
    payloadBytes: sc.payloadBytes ?? 200,
    pollIntervalMs: sc.pollIntervalMs ?? 1000,
    durationSec: sc.durationSec ?? 20,
    warmupSec: sc.warmupSec ?? 5,
    port: sc.port ?? 3210,
    host: '127.0.0.1',
  };
  const runId = `${cfg.transport}-c${cfg.clients}-r${cfg.rate}-${Date.now()}`;
  const channel = `bench-${runId}`;
  const base = `http://${cfg.host}:${cfg.port}`;
  const kids = [];
  try {
    // 1. server first (Redis pub/sub has no replay: subscriber must exist before the producer speaks)
    const server = spawn('node', ['src/server.js'], {
      cwd: ROOT,
      env: { ...process.env, PORT: cfg.port, HOST: cfg.host, CHANNEL: channel, INSTANCE_ID: runId },
      stdio: ['ignore', 'ignore', 'pipe'],
    });
    kids.push(server);
    let serverErr = '';
    server.stderr.on('data', (d) => (serverErr += d));
    await waitHealthy(`${base}/healthz`, 10000).catch((e) => {
      throw new Error(`server failed to start: ${e.message}\n${serverErr}`);
    });

    // 2. producer
    const producer = spawn(
      'node',
      ['src/producer.js', `--rate=${cfg.rate}`, `--bytes=${cfg.payloadBytes}`, `--channel=${channel}`],
      { cwd: ROOT, env: process.env, stdio: ['ignore', 'ignore', 'pipe'] },
    );
    kids.push(producer);

    // 3. workers (parallel load generators)
    const nWorkers = Math.max(1, Math.min(os.availableParallelism() - 2, 8, cfg.clients));
    const per = splitClients(cfg.clients, nWorkers);
    const rampMs = Math.min(5000, Math.max(500, cfg.clients * 2));
    const workers = per.map(() => fork(new URL('./worker.js', import.meta.url), { cwd: ROOT }));
    kids.push(...workers);
    for (const w of workers)
      w.on('message', (m) => {
        if (m.type === 'fatal') throw new Error(`worker: ${m.error}`);
      });
    const allReady = Promise.all(
      workers.map((w) => new Promise((res) => w.on('message', (m) => m.type === 'ready' && res()))),
    );
    workers.forEach((w, i) =>
      w.send({
        cmd: 'start',
        cfg: {
          transport: cfg.transport,
          clients: per[i],
          host: cfg.host,
          port: cfg.port,
          pollIntervalMs: cfg.pollIntervalMs,
          rampMs,
        },
      }),
    );
    await withTimeout(allReady, 60000, 'clients failed to connect');

    // 4. warmup (excluded from stats), then measure
    await sleep(cfg.warmupSec * 1000);
    const m0 = await getJson(`${base}/metrics`);
    const samples = [];
    const sampler = setInterval(() => {
      getJson(`${base}/metrics`).then((s) => samples.push(s)).catch(() => {});
    }, 2000);
    const t0 = Date.now();
    workers.forEach((w) => w.send({ cmd: 'measure' }));
    await sleep(cfg.durationSec * 1000);
    const resultsP = Promise.all(
      workers.map((w) => new Promise((res) => w.on('message', (m) => m.type === 'result' && res(m)))),
    );
    workers.forEach((w) => w.send({ cmd: 'stop' }));
    const t1 = Date.now();
    clearInterval(sampler);
    const m1 = await getJson(`${base}/metrics`);
    const wres = await withTimeout(resultsP, 15000, 'workers did not report');
    workers.forEach((w) => w.send({ cmd: 'shutdown' }));

    // 5. merge + derive
    const hist = new Histogram();
    const tot = { events: 0, appBytes: 0, requests: 0, missed: 0, errors: 0, reconnects: 0, connectFailures: 0 };
    let genP99 = 0;
    for (const r of wres) {
      hist.merge(Histogram.fromJSON(r.latency));
      for (const k of Object.keys(tot)) tot[k] += r[k];
      genP99 = Math.max(genP99, r.loopDelayP99Ms || 0);
    }
    const wallSec = (t1 - t0) / 1000;
    const key = cfg.transport === 'poll-debounced' ? 'pollDebounced' : cfg.transport;
    const s0 = m0.transports[key];
    const s1 = m1.transports[key];
    const cpuUs =
      m1.process.cpuMicros.user - m0.process.cpuMicros.user + m1.process.cpuMicros.system - m0.process.cpuMicros.system;
    const all = [m0, ...samples, m1];
    const expected = cfg.rate * wallSec * cfg.clients; // producer rate is drift-corrected, so ~exact
    const result = {
      runId,
      scenario: cfg,
      wallSec: +wallSec.toFixed(2),
      latencyMs: hist.summary(),
      delivery: {
        events: tot.events,
        expectedApprox: Math.round(expected),
        deliveryRatio: expected ? +(tot.events / expected).toFixed(4) : null,
        missed: tot.missed,
        missedRate: tot.missed + tot.events ? +(tot.missed / (tot.missed + tot.events)).toFixed(6) : 0,
      },
      client: {
        appBytes: tot.appBytes,
        appBytesPerClient: Math.round(tot.appBytes / cfg.clients),
        requests: tot.requests,
        reqPerClientPerSec: +(tot.requests / cfg.clients / wallSec).toFixed(3),
        errors: tot.errors,
        reconnects: tot.reconnects,
        connectFailures: tot.connectFailures,
      },
      serverWire: {
        bytesSent: s1.bytesSent - s0.bytesSent, // socket-level for HTTP (incl headers); app-level for sse/ws
        bytesPerClient: Math.round((s1.bytesSent - s0.bytesSent) / cfg.clients),
        wireGaugeDelta: s1.liveWireBytes != null && s0.liveWireBytes != null ? s1.liveWireBytes - s0.liveWireBytes : null,
        requests: s1.requests - s0.requests,
        eventsDelivered: s1.eventsDelivered - s0.eventsDelivered,
        backpressure: s1.backpressure - s0.backpressure,
        missedReported: s1.missed - s0.missed,
        connectionsAtEnd: s1.connections,
      },
      server: {
        cpuPct: +((cpuUs / 1e6 / wallSec) * 100).toFixed(1),
        rssMaxMB: +(Math.max(...all.map((s) => s.process.memory.rss)) / 1e6).toFixed(1),
        loopDelayP99Ms: +Math.max(...[...samples, m1].map((s) => s.process.eventLoopDelayMs.p99)).toFixed(2),
      },
      generator: {
        workers: nWorkers,
        loopDelayP99Ms: +genP99.toFixed(2),
        // If the LOAD GENERATOR's own loop stalled >50ms, client-side latency numbers are suspect.
        generatorLimited: genP99 > 50,
      },
      finishedAt: new Date().toISOString(),
    };
    mkdirSync(`${ROOT}results`, { recursive: true });
    writeFileSync(`${ROOT}results/${runId}.json`, JSON.stringify(result, null, 2));
    return result;
  } finally {
    for (const k of kids) {
      try {
        k.kill('SIGINT');
      } catch {}
    }
    await sleep(400);
    for (const k of kids) {
      try {
        if (k.exitCode == null) k.kill('SIGKILL');
      } catch {}
    }
  }
}

// --- CLI ---
const isMain = process.argv[1] && process.argv[1] === fileURLToPath(import.meta.url);
if (isMain) {
  const argv = Object.fromEntries(
    process.argv.slice(2).map((a) => {
      const m = a.match(/^--([^=]+)=(.*)$/);
      return m ? [m[1], m[2]] : [a.replace(/^--/, ''), true];
    }),
  );
  if (!argv.transport || !argv.clients) {
    console.error('usage: node harness/run.js --transport=poll|poll-debounced|longpoll|sse|ws --clients=N [--rate=R] [--payload=B] [--duration=S] [--warmup=S] [--pollInterval=MS] [--port=P]');
    process.exit(1);
  }
  const res = await runScenario({
    transport: argv.transport,
    clients: Number(argv.clients),
    rate: argv.rate ? Number(argv.rate) : undefined,
    payloadBytes: argv.payload ? Number(argv.payload) : undefined,
    durationSec: argv.duration ? Number(argv.duration) : undefined,
    warmupSec: argv.warmup ? Number(argv.warmup) : undefined,
    pollIntervalMs: argv.pollInterval ? Number(argv.pollInterval) : undefined,
    port: argv.port ? Number(argv.port) : undefined,
  });
  const L = res.latencyMs;
  console.log(
    `[${res.runId}] p50=${L.p50}ms p95=${L.p95}ms p99=${L.p99}ms | delivered=${res.delivery.events} (ratio ${res.delivery.deliveryRatio}) missed=${res.delivery.missed} | wireB/client=${res.serverWire.bytesPerClient} | srvCPU=${res.server.cpuPct}% rss=${res.server.rssMaxMB}MB loopP99=${res.server.loopDelayP99Ms}ms | genP99=${res.generator.loopDelayP99Ms}ms${res.generator.generatorLimited ? ' GENERATOR-LIMITED' : ''}`,
  );
  console.log(`result -> results/${res.runId}.json`);
  process.exit(0);
}
