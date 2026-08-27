// Run one scenario against an EXTERNAL, already-running server -- the containerized server whose
// egress is shaped by netem (see docker/netem.sh). This is the flaky-network arm of the study.
//
// Why this is a separate runner rather than a flag on run.js: run.js owns the server lifecycle
// (spawn, health-check, identity-assert, kill). Here the server outlives the run and is deliberately
// NOT ours to manage, and its port is fixed by compose rather than bind-probed. Keeping the two
// runners apart avoids threading a "someone else owns the server" mode through every step.
//
// The producer still runs HERE, on the host, alongside the clients -- so t_emit and t_recv share one
// clock (src/clock.js). Only the server sits behind the impaired link.
//
//   node harness/netem-run.js --transport=ws --clients=500 --rate=20 --profile=wifi
import { spawn, fork, execFileSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { Histogram } from './histogram.js';

const ROOT = fileURLToPath(new URL('..', import.meta.url));
const TRANSPORTS = ['poll', 'poll-debounced', 'longpoll', 'sse', 'ws'];
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const argv = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const m = a.match(/^--([^=]+)=(.*)$/);
    return m ? [m[1], m[2]] : [a.replace(/^--/, ''), true];
  }),
);

const cfg = {
  transport: argv.transport,
  clients: Number(argv.clients || 500),
  rate: Number(argv.rate || 20),
  payloadBytes: Number(argv.payload || 200),
  pollIntervalMs: Number(argv.pollInterval || 1000),
  durationSec: Number(argv.duration || 30),
  // Impaired links need a longer warmup: connections take longer to establish and the first
  // seconds are dominated by handshake retries rather than steady-state delivery.
  warmupSec: Number(argv.warmup || 12),
  host: argv.host || '127.0.0.1',
  port: Number(argv.port || 3900),
  profile: argv.profile || 'unknown',
  // Unique per run by default: a fixed channel lets a leftover producer from an earlier run poison
  // this one (see the recreate step below).
  channel: argv.channel || `netem-${Date.now()}`,
  // The rig runs its own Redis (the host's binds to 127.0.0.1 with protected-mode on and is not
  // reachable from a container). The server talks to it over the compose network; the host-side
  // producer reaches the same instance through the published port.
  redisUrl: argv.redisUrl || 'redis://127.0.0.1:6380',
};

if (!TRANSPORTS.includes(cfg.transport)) {
  console.error(`usage: node harness/netem-run.js --transport=${TRANSPORTS.join('|')} --clients=N [--rate=R] [--profile=NAME] [--duration=S] [--port=3900]`);
  process.exit(1);
}

const base = `http://${cfg.host}:${cfg.port}`;
const getJson = async (u) => {
  const res = await fetch(u);
  if (!res.ok) throw new Error(`${u} -> ${res.status}`);
  return res.json();
};
const withTimeout = (p, ms, what) =>
  Promise.race([p, sleep(ms).then(() => Promise.reject(new Error(`timeout: ${what}`)))]);

const kids = [];
try {
  // FRESH SERVER PER RUN. run.js spawns a new server for every scenario; this runner reuses a
  // long-lived container, and without a reset that difference silently corrupts results: the
  // server's ring buffer retains previous runs' events while each new producer restarts `seq` at 0,
  // violating the buffer's monotonic-sequence assumption. Cursor-based transports (poll,
  // poll-debounced, longpoll) then read stale events and report absurd latency (~96 s) and
  // delivery ratios of 15-22x, while sse/ws — which join at the live tail — look fine. Observed
  // exactly that way before this reset existed.
  //
  // Restarting recreates the network namespace and therefore DROPS the netem qdisc, so the profile
  // must be applied AFTER the restart, not before. That ordering is why the profile is applied here
  // per-run rather than once per profile block.
  // Recreate (not merely restart) so the server comes up subscribed to a channel unique to THIS
  // run. run.js already isolates every scenario by channel; the netem rig originally reused a fixed
  // "events" channel, and a single leftover producer from an interrupted run kept publishing to it
  // — two producers, each numbering from seq 0, corrupted the buffer's ordering and produced
  // delivery ratios of 18-22x. Per-run channels make a stray producer harmless instead of silent.
  if (!argv.noReset) {
    try {
      execFileSync(
        'docker',
        ['compose', '-f', 'docker/compose.yml', 'up', '-d', '--force-recreate', '--no-deps', 'fanout-server'],
        { cwd: ROOT, env: { ...process.env, CHANNEL: cfg.channel }, stdio: 'ignore' },
      );
    } catch (e) {
      console.error(`could not recreate container: ${e.message}`);
      process.exit(1);
    }
  }

  // An impaired link makes a slow health check normal, so allow generous time before giving up.
  // Each ATTEMPT needs its own timeout, not just the overall loop: while the container restarts,
  // Docker's port proxy accepts the TCP connection but never answers, and fetch has no default
  // timeout — so a single hung attempt would block the retry loop until the outer deadline and
  // report the server as unreachable even though it came back seconds later. Observed exactly that.
  const health = await withTimeout(
    (async () => {
      for (;;) {
        const h = await fetch(`${base}/healthz`, { signal: AbortSignal.timeout(2000) })
          .then((r) => (r.ok ? r.json() : null))
          .catch(() => null);
        if (h) return h;
        await sleep(300);
      }
    })(),
    60000,
    'container server not reachable',
  ).catch(() => null);
  if (!health) {
    console.error(`no server at ${base} — start it with:  docker compose -f docker/compose.yml up -d --build`);
    process.exit(1);
  }

  // Apply the impairment now that the namespace exists again.
  if (!argv.noReset && cfg.profile !== 'unknown') {
    try {
      execFileSync('bash', ['docker/netem.sh', cfg.profile], { cwd: ROOT, stdio: 'ignore' });
    } catch (e) {
      console.error(`could not apply netem profile ${cfg.profile}: ${e.message}`);
      process.exit(1);
    }
    await sleep(500); // let the qdisc settle before traffic starts
  }

  const runId = `netem-${cfg.profile}-${cfg.transport}-c${cfg.clients}-r${cfg.rate}-${Date.now()}`;

  // Producer on the host, publishing to the same channel the container subscribes to.
  kids.push(
    spawn('node', ['src/producer.js', `--rate=${cfg.rate}`, `--bytes=${cfg.payloadBytes}`, `--channel=${cfg.channel}`], {
      cwd: ROOT,
      env: { ...process.env, REDIS_URL: cfg.redisUrl },
      stdio: ['ignore', 'ignore', 'pipe'],
    }),
  );

  const nWorkers = Math.max(1, Math.min(os.availableParallelism() - 2, 8, cfg.clients));
  const per = Array.from({ length: nWorkers }, (_, i) =>
    Math.floor(cfg.clients / nWorkers) + (i < cfg.clients % nWorkers ? 1 : 0),
  );
  const rampMs = Math.min(8000, Math.max(1000, cfg.clients * 3)); // slower ramp: impaired handshakes
  const workers = per.map(() => fork(new URL('./worker.js', import.meta.url), { cwd: ROOT }));
  kids.push(...workers);
  const allReady = Promise.all(
    workers.map((w) => new Promise((res) => w.on('message', (m) => m.type === 'ready' && res()))),
  );
  workers.forEach((w, i) =>
    w.send({
      cmd: 'start',
      cfg: { transport: cfg.transport, clients: per[i], host: cfg.host, port: cfg.port, pollIntervalMs: cfg.pollIntervalMs, rampMs },
    }),
  );
  await withTimeout(allReady, 120000, 'clients failed to connect over the impaired link');

  await sleep(cfg.warmupSec * 1000);
  const m0 = await getJson(`${base}/metrics`);
  const t0 = Date.now();
  workers.forEach((w) => w.send({ cmd: 'measure' }));
  await sleep(cfg.durationSec * 1000);
  const resultsP = Promise.all(
    workers.map((w) => new Promise((res) => w.on('message', (m) => m.type === 'result' && res(m)))),
  );
  workers.forEach((w) => w.send({ cmd: 'stop' }));
  const t1 = Date.now();
  const m1 = await getJson(`${base}/metrics`);
  const wres = await withTimeout(resultsP, 30000, 'workers did not report');
  workers.forEach((w) => w.send({ cmd: 'shutdown' }));

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
  const expected = cfg.rate * wallSec * cfg.clients;
  const result = {
    runId,
    netemProfile: cfg.profile,
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
    // Under impairment these are the headline columns: reconnects and connect failures are how a
    // transport's recovery story shows up in numbers, and missed is whether recovery lost data.
    resilience: {
      reconnects: tot.reconnects,
      connectFailures: tot.connectFailures,
      errors: tot.errors,
      missed: tot.missed,
    },
    client: { appBytes: tot.appBytes, appBytesPerClient: Math.round(tot.appBytes / cfg.clients), requests: tot.requests },
    serverWire: {
      bytesPerClient: Math.round(
        ((s1.liveWireBytes != null && s0.liveWireBytes != null ? s1.liveWireBytes - s0.liveWireBytes : s1.bytesSent - s0.bytesSent)) /
          cfg.clients,
      ),
      backpressure: s1.backpressure - s0.backpressure,
      eventsDelivered: s1.eventsDelivered - s0.eventsDelivered,
    },
    generator: { workers: nWorkers, loopDelayP99Ms: +genP99.toFixed(2), generatorLimited: genP99 > 50 },
    finishedAt: new Date().toISOString(),
  };
  mkdirSync(`${ROOT}results/netem`, { recursive: true });
  writeFileSync(`${ROOT}results/netem/${runId}.json`, JSON.stringify(result, null, 2));
  const L = result.latencyMs;
  console.log(
    `[${cfg.profile}] ${cfg.transport} c=${cfg.clients} -> p50=${L.p50}ms p95=${L.p95}ms p99=${L.p99}ms | ratio=${result.delivery.deliveryRatio} missed=${tot.missed} | reconnects=${tot.reconnects} connectFail=${tot.connectFailures} errors=${tot.errors} | wireB/cl=${result.serverWire.bytesPerClient}`,
  );
  console.log(`result -> results/netem/${runId}.json`);
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
process.exit(0);
