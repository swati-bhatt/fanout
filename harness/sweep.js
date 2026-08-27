// Run a scenario matrix sequentially and append one CSV row per run to results/summary.csv.
// Tiers:
//   --tier=mini  (default) 5 transports x {50,500,2000} clients @ 20 ev/s, plus a low-rate
//                (1 ev/s) poll-vs-adaptive pair that shows WHERE adaptive polling actually wins.
//                ~17 runs, ~12 min.
//   --tier=full  5 transports x {50,500,2000,5000,10000} clients x {1,20,100} ev/s.
//                75 runs, ~60-75 min — run it attended once, then leave it alone.
// Every run gets a fresh server+producer+channel and its own port (no TIME_WAIT collisions).
import { appendFileSync, existsSync, mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { runScenario } from './run.js';

const ROOT = fileURLToPath(new URL('..', import.meta.url));
const T = ['poll', 'poll-debounced', 'longpoll', 'sse', 'ws'];
const argv = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const m = a.match(/^--([^=]+)=(.*)$/);
    return m ? [m[1], m[2]] : [a.replace(/^--/, ''), true];
  }),
);
const tier = argv.tier || 'mini';

const scenarios = [];
if (tier === 'mini') {
  for (const clients of [50, 500, 2000])
    for (const t of T) scenarios.push({ transport: t, clients, rate: 20, durationSec: 20, warmupSec: 5 });
  // low-rate pair: adaptive polling's whole reason to exist (backoff while idle)
  for (const t of ['poll', 'poll-debounced'])
    scenarios.push({ transport: t, clients: 500, rate: 1, durationSec: 30, warmupSec: 5 });
} else if (tier === 'full') {
  for (const clients of [50, 500, 2000, 5000, 10000])
    for (const rate of [1, 20, 100])
      for (const t of T)
        scenarios.push({ transport: t, clients, rate, durationSec: 30, warmupSec: 8 });
} else {
  console.error('unknown --tier (mini|full)');
  process.exit(1);
}
// ports are bind-probed dynamically per run (see harness/run.js freePort) -- no static ranges

// --only=transport:clients:rate re-runs a single cell (e.g. after a transient failure) and
// appends it to the same summary.csv
let only = argv.only;
if (only) {
  const [t, c, r] = only.split(':');
  const keep = scenarios.filter((s) => s.transport === t && s.clients === Number(c) && s.rate === Number(r));
  if (!keep.length) {
    console.error(`--only=${only} matches no scenario in tier ${tier}`);
    process.exit(1);
  }
  scenarios.length = 0;
  scenarios.push(...keep);
}

const CSV = `${ROOT}results/summary.csv`;
const HEADER =
  'finishedAt,transport,clients,rate,payloadBytes,pollIntervalMs,durationSec,latSamples,p50ms,p90ms,p95ms,p99ms,p999ms,maxMs,deliveryRatio,missedRate,wireBytesPerClient,appBytesPerClient,reqPerClientPerSec,serverCpuPct,serverRssMaxMB,serverLoopP99SliceMaxMs,serverLoopP99SliceMedMs,clampedSubResolution,serverBackpressure,generatorLoopP99ms,generatorLimited,errors,reconnects,runId\n';

function row(r) {
  const s = r.scenario;
  const L = r.latencyMs;
  return [
    r.finishedAt, s.transport, s.clients, s.rate, s.payloadBytes, s.pollIntervalMs, s.durationSec,
    L.count ?? 0, L.p50 ?? '', L.p90 ?? '', L.p95 ?? '', L.p99 ?? '', L.p999 ?? '', L.max ?? '',
    r.delivery.deliveryRatio ?? '', r.delivery.missedRate,
    r.serverWire.bytesPerClient, r.client.appBytesPerClient, r.client.reqPerClientPerSec,
    r.server.cpuPct, r.server.rssMaxMB, r.server.loopDelayP99SliceMaxMs, r.server.loopDelayP99SliceMedMs, L.clampedSubResolution ?? 0, r.serverWire.backpressure,
    r.generator.loopDelayP99Ms, r.generator.generatorLimited, r.client.errors, r.client.reconnects, r.runId,
  ].join(',') + '\n';
}

mkdirSync(`${ROOT}results`, { recursive: true });
if (!existsSync(CSV)) appendFileSync(CSV, HEADER);

console.log(`[sweep:${tier}] ${scenarios.length} runs (~${Math.round((scenarios.length * (tier === 'mini' ? 40 : 55)) / 60)} min)`);
let failed = 0;
for (let i = 0; i < scenarios.length; i++) {
  const sc = scenarios[i];
  const tag = `[${i + 1}/${scenarios.length}] ${sc.transport} c=${sc.clients} r=${sc.rate}`;
  try {
    const r = await runScenario(sc);
    appendFileSync(CSV, row(r));
    const L = r.latencyMs;
    console.log(
      `${tag} -> p50=${L.p50}ms p95=${L.p95}ms p99=${L.p99}ms ratio=${r.delivery.deliveryRatio} wireB/cl=${r.serverWire.bytesPerClient} cpu=${r.server.cpuPct}% loopP99max=${r.server.loopDelayP99SliceMaxMs}/med=${r.server.loopDelayP99SliceMedMs}ms${r.generator.generatorLimited ? ' GENERATOR-LIMITED' : ''}`,
    );
  } catch (e) {
    failed++;
    console.error(`${tag} FAILED: ${e.message}`);
  }
  await new Promise((r) => setTimeout(r, 2000)); // cooldown between runs
}
console.log(`[sweep:${tier}] done — ${scenarios.length - failed}/${scenarios.length} ok. CSV: results/summary.csv`);
process.exit(failed ? 1 : 0);
