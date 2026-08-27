// Run a scenario matrix and append one CSV row per RUN to results/summary.csv.
// Aggregate across replicates afterwards with `node harness/aggregate.js`.
//
// Tiers (compose with --reps=N):
//   --tier=mini   5 transports x {50,500,2000} @20ev/s + a low-rate poll-vs-adaptive pair. ~17 runs.
//   --tier=core   the paper's headline cells: 5 transports x {50,500,2000} @20ev/s. 15 cells.
//   --tier=scale  the C10K frontier: 5 transports x {5000,10000} @20ev/s. 10 cells.
//   --tier=rate   the arrival-rate axis: 5 transports x c=500 x {1,20,100}. 15 cells.
//   --tier=full   core + scale + rate, deduped.
//
// REPLICATES ARE INTERLEAVED, NOT BLOCKED. With --reps=5 the runner executes every cell once
// (round 1), then every cell again (round 2), and so on -- instead of five back-to-back runs of
// one cell. Blocked replicates confound a cell with whatever the machine was doing during its
// block (thermal ramp, a background daemon, cache state); interleaving spreads that noise evenly
// across all cells so between-transport differences survive it. Round order is also rotated each
// round so no transport permanently occupies the "first run after cooldown" slot.
//
// Every run gets a fresh server + producer + Redis channel and a bind-probed free port, and
// asserts the responding server is the one it launched (see run.js) -- an orphaned server from a
// killed sweep once silently served a cell's measurements.
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
const reps = Math.max(1, Number(argv.reps || 1));

const cells = [];
const add = (c) => {
  if (!cells.some((x) => x.transport === c.transport && x.clients === c.clients && x.rate === c.rate)) cells.push(c);
};
const core = () => {
  for (const clients of [50, 500, 2000])
    for (const t of T) add({ transport: t, clients, rate: 20, durationSec: 20, warmupSec: 5 });
};
const scale = () => {
  // Longer warmup: 10k connections take real time to establish and the first seconds are
  // dominated by connect churn, not steady-state delivery.
  for (const clients of [5000, 10000])
    for (const t of T) add({ transport: t, clients, rate: 20, durationSec: 30, warmupSec: 15 });
};
const rate = () => {
  for (const r of [1, 20, 100])
    for (const t of T) add({ transport: t, clients: 500, rate: r, durationSec: 30, warmupSec: 8 });
};

if (tier === 'mini') {
  core();
  for (const t of ['poll', 'poll-debounced']) add({ transport: t, clients: 500, rate: 1, durationSec: 30, warmupSec: 5 });
} else if (tier === 'core') core();
else if (tier === 'scale') scale();
else if (tier === 'rate') rate();
else if (tier === 'full') {
  core();
  scale();
  rate();
} else {
  console.error('unknown --tier (mini|core|scale|rate|full)');
  process.exit(1);
}

// --only=transport:clients:rate isolates one cell (e.g. to re-run after a transient failure)
if (argv.only) {
  const [t, c, r] = String(argv.only).split(':');
  const keep = cells.filter((s) => s.transport === t && s.clients === Number(c) && s.rate === Number(r));
  if (!keep.length) {
    console.error(`--only=${argv.only} matches no cell in tier ${tier}`);
    process.exit(1);
  }
  cells.length = 0;
  cells.push(...keep);
}

// Build the interleaved run order: round-robin over cells, rotating the starting offset per round.
const queue = [];
for (let rep = 0; rep < reps; rep++)
  for (let i = 0; i < cells.length; i++) queue.push({ ...cells[(i + rep) % cells.length], rep: rep + 1 });

const CSV = `${ROOT}results/summary.csv`;
const HEADER =
  'finishedAt,transport,clients,rate,rep,payloadBytes,pollIntervalMs,durationSec,latSamples,p50ms,p90ms,p95ms,p99ms,p999ms,maxMs,deliveryRatio,missedRate,wireBytesPerClient,appBytesPerClient,reqPerClientPerSec,serverCpuPct,serverRssMaxMB,serverLoopP99SliceMaxMs,serverLoopP99SliceMedMs,clampedSubResolution,serverBackpressure,generatorLoopP99ms,generatorLimited,errors,reconnects,runId\n';

function row(r, rep) {
  const s = r.scenario;
  const L = r.latencyMs;
  return (
    [
      r.finishedAt, s.transport, s.clients, s.rate, rep, s.payloadBytes, s.pollIntervalMs, s.durationSec,
      L.count ?? 0, L.p50 ?? '', L.p90 ?? '', L.p95 ?? '', L.p99 ?? '', L.p999 ?? '', L.max ?? '',
      r.delivery.deliveryRatio ?? '', r.delivery.missedRate,
      r.serverWire.bytesPerClient, r.client.appBytesPerClient, r.client.reqPerClientPerSec,
      r.server.cpuPct, r.server.rssMaxMB, r.server.loopDelayP99SliceMaxMs, r.server.loopDelayP99SliceMedMs,
      L.clampedSubResolution ?? 0, r.serverWire.backpressure,
      r.generator.loopDelayP99Ms, r.generator.generatorLimited, r.client.errors, r.client.reconnects, r.runId,
    ].join(',') + '\n'
  );
}

mkdirSync(`${ROOT}results`, { recursive: true });
if (!existsSync(CSV)) appendFileSync(CSV, HEADER);

const perRunSec = 55;
console.log(
  `[sweep:${tier}] ${cells.length} cells x ${reps} rep(s) = ${queue.length} runs (~${Math.round((queue.length * perRunSec) / 60)} min, interleaved)`,
);
let failed = 0;
const t0 = Date.now();
for (let i = 0; i < queue.length; i++) {
  const sc = queue[i];
  const eta = i ? Math.round((((Date.now() - t0) / i) * (queue.length - i)) / 60000) : null;
  const tag = `[${i + 1}/${queue.length}${eta != null ? ` ~${eta}m left` : ''}] ${sc.transport} c=${sc.clients} r=${sc.rate} rep${sc.rep}`;
  try {
    const r = await runScenario(sc);
    appendFileSync(CSV, row(r, sc.rep));
    const L = r.latencyMs;
    console.log(
      `${tag} -> p50=${L.p50}ms p95=${L.p95}ms p99=${L.p99}ms ratio=${r.delivery.deliveryRatio} wireB/cl=${r.serverWire.bytesPerClient} cpu=${r.server.cpuPct}% loopP99max=${r.server.loopDelayP99SliceMaxMs}/med=${r.server.loopDelayP99SliceMedMs}ms${r.generator.generatorLimited ? ' GENERATOR-LIMITED' : ''}`,
    );
  } catch (e) {
    failed++;
    console.error(`${tag} FAILED: ${e.message}`);
  }
  // Cooldown scales with fleet size. Each run leaves `clients` sockets in TIME_WAIT (~30s on
  // macOS, not tunable without sudo); at 10k that is a fifth of the ephemeral port range still
  // held when the next run starts, and back-to-back large runs exhaust it — observed as
  // "clients failed to connect" on a 10k cell whose standalone run had succeeded minutes earlier.
  const cooldownMs = sc.clients >= 5000 ? 30000 : sc.clients >= 2000 ? 8000 : 2000;
  await new Promise((res) => setTimeout(res, cooldownMs));
}
console.log(
  `[sweep:${tier}] done — ${queue.length - failed}/${queue.length} ok in ${Math.round((Date.now() - t0) / 60000)} min. CSV: results/summary.csv`,
);
console.log('[sweep] aggregate with: node harness/aggregate.js');
process.exit(failed ? 1 : 0);
