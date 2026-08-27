// Run the impairment matrix: every netem profile x every transport, against the containerized
// server. Appends one row per run to results/netem/summary.csv.
//
//   docker compose -f docker/compose.yml up -d --build     # once
//   node harness/netem-sweep.js --reps=2 --clients=500
//
// ORDERING: profiles are the OUTER loop and transports the inner one, because changing a netem
// profile is a global, stateful `tc` operation on the container -- flipping it between every run
// would multiply the chance of a run measuring a half-applied qdisc. Within a profile the
// transport order rotates per rep, for the same anti-blocking reason as the main sweep.
//
// The `clean` profile runs FIRST and is not merely a control: it quantifies the container tax
// (virtual NIC + port proxy) that every impaired number also carries. Impairment deltas must be
// read against clean-in-container, never against the host-loopback sweep.
import { execFileSync } from 'node:child_process';
import { appendFileSync, existsSync, mkdirSync, readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const ROOT = fileURLToPath(new URL('..', import.meta.url));
const argv = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const m = a.match(/^--([^=]+)=(.*)$/);
    return m ? [m[1], m[2]] : [a.replace(/^--/, ''), true];
  }),
);

const PROFILES = (argv.profiles || 'clean,wifi,mobile,lossy,satellite').split(',');
const TRANSPORTS = (argv.transports || 'poll,poll-debounced,longpoll,sse,ws').split(',');
const reps = Math.max(1, Number(argv.reps || 1));
const clients = Number(argv.clients || 500);
const rate = Number(argv.rate || 20);
const duration = Number(argv.duration || 25);
const port = Number(argv.port || 3900);

const sh = (cmd, args) => execFileSync(cmd, args, { cwd: ROOT, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });

// Preflight: the container must be up before anything is measured, otherwise every run fails
// identically and the matrix records 25 useless rows.
try {
  const health = sh('curl', ['-sf', '--max-time', '20', `http://127.0.0.1:${port}/healthz`]);
  console.log(`[netem-sweep] server up: ${JSON.parse(health).instanceId}`);
} catch {
  console.error(`[netem-sweep] no server at :${port} — start it first:\n  docker compose -f docker/compose.yml up -d --build`);
  process.exit(1);
}

const CSV = `${ROOT}results/netem/summary.csv`;
const HEADER =
  'finishedAt,profile,transport,clients,rate,rep,latSamples,p50ms,p95ms,p99ms,maxMs,deliveryRatio,missed,reconnects,connectFailures,errors,wireBytesPerClient,backpressure,generatorLimited,runId\n';
mkdirSync(`${ROOT}results/netem`, { recursive: true });
if (!existsSync(CSV)) appendFileSync(CSV, HEADER);

// netem-run.js writes its own per-run JSON; find the newest one to harvest structured fields
// rather than re-parsing the console line.
function newestResult() {
  const dir = `${ROOT}results/netem`;
  const files = readdirSync(dir).filter((f) => f.endsWith('.json'));
  if (!files.length) return null;
  const newest = files
    .map((f) => ({ f, t: Number(f.match(/-(\d+)\.json$/)?.[1] || 0) }))
    .sort((a, b) => b.t - a.t)[0].f;
  return JSON.parse(readFileSync(`${dir}/${newest}`, 'utf8'));
}

const total = PROFILES.length * TRANSPORTS.length * reps;
console.log(`[netem-sweep] ${PROFILES.length} profiles x ${TRANSPORTS.length} transports x ${reps} rep(s) = ${total} runs`);
let done = 0;
let failed = 0;
const t0 = Date.now();

for (const profile of PROFILES) {
  // The profile is applied by netem-run.js AFTER it restarts the container (a restart recreates
  // the network namespace and drops the qdisc), so the sweep no longer applies it here — doing so
  // would only shape the interval between runs.
  console.log(`[netem-sweep] profile ${profile}`);

  for (let rep = 0; rep < reps; rep++) {
    const order = TRANSPORTS.map((_, i) => TRANSPORTS[(i + rep) % TRANSPORTS.length]);
    for (const transport of order) {
      done++;
      const eta = done > 1 ? Math.round((((Date.now() - t0) / (done - 1)) * (total - done + 1)) / 60000) : null;
      const tag = `[${done}/${total}${eta != null ? ` ~${eta}m` : ''}] ${profile}/${transport} rep${rep + 1}`;
      try {
        sh('node', [
          'harness/netem-run.js',
          `--transport=${transport}`,
          `--clients=${clients}`,
          `--rate=${rate}`,
          `--duration=${duration}`,
          `--profile=${profile}`,
          `--port=${port}`,
        ]);
        const r = newestResult();
        if (!r) throw new Error('no result JSON produced');
        const L = r.latencyMs;
        appendFileSync(
          CSV,
          [
            r.finishedAt, profile, transport, clients, rate, rep + 1,
            L.count ?? 0, L.p50 ?? '', L.p95 ?? '', L.p99 ?? '', L.max ?? '',
            r.delivery.deliveryRatio ?? '', r.resilience.missed, r.resilience.reconnects,
            r.resilience.connectFailures, r.resilience.errors,
            r.serverWire.bytesPerClient, r.serverWire.backpressure, r.generator.generatorLimited, r.runId,
          ].join(',') + '\n',
        );
        console.log(`${tag} -> p50=${L.p50}ms p99=${L.p99}ms ratio=${r.delivery.deliveryRatio} missed=${r.resilience.missed} reconn=${r.resilience.reconnects}`);
      } catch (e) {
        failed++;
        console.error(`${tag} FAILED: ${String(e.message).split('\n')[0]}`);
      }
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
}

try {
  sh('bash', ['docker/netem.sh', 'clear']);
  console.log('[netem-sweep] link cleared');
} catch {
  console.error('[netem-sweep] WARNING: could not clear netem — run ./docker/netem.sh clear');
}
console.log(`[netem-sweep] done — ${total - failed}/${total} ok in ${Math.round((Date.now() - t0) / 60000)} min. CSV: results/netem/summary.csv`);
process.exit(failed ? 1 : 0);
