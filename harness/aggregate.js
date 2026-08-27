// Aggregate replicated runs from results/summary.csv into results/aggregate.csv, one row per cell.
//
// Uses the MEDIAN across replicates, not the mean: a single contaminated run (a background process,
// a thermal excursion, an orphaned server) shifts a mean and barely moves a median. That failure
// mode is not hypothetical here -- it is exactly what invalidated an earlier sweep's headline.
//
// Every cell also carries its SPREAD (min, max, and the max/min ratio of p99). Spread is the
// honesty column: a cell whose p99 varies 20x across replicates has not measured anything, and no
// claim may rest on it regardless of how good its median looks. Cells are stamped `stable` when
// p99 spread <= 2x with n >= 3, and flagged otherwise.
//
//   node harness/aggregate.js [--in results/summary.csv] [--out results/aggregate.csv]
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const ROOT = fileURLToPath(new URL('..', import.meta.url));
const argv = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const m = a.match(/^--([^=]+)=(.*)$/);
    return m ? [m[1], m[2]] : [a.replace(/^--/, ''), true];
  }),
);
const IN = argv.in ? `${ROOT}${argv.in}` : `${ROOT}results/summary.csv`;
const OUT = argv.out ? `${ROOT}${argv.out}` : `${ROOT}results/aggregate.csv`;

if (!existsSync(IN)) {
  console.error(`no input CSV at ${IN} -- run a sweep first`);
  process.exit(1);
}

const lines = readFileSync(IN, 'utf8').trim().split('\n');
const head = lines[0].split(',');
const rows = lines.slice(1).map((l) => Object.fromEntries(l.split(',').map((v, i) => [head[i], v])));
if (!rows.length) {
  console.error('input CSV has no data rows');
  process.exit(1);
}

const num = (v) => (v === '' || v == null ? null : Number(v));
const med = (a) => {
  if (!a.length) return null;
  const s = [...a].sort((x, y) => x - y);
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
const r3 = (x) => (x == null ? '' : +x.toFixed(3));

// Group by cell. Rows predating the `rep` column simply have rep undefined; they still group.
const cells = new Map();
for (const r of rows) {
  const key = `${r.transport}|${r.clients}|${r.rate}`;
  if (!cells.has(key)) cells.set(key, []);
  cells.get(key).push(r);
}

const OUTHEAD = [
  'transport', 'clients', 'rate', 'n',
  'p50ms_med', 'p99ms_med', 'p99ms_min', 'p99ms_max', 'p99_spread_x',
  'wireKBperClient_med', 'cpuPct_med', 'loopP99MedMs_med',
  'deliveryRatio_worst', 'missedRate_worst', 'clamped_total',
  'generatorLimited_any', 'stable',
].join(',');

const out = [];
for (const [key, rs] of cells) {
  const [transport, clients, rate] = key.split('|');
  const p99s = rs.map((r) => num(r.p99ms)).filter((v) => v != null);
  const p50s = rs.map((r) => num(r.p50ms)).filter((v) => v != null);
  const min = p99s.length ? Math.min(...p99s) : null;
  const max = p99s.length ? Math.max(...p99s) : null;
  const spread = min && min > 0 ? max / min : null;
  const n = rs.length;
  const stable = n >= 3 && spread != null && spread <= 2;
  out.push({
    sortKey: [Number(rate), Number(clients), transport],
    line: [
      transport, clients, rate, n,
      r3(med(p50s)), r3(med(p99s)), r3(min), r3(max), spread == null ? '' : +spread.toFixed(2),
      r3(med(rs.map((r) => num(r.wireBytesPerClient) / 1024).filter((v) => v != null))),
      r3(med(rs.map((r) => num(r.serverCpuPct)).filter((v) => v != null))),
      r3(med(rs.map((r) => num(r.serverLoopP99SliceMedMs)).filter((v) => v != null))),
      r3(Math.min(...rs.map((r) => num(r.deliveryRatio) ?? 1))),
      r3(Math.max(...rs.map((r) => num(r.missedRate) ?? 0))),
      rs.reduce((a, r) => a + (num(r.clampedSubResolution) ?? 0), 0),
      rs.some((r) => r.generatorLimited === 'true'),
      stable,
    ].join(','),
  });
}
out.sort((a, b) =>
  a.sortKey[0] - b.sortKey[0] || a.sortKey[1] - b.sortKey[1] || String(a.sortKey[2]).localeCompare(String(b.sortKey[2])),
);

writeFileSync(OUT, OUTHEAD + '\n' + out.map((o) => o.line).join('\n') + '\n');
console.log(`[aggregate] ${cells.size} cells from ${rows.length} runs -> ${OUT.replace(ROOT, '')}`);

// Console view, plus an explicit trust summary -- unstable or under-replicated cells are called
// out by name so they cannot quietly become claims.
const w = OUTHEAD.split(',').map((h, i) => Math.max(h.length, ...out.map((o) => String(o.line.split(',')[i]).length)));
const fmt = (cellsArr) => cellsArr.map((c, i) => String(c).padEnd(w[i])).join('  ');
console.log('\n' + fmt(OUTHEAD.split(',')));
for (const o of out) console.log(fmt(o.line.split(',')));

const unstable = out.filter((o) => o.line.split(',')[16] === 'false');
const genLimited = out.filter((o) => o.line.split(',')[15] === 'true');
console.log('');
if (unstable.length) {
  console.log(`[trust] ${unstable.length}/${out.length} cells NOT stable (n<3 or p99 spread >2x) -- do not build claims on these:`);
  for (const o of unstable) {
    const f = o.line.split(',');
    console.log(`         ${f[0]} c=${f[1]} r=${f[2]}  n=${f[3]} spread=${f[8]}x`);
  }
} else {
  console.log(`[trust] all ${out.length} cells stable (n>=3, p99 spread <=2x)`);
}
if (genLimited.length) console.log(`[trust] ${genLimited.length} cell(s) GENERATOR-LIMITED -- the load generator stalled; those latencies are not the server's.`);
