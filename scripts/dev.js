// Dev convenience: run one server + one producer together with prefixed logs. Ctrl-C stops both.
// (For benchmarks, run them as separate processes by hand so producer CPU is isolated from server CPU.)
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import process from 'node:process';

const ROOT = fileURLToPath(new URL('..', import.meta.url));
const procs = [];

function run(name, args) {
  const p = spawn('node', args, { cwd: ROOT, env: process.env });
  p.stdout.on('data', (d) => process.stdout.write(`[${name}] ${d}`));
  p.stderr.on('data', (d) => process.stderr.write(`[${name}] ${d}`));
  p.on('exit', (code) => {
    console.log(`[dev] ${name} exited (${code})`);
    shutdown();
  });
  procs.push(p);
}

function shutdown() {
  for (const p of procs) if (!p.killed) p.kill('SIGINT');
}
process.on('SIGINT', () => {
  shutdown();
  process.exit(0);
});

run('server', ['src/server.js']);
// Redis pub/sub is fire-and-forget (no replay) — start the server first so it's subscribed before
// the producer publishes, otherwise the earliest events are dropped on the floor.
setTimeout(() => run('producer', ['src/producer.js']), 600);
