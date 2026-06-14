// Event producer. Publishes events to a Redis channel at a configurable rate R. Every server
// instance subscribes and fans out to its clients — so the producer is decoupled from the servers
// and you can run 1 producer + K servers (the horizontal-scale story).
//
// Run standalone (default) so its CPU does NOT pollute the server's CPU measurement:
//   node src/producer.js --rate=100 --bytes=200 --duration=60
import { fileURLToPath } from 'node:url';
import { config } from './config.js';
import { makeRedis } from './redis.js';
import { nowMs } from './clock.js';

// Build an event whose total serialized size is ~targetBytes (envelope + padding), so the bandwidth
// axis is controlled. t_emit is stamped at the instant of emission for accurate end-to-end latency.
function makeEvent(topic, seq, targetBytes) {
  const base = { topic, seq, t_emit: nowMs(), data: '' };
  const overhead = Buffer.byteLength(JSON.stringify(base));
  base.data = 'x'.repeat(Math.max(0, targetBytes - overhead));
  return base;
}

export async function startProducer(opts = {}) {
  const rate = opts.rate ?? config.rate;
  const bytes = opts.bytes ?? config.payloadBytes;
  const channel = opts.channel ?? config.channel;
  const topic = opts.topic ?? config.topic;
  const durationSec = opts.durationSec ?? 0; // 0 = run until stopped

  const pub = makeRedis('producer');
  const startMs = nowMs();
  let seq = 0;
  let emitted = 0;
  let stopped = false;
  let timer = null;

  // Drift-corrected scheduler: emit however many events SHOULD have been emitted by now. Keeps the
  // long-run rate accurate even when the host's timer resolution is coarse or a tick runs late.
  const tickMs = 5;
  const tick = () => {
    if (stopped) return;
    const elapsedSec = (nowMs() - startMs) / 1000;
    if (durationSec && elapsedSec >= durationSec) return stop();
    const target = Math.floor(elapsedSec * rate);
    while (emitted < target) {
      pub.publish(channel, JSON.stringify(makeEvent(topic, seq++, bytes)));
      emitted++;
    }
    timer = setTimeout(tick, tickMs);
  };

  const reportTimer = setInterval(() => {
    const elapsed = (nowMs() - startMs) / 1000;
    console.log(
      `[producer] emitted=${emitted} seq=${seq} effRate=${(emitted / elapsed).toFixed(1)}/s target=${rate}/s`,
    );
  }, 1000);

  async function stop() {
    if (stopped) return;
    stopped = true;
    clearTimeout(timer);
    clearInterval(reportTimer);
    await pub.quit().catch(() => pub.disconnect());
    console.log(`[producer] stopped after emitting ${emitted} events`);
  }

  console.log(
    `[producer] channel=${channel} rate=${rate}/s bytes=${bytes}${durationSec ? ` duration=${durationSec}s` : ''}`,
  );
  tick();
  return { stop };
}

// --- CLI entry: parse --key=value flags and run until duration/SIGINT ---
const isMain = process.argv[1] && process.argv[1] === fileURLToPath(import.meta.url);
if (isMain) {
  const argv = Object.fromEntries(
    process.argv.slice(2).map((a) => {
      const m = a.match(/^--([^=]+)=(.*)$/);
      return m ? [m[1], m[2]] : [a.replace(/^--/, ''), true];
    }),
  );
  const handle = await startProducer({
    rate: argv.rate ? Number(argv.rate) : undefined,
    bytes: argv.bytes ? Number(argv.bytes) : undefined,
    durationSec: argv.duration ? Number(argv.duration) : 0,
    channel: argv.channel || undefined,
  });
  const shutdown = async () => {
    await handle.stop();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}
