// The server: subscribes to the Redis channel ONCE and fans every event out to all five transports.
// Stateless w.r.t. the producer (decoupled via Redis), so you can run K of these behind nothing but
// shared Redis to study horizontal fan-out.
//
// Hot-path note: events arrive from Redis as a JSON string. We parse once (for buffer + seq) and
// reuse the ORIGINAL string for sse/ws, so serialization cost stays O(events), not O(events x clients)
// — important so the measurement reflects transport overhead, not JSON.stringify.
import Fastify from 'fastify';
import { config } from './config.js';
import { makeRedis } from './redis.js';
import { EventBuffer } from './eventBuffer.js';
import { metrics } from './metrics.js';
import { nowMs } from './clock.js';
import { createPoll } from './transports/poll.js';
import { createPollDebounced } from './transports/pollDebounced.js';
import { createLongPoll } from './transports/longpoll.js';
import { createSSE } from './transports/sse.js';
import { createWS } from './transports/ws.js';

const buffer = new EventBuffer(config.bufferSize);
const fastify = Fastify({ logger: false }); // logging would add overhead and skew measurements

// Don't let Node's platform timeouts abort long-poll / SSE and corrupt latency data.
fastify.server.requestTimeout = 0;
fastify.server.headersTimeout = 0;

// Disable Nagle on every HTTP socket. The ws library does this internally, but raw Node HTTP
// sockets leave Nagle ON — small SSE frames then sit in the kernel interacting with delayed ACKs
// (~40ms tail spikes), which would silently bias the comparison toward WebSocket. Found via the
// harness: SSE p95 jumped 47ms at only 50 clients, in ~40ms multiples — the classic signature.
fastify.server.on('connection', (socket) => socket.setNoDelay(true));

// --- HTTP byte accounting (wire-level, incl. headers) + permissive CORS for the M2 React page ---
function routeTransport(url) {
  if (url.startsWith('/poll-debounced')) return 'pollDebounced';
  if (url.startsWith('/poll')) return 'poll';
  if (url.startsWith('/longpoll')) return 'longpoll';
  return null; // /sse is hijacked (counted in-module); /metrics, /healthz excluded
}
fastify.addHook('onRequest', async (req, reply) => {
  reply.header('Access-Control-Allow-Origin', '*');
  reply.header('Access-Control-Allow-Headers', '*');
  reply.header('Access-Control-Allow-Methods', '*');
  req._bw = req.socket ? req.socket.bytesWritten : 0;
});
fastify.addHook('onResponse', async (req) => {
  if (req.method === 'OPTIONS') return;
  const t = routeTransport(req.url || '');
  if (!t) return;
  metrics.inc(t, 'requests', 1);
  if (req.socket) metrics.inc(t, 'bytesSent', req.socket.bytesWritten - req._bw);
});
fastify.options('/*', async (req, reply) => reply.code(204).send());

// --- Transports ---
const poll = createPoll(buffer);
const pollDebounced = createPollDebounced(buffer);
const longpoll = createLongPoll(buffer);
const sse = createSSE(buffer);
const ws = createWS(buffer);
poll.register(fastify);
pollDebounced.register(fastify);
longpoll.register(fastify);
sse.register(fastify);
ws.attach(fastify.server);
metrics.registerGauge('sse', () => ({ liveWireBytes: sse.liveWireBytes() }));
metrics.registerGauge('ws', () => ({ liveWireBytes: ws.liveWireBytes() }));

// --- Observability ---
fastify.get('/metrics', async () => metrics.snapshot());
fastify.get('/healthz', async () => ({ ok: true, instanceId: config.instanceId, serverTime: nowMs() }));

// --- Redis fan-out ---
const sub = makeRedis('subscriber');
await sub.subscribe(config.channel);
sub.on('message', (channel, message) => {
  if (channel !== config.channel) return;
  metrics.redisEvent();
  let ev;
  try {
    ev = JSON.parse(message);
  } catch {
    return;
  }
  buffer.push(ev);
  longpoll.onEvent();
  sse.onEvent(ev, message);
  ws.onEvent(ev, message);
});

await fastify.listen({ port: config.port, host: config.host });
console.log(
  `[server] ${config.instanceId} listening on http://${config.host}:${config.port}  (redis ${config.redisUrl}, channel "${config.channel}")`,
);
console.log('[server] transports: GET /poll  /poll-debounced  /longpoll  /sse  |  WS /ws  |  /metrics /healthz');

// --- Optional embedded producer (DEV ONLY; pollutes server CPU — keep separate for benchmarks) ---
let producerHandle = null;
if (config.embedProducer) {
  const { startProducer } = await import('./producer.js');
  producerHandle = await startProducer();
  console.log('[server] embedded producer started (EMBED_PRODUCER=1)');
}

// --- Graceful shutdown ---
let closing = false;
async function shutdown(sig) {
  if (closing) return;
  closing = true;
  console.log(`[server] ${sig} -> shutting down`);
  try {
    if (producerHandle) await producerHandle.stop();
    ws.close();
    await fastify.close();
    await sub.quit().catch(() => sub.disconnect());
  } finally {
    process.exit(0);
  }
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
