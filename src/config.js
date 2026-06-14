// Central config, env-driven with sane defaults. Every knob the sweep (Milestone 3) varies
// lives here so a run is fully described by its environment.
import process from 'node:process';

const num = (v, d) => (v === undefined || v === '' ? d : Number(v));
const str = (v, d) => (v === undefined || v === '' ? d : v);
const bool = (v, d) => (v === undefined || v === '' ? d : v === '1' || v === 'true');

export const config = {
  instanceId: str(process.env.INSTANCE_ID, `inst-${process.pid}`),
  port: num(process.env.PORT, 3000),
  host: str(process.env.HOST, '0.0.0.0'),

  // Redis pub/sub fan-out
  redisUrl: str(process.env.REDIS_URL, 'redis://127.0.0.1:6379'),
  channel: str(process.env.CHANNEL, 'events'),

  // Producer
  rate: num(process.env.RATE, 10), // events/sec (R)
  payloadBytes: num(process.env.PAYLOAD_BYTES, 200),
  topic: str(process.env.TOPIC, 'events'),
  embedProducer: bool(process.env.EMBED_PRODUCER, false),

  // Per-instance ring buffer (poll/longpoll/resync)
  bufferSize: num(process.env.BUFFER_SIZE, 10000),

  // Polling
  pollMaxBatch: num(process.env.POLL_MAX_BATCH, 1000),

  // Adaptive / debounced poll hints
  pollMinMs: num(process.env.POLL_MIN_MS, 50),
  pollMaxMs: num(process.env.POLL_MAX_MS, 5000),
  pollBackoffFactor: num(process.env.POLL_BACKOFF, 2),

  // Long poll
  longPollTimeoutMs: num(process.env.LONGPOLL_TIMEOUT_MS, 25000),

  // Heartbeats
  sseHeartbeatMs: num(process.env.SSE_HEARTBEAT_MS, 15000),
  wsHeartbeatMs: num(process.env.WS_HEARTBEAT_MS, 15000),

  // Backpressure
  wsMaxBufferedBytes: num(process.env.WS_MAX_BUFFERED_BYTES, 1 << 20),
};
