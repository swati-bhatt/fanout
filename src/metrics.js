// Server-side metrics, exposed as JSON at /metrics for the Milestone 2 harness to scrape.
//
// BYTE ACCOUNTING (be honest about this in the paper):
//   - HTTP transports (poll / poll-debounced / longpoll): bytesSent is measured at the SOCKET level
//     (socket.bytesWritten delta per response), so it INCLUDES HTTP response headers. That is the
//     whole point of the polling-cost story — a short poll pays full request/response headers every
//     time, even when the body is empty.
//   - Persistent transports (sse / ws): bytesSent is the sum of application frame/message byte
//     lengths (deterministic and reproducible). The one-time connect headers are amortized to ~0
//     over a long stream; WebSocket frame overhead (2-14 B/msg) is noted separately, not folded in.
//   A `liveWireBytes` gauge (open-connection socket.bytesWritten deltas) is also exposed for sse/ws
//   as a wire-level cross-check.
import { monitorEventLoopDelay } from 'node:perf_hooks';
import { config } from './config.js';
import { nowMs } from './clock.js';

const TRANSPORTS = ['poll', 'pollDebounced', 'longpoll', 'sse', 'ws'];

const newCounters = () => ({
  connections: 0, // gauge: currently-open (longpoll in-flight, sse/ws live)
  requests: 0, // counter: completed HTTP requests (poll/debounced/longpoll)
  eventsDelivered: 0, // counter: events written to clients
  bytesSent: 0, // see byte-accounting note above
  missed: 0, // counter: buffer-overrun losses reported to clients
  backpressure: 0, // counter: write-buffer-full events (slow client signal)
});

const state = {
  startedMs: nowMs(),
  redisEventsReceived: 0,
  transports: Object.fromEntries(TRANSPORTS.map((t) => [t, newCounters()])),
};

const gauges = {}; // transport -> () => ({ ...extra fields }) e.g. live wire bytes

// Event-loop delay is a clean saturation signal: when the server is overwhelmed (e.g. 10k WS
// fan-out), the loop falls behind and tail latency balloons. Reset per scrape => per-interval stats.
const eld = monitorEventLoopDelay({ resolution: 10 });
eld.enable();

export const metrics = {
  inc(t, k, n = 1) {
    state.transports[t][k] += n;
  },
  dec(t, k, n = 1) {
    state.transports[t][k] -= n;
  },
  redisEvent() {
    state.redisEventsReceived++;
  },
  registerGauge(t, fn) {
    gauges[t] = fn;
  },
  snapshot() {
    const transports = {};
    for (const t of TRANSPORTS) {
      transports[t] = { ...state.transports[t], ...(gauges[t] ? gauges[t]() : {}) };
    }
    const snap = {
      instanceId: config.instanceId,
      serverTime: nowMs(),
      uptimeSec: (nowMs() - state.startedMs) / 1000,
      redisEventsReceived: state.redisEventsReceived,
      process: {
        cpuMicros: process.cpuUsage(), // cumulative user+system micros; harness deltas this
        memory: process.memoryUsage(),
        eventLoopDelayMs: {
          mean: eld.mean / 1e6,
          p50: eld.percentile(50) / 1e6,
          p99: eld.percentile(99) / 1e6,
          max: eld.max / 1e6,
        },
      },
      transports,
    };
    eld.reset(); // make each scrape an independent interval sample
    return snap;
  },
};
