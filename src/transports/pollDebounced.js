// Adaptive / debounced polling. Same as short poll, but the server returns a `nextPollMs` hint:
// fast when events are flowing, exponentially backing off while idle. The client honors the hint,
// so quiet periods cost far fewer wasted requests than fixed-interval short polling — at the price
// of staleness that grows with the backoff. This transport exists to quantify that trade.
import { config } from '../config.js';
import { metrics } from '../metrics.js';
import { nowMs } from '../clock.js';

export function createPollDebounced(buffer) {
  return {
    register(fastify) {
      fastify.get('/poll-debounced', async (req) => {
        const since = Number(req.query.since ?? -1);
        const idle = Number(req.query.idle ?? 0); // consecutive empty polls the client has seen
        const { events, missed, tailSeq } = buffer.since(since, config.pollMaxBatch);
        metrics.inc('pollDebounced', 'eventsDelivered', events.length);
        if (missed) metrics.inc('pollDebounced', 'missed', missed);

        const nextPollMs =
          events.length > 0
            ? config.pollMinMs // activity -> poll fast
            : Math.min(
                config.pollMinMs * config.pollBackoffFactor ** idle,
                config.pollMaxMs,
              ); // idle -> back off, capped

        return { events, tailSeq, missed, nextPollMs, serverTime: nowMs() };
      });
    },
  };
}
