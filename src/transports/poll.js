// Short polling. Client repeatedly asks "anything after seq X?"; server answers immediately, even
// when empty. This is the baseline: simple, stateless, but pays a full HTTP round-trip per poll and
// its freshness is bounded by the client's poll interval (that delay shows up in end-to-end latency).
import { config } from '../config.js';
import { metrics } from '../metrics.js';
import { nowMs } from '../clock.js';

export function createPoll(buffer) {
  return {
    register(fastify) {
      fastify.get('/poll', async (req) => {
        const since = Number(req.query.since ?? -1);
        const { events, missed, tailSeq } = buffer.since(since, config.pollMaxBatch);
        metrics.inc('poll', 'eventsDelivered', events.length);
        if (missed) metrics.inc('poll', 'missed', missed);
        // requests + wire bytes (incl. headers) are counted by the server's onResponse hook.
        return { events, tailSeq, missed, serverTime: nowMs() };
      });
    },
  };
}
