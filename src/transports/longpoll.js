// Long polling. The server HOLDS the request open until either a newer event arrives or a timeout
// fires (then it returns empty and the client immediately re-polls). Near-real-time latency without
// a persistent protocol, but each waiting client ties up an HTTP request/connection — and over
// HTTP/1.1 a held request blocks that connection (head-of-line blocking; browsers cap ~6/host).
// The `since` cursor closes the gap between responses so nothing is missed across reconnects.
import { config } from '../config.js';
import { metrics } from '../metrics.js';
import { nowMs } from '../clock.js';

export function createLongPoll(buffer) {
  const waiters = new Set(); // { since, reply, resolve, done, timer }

  function settle(w, body) {
    if (w.done) return;
    w.done = true;
    clearTimeout(w.timer);
    waiters.delete(w);
    metrics.dec('longpoll', 'connections');
    if (!w.reply.sent) w.reply.send(body);
    w.resolve();
  }

  return {
    register(fastify) {
      fastify.get('/longpoll', (req, reply) => {
        const since = Number(req.query.since ?? -1);

        // Cursor bootstrap: since<0 means "join at the live tail". A waiter registered with -1
        // would never match (buffer.since treats <0 as the no-backfill sentinel) and hang until
        // timeout — so hand the client its cursor immediately; it re-polls with since=tailSeq.
        if (since < 0) {
          reply.send({ events: [], tailSeq: buffer.tailSeq, missed: 0, bootstrap: true, serverTime: nowMs() });
          return;
        }

        // Fast path: data already available -> answer now.
        const now = buffer.since(since, config.pollMaxBatch);
        if (now.events.length || now.missed) {
          metrics.inc('longpoll', 'eventsDelivered', now.events.length);
          if (now.missed) metrics.inc('longpoll', 'missed', now.missed);
          reply.send({ events: now.events, tailSeq: now.tailSeq, missed: now.missed, serverTime: nowMs() });
          return;
        }

        // Slow path: register a waiter and return a promise so Fastify keeps the request open.
        return new Promise((resolve) => {
          const w = { since, reply, resolve, done: false, timer: null };
          w.timer = setTimeout(
            () => settle(w, { events: [], tailSeq: buffer.tailSeq, missed: 0, timeout: true, serverTime: nowMs() }),
            config.longPollTimeoutMs,
          );
          // Client hung up before we answered -> stop waiting, don't try to send.
          reply.raw.on('close', () => {
            if (w.done) return;
            w.done = true;
            clearTimeout(w.timer);
            waiters.delete(w);
            metrics.dec('longpoll', 'connections');
            resolve();
          });
          waiters.add(w);
          metrics.inc('longpoll', 'connections');
        });
      });
    },

    // Called on every new event: wake any waiter that now has data.
    onEvent() {
      for (const w of [...waiters]) {
        const r = buffer.since(w.since, config.pollMaxBatch);
        if (r.events.length || r.missed) {
          metrics.inc('longpoll', 'eventsDelivered', r.events.length);
          if (r.missed) metrics.inc('longpoll', 'missed', r.missed);
          settle(w, { events: r.events, tailSeq: r.tailSeq, missed: r.missed, serverTime: nowMs() });
        }
      }
    },
  };
}
