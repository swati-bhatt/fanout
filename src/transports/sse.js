// Server-Sent Events. One long-lived HTTP response; the server writes `data:` frames as events
// occur. Unidirectional and text-only, but dead simple, rides plain HTTP, and EventSource gives
// automatic reconnect with Last-Event-ID — which we map to our `seq` so a reconnecting client
// resyncs the gap from the buffer. Heartbeat comments keep intermediaries from idling it out.
import { config } from '../config.js';
import { metrics } from '../metrics.js';
import { nowMs } from '../clock.js';

export function createSSE(buffer) {
  const clients = new Set(); // { res, socket, startBW }

  function writeEvent(client, ev, evJson) {
    const frame = `id: ${ev.seq}\ndata: ${evJson}\n\n`;
    const ok = client.res.write(frame);
    metrics.inc('sse', 'eventsDelivered', 1);
    metrics.inc('sse', 'bytesSent', Buffer.byteLength(frame));
    if (!ok) metrics.inc('sse', 'backpressure', 1); // kernel/Node write buffer full => slow client
  }

  return {
    register(fastify) {
      fastify.get('/sse', (req, reply) => {
        reply.hijack(); // take over the socket; Fastify won't serialize/end it
        const res = reply.raw;
        res.writeHead(200, {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache, no-transform',
          Connection: 'keep-alive',
          'Access-Control-Allow-Origin': '*',
          'X-Accel-Buffering': 'no', // disable proxy buffering if one is ever in front
        });
        res.write('retry: 1000\n\n'); // tell EventSource how long to wait before reconnecting

        const socket = req.socket;
        const client = { res, socket, startBW: socket.bytesWritten };
        clients.add(client);
        metrics.inc('sse', 'connections', 1);

        // Resync on (re)connect: explicit ?since= wins, else native Last-Event-ID, else live tail.
        const since = Number(req.query.since ?? req.headers['last-event-id'] ?? -1);
        for (const ev of buffer.since(since, config.pollMaxBatch).events) writeEvent(client, ev, JSON.stringify(ev));

        const hb = setInterval(() => res.write(`: hb ${Math.round(nowMs())}\n\n`), config.sseHeartbeatMs);
        req.raw.on('close', () => {
          clearInterval(hb);
          clients.delete(client);
          metrics.dec('sse', 'connections', 1);
        });
      });
    },

    onEvent(ev, evJson) {
      for (const c of clients) writeEvent(c, ev, evJson);
    },
    liveWireBytes() {
      let s = 0;
      for (const c of clients) s += c.socket.bytesWritten - c.startBW;
      return s;
    },
  };
}
