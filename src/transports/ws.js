// WebSocket. A single upgraded TCP connection carrying full-duplex frames. Lowest per-message
// overhead and the only transport that also does client->server cheaply (used for the optional
// typeahead axis via the ping/pong echo). Costs: a persistent connection + memory per client (the
// C10K pressure), manual reconnect, and explicit heartbeats to detect dead peers. We also watch
// bufferedAmount to surface backpressure to slow clients.
import { WebSocketServer } from 'ws';
import { config } from '../config.js';
import { metrics } from '../metrics.js';
import { nowMs } from '../clock.js';

export function createWS(buffer) {
  const wss = new WebSocketServer({ noServer: true });
  const startBW = new WeakMap(); // ws -> socket.bytesWritten at connect (for wire-byte gauge)

  function sendEvent(ws, evJson) {
    if (ws.readyState !== ws.OPEN) return;
    if (ws.bufferedAmount > config.wsMaxBufferedBytes) {
      metrics.inc('ws', 'backpressure', 1); // slow client: skip rather than grow an unbounded buffer
      return;
    }
    ws.send(evJson);
    metrics.inc('ws', 'eventsDelivered', 1);
    metrics.inc('ws', 'bytesSent', Buffer.byteLength(evJson)); // app bytes; frame overhead ~2-14B/msg
  }

  wss.on('connection', (ws, req) => {
    ws.isAlive = true;
    ws.on('pong', () => {
      ws.isAlive = true;
    });
    metrics.inc('ws', 'connections', 1);
    startBW.set(ws, ws._socket ? ws._socket.bytesWritten : 0);

    const since = Number(new URL(req.url, 'http://localhost').searchParams.get('since') ?? -1);
    for (const ev of buffer.since(since, config.pollMaxBatch).events) sendEvent(ws, JSON.stringify(ev));

    ws.on('message', (data) => {
      let msg;
      try {
        msg = JSON.parse(data.toString());
      } catch {
        return;
      }
      if (msg.type === 'ping') {
        // axis 2: client->server->client RTT (typeahead), comparable to a debounced HTTP POST.
        ws.send(JSON.stringify({ type: 'pong', t: msg.t, serverTime: nowMs() }));
      } else if (msg.type === 'resume') {
        for (const ev of buffer.since(Number(msg.since ?? -1), config.pollMaxBatch).events) sendEvent(ws, JSON.stringify(ev));
      }
    });

    ws.on('close', () => metrics.dec('ws', 'connections', 1));
  });

  // Heartbeat: ping everyone each interval; terminate any socket that missed the previous pong.
  const hb = setInterval(() => {
    for (const ws of wss.clients) {
      if (!ws.isAlive) {
        ws.terminate();
        continue;
      }
      ws.isAlive = false;
      ws.ping();
    }
  }, config.wsHeartbeatMs);
  hb.unref?.();

  return {
    attach(server) {
      // Manual upgrade handling so only /ws is a WebSocket; everything else stays HTTP.
      server.on('upgrade', (req, socket, head) => {
        let pathname;
        try {
          pathname = new URL(req.url, 'http://localhost').pathname;
        } catch {
          socket.destroy();
          return;
        }
        if (pathname === '/ws') {
          wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
        } else {
          socket.destroy();
        }
      });
    },
    onEvent(_ev, evJson) {
      for (const ws of wss.clients) sendEvent(ws, evJson);
    },
    liveWireBytes() {
      let s = 0;
      for (const ws of wss.clients) {
        const start = startBW.get(ws);
        if (start != null && ws._socket) s += ws._socket.bytesWritten - start;
      }
      return s;
    },
    close() {
      clearInterval(hb);
      for (const ws of wss.clients) ws.terminate();
      wss.close();
    },
  };
}
