// Load-generator worker: a forked child process holding N clients of ONE transport.
// Several workers run in parallel (one per core, roughly) so the GENERATOR never bottlenecks on a
// single event loop — and each worker measures its own event-loop delay while recording, so a run
// where the generator itself saturated is detectable (run.js flags it) instead of silently
// inflating the server's latency numbers.
//
// IPC protocol (parent = harness/run.js):
//   parent -> {cmd:'start', cfg}   connect clients, staggered over cfg.rampMs
//   child  -> {type:'ready'}       every client is connected / its loop is running
//   parent -> {cmd:'measure'}      begin recording latency/bytes/missed
//   parent -> {cmd:'stop'}         stop recording, reply {type:'result', ...}
//   parent -> {cmd:'shutdown'}     tear down and exit
//
// Measurement rules (mirror the paper's methodology):
//   latency  = nowMs() - ev.t_emit per received event (same-host clock, see src/clock.js)
//   missed   = gaps in the per-client seq stream, counted uniformly for ALL transports
//              (covers buffer overruns for pollers AND server-side backpressure skips for ws)
//   appBytes = application-level bytes received (poll: JSON body; sse: stream chunks incl framing
//              and heartbeats; ws: message payloads). Wire-level bytes incl HTTP headers come from
//              the SERVER's socket-level accounting (/metrics), which run.js records alongside.
import http from 'node:http';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import WebSocket from 'ws';
import { nowMs } from '../src/clock.js';
import { Histogram } from './histogram.js';

let cfg = null;
let recording = false;
let stopped = false;
let readySent = false;
let opened = 0;
let agent = null;
let eld = null;
const hist = new Histogram();
const stats = { events: 0, appBytes: 0, requests: 0, missed: 0, errors: 0, reconnects: 0, connectFailures: 0 };
const closers = [];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = (ms) => ms * (0.9 + Math.random() * 0.2); // ±10%: real fleets aren't phase-locked

function maybeReady() {
  if (!readySent && opened >= cfg.clients) {
    readySent = true;
    process.send({ type: 'ready' });
  }
}

function onEvent(c, ev) {
  if (typeof ev.seq !== 'number') return;
  if (recording) {
    stats.events++;
    if (typeof ev.t_emit === 'number') hist.record(nowMs() - ev.t_emit);
    if (c.lastSeq != null && ev.seq > c.lastSeq + 1) stats.missed += ev.seq - c.lastSeq - 1;
  }
  if (c.lastSeq == null || ev.seq > c.lastSeq) c.lastSeq = ev.seq;
}

function getJson(path) {
  return new Promise((resolve, reject) => {
    const req = http.get({ host: cfg.host, port: cfg.port, path, agent }, (res) => {
      const chunks = [];
      let bytes = 0;
      res.on('data', (d) => {
        bytes += d.length;
        chunks.push(d);
      });
      res.on('end', () => {
        try {
          resolve({ json: JSON.parse(Buffer.concat(chunks).toString()), bytes });
        } catch (e) {
          reject(e);
        }
      });
      res.on('error', reject);
    });
    req.on('error', reject);
  });
}

// --- short poll + adaptive poll (same loop; adaptive honors the server's nextPollMs hint) ---
async function pollLoop(c, path, adaptive) {
  opened++;
  maybeReady();
  await sleep(Math.random() * cfg.pollIntervalMs); // desynchronize initial phase
  while (!stopped) {
    let waitMs = cfg.pollIntervalMs;
    try {
      const q = `${path}?since=${c.cursor}` + (adaptive ? `&idle=${c.idle}` : '');
      const { json, bytes } = await getJson(q);
      if (recording) {
        stats.requests++;
        stats.appBytes += bytes;
      }
      for (const ev of json.events) onEvent(c, ev);
      // advance to the last event actually RECEIVED -- not tailSeq, which would skip any
      // batch-capped backlog and then miscount the skipped events as loss
      if (json.events.length) c.cursor = json.events[json.events.length - 1].seq;
      else if (json.tailSeq != null) c.cursor = json.tailSeq;
      c.idle = json.events.length ? 0 : c.idle + 1;
      if (adaptive && json.nextPollMs != null) waitMs = Math.min(Math.max(json.nextPollMs, 10), 10000);
    } catch {
      if (recording) stats.errors++;
    }
    await sleep(jitter(waitMs));
  }
}

// --- long poll: immediately re-poll after every response (server holds when no data) ---
async function longpollLoop(c) {
  opened++;
  maybeReady();
  await sleep(Math.random() * 500); // stagger the initial connection burst only
  while (!stopped) {
    try {
      const { json, bytes } = await getJson(`/longpoll?since=${c.cursor}`);
      if (recording) {
        stats.requests++;
        stats.appBytes += bytes;
      }
      for (const ev of json.events) onEvent(c, ev);
      if (json.events.length) c.cursor = json.events[json.events.length - 1].seq;
      else if (json.tailSeq != null) c.cursor = json.tailSeq;
      else await sleep(100); // stream has produced nothing yet; don't spin on bootstrap
    } catch {
      if (recording) stats.errors++;
      await sleep(250); // don't spin against a dead server
    }
  }
}

// --- SSE: one persistent response, parse frames incrementally; reconnect with since=lastSeq ---
function sseClient(c) {
  let done = false;
  const finish = () => {
    if (done || stopped) return;
    done = true;
    if (recording) stats.reconnects++;
    c.cursor = c.lastSeq ?? -1;
    setTimeout(() => sseClient(c), 250);
  };
  const req = http.get(
    {
      host: cfg.host,
      port: cfg.port,
      path: `/sse?since=${c.cursor}`,
      agent: false, // dedicated socket per SSE client, like a real browser connection
      headers: { accept: 'text/event-stream' },
    },
    (res) => {
      if (!c.openedOnce) {
        c.openedOnce = true;
        opened++;
        maybeReady();
      }
      let buf = '';
      res.on('data', (chunk) => {
        if (recording) stats.appBytes += chunk.length; // includes SSE framing + heartbeats
        buf += chunk.toString();
        let i;
        while ((i = buf.indexOf('\n\n')) >= 0) {
          const frame = buf.slice(0, i);
          buf = buf.slice(i + 2);
          const line = frame.split('\n').find((l) => l.startsWith('data:'));
          if (!line) continue;
          try {
            onEvent(c, JSON.parse(line.slice(5).trim()));
          } catch {
            /* heartbeat/partial */
          }
        }
      });
      res.on('end', finish);
      res.on('close', finish);
      res.on('error', finish);
    },
  );
  req.on('error', () => {
    if (!c.openedOnce) {
      c.openedOnce = true;
      opened++;
      stats.connectFailures++;
      maybeReady();
    }
    if (recording) stats.errors++;
    finish();
  });
  closers[c.idx] = () => {
    done = true;
    req.destroy();
  };
}

// --- WebSocket: persistent duplex; reconnect with since=lastSeq on unexpected close ---
function wsClient(c) {
  const ws = new WebSocket(`ws://${cfg.host}:${cfg.port}/ws?since=${c.cursor}`);
  ws.on('open', () => {
    if (!c.openedOnce) {
      c.openedOnce = true;
      opened++;
      maybeReady();
    }
  });
  ws.on('message', (data) => {
    if (recording) stats.appBytes += data.length;
    try {
      const ev = JSON.parse(data.toString());
      if (ev.seq != null) onEvent(c, ev);
    } catch {
      /* control frame */
    }
  });
  ws.on('error', () => {
    if (recording) stats.errors++;
  });
  ws.on('close', () => {
    if (stopped) return;
    if (!c.openedOnce) {
      c.openedOnce = true;
      opened++;
      stats.connectFailures++;
      maybeReady();
    }
    if (recording) stats.reconnects++;
    c.cursor = c.lastSeq ?? -1;
    setTimeout(() => wsClient(c), 250);
  });
  closers[c.idx] = () => ws.terminate();
}

function start(startCfg) {
  cfg = startCfg;
  agent = new http.Agent({ keepAlive: true, maxSockets: Infinity });
  for (let i = 0; i < cfg.clients; i++) {
    const c = { idx: i, cursor: -1, lastSeq: null, idle: 0, openedOnce: false };
    const delay = (i / cfg.clients) * cfg.rampMs; // stagger connections: no thundering-herd connect
    setTimeout(() => {
      if (stopped) return;
      switch (cfg.transport) {
        case 'poll':
          pollLoop(c, '/poll', false);
          break;
        case 'poll-debounced':
          pollLoop(c, '/poll-debounced', true);
          break;
        case 'longpoll':
          longpollLoop(c);
          break;
        case 'sse':
          sseClient(c);
          break;
        case 'ws':
          wsClient(c);
          break;
        default:
          process.send({ type: 'fatal', error: `unknown transport ${cfg.transport}` });
          process.exit(1);
      }
    }, delay);
  }
}

process.on('message', (m) => {
  if (m.cmd === 'start') start(m.cfg);
  else if (m.cmd === 'measure') {
    eld = monitorEventLoopDelay({ resolution: 10 });
    eld.enable();
    recording = true;
  } else if (m.cmd === 'stop') {
    recording = false;
    process.send({
      type: 'result',
      ...stats,
      latency: hist.toJSON(),
      loopDelayP99Ms: eld ? eld.percentile(99) / 1e6 : null,
    });
  } else if (m.cmd === 'shutdown') {
    stopped = true;
    for (const f of closers) f && f();
    if (agent) agent.destroy();
    setTimeout(() => process.exit(0), 100);
  }
});
