# fanout

**A measurement study of live-update delivery.** One trivial event stream, served five ways —
short polling, adaptive polling, long polling, Server-Sent Events, WebSocket — and measured
head-to-head: end-to-end latency (p50/p95/p99), bytes per client, server CPU/memory, and
missed-update rate, from 10 to 10,000 concurrent clients.

The application is deliberately trivial. The contribution is the rigor of the comparison and the
resulting **decision rule**: *for a given freshness target and client count, which transport wins,
and where are the crossover points?*

## Architecture

```
producer.js ──publish──▶ Redis pub/sub ──subscribe──▶ server.js ──▶ ring buffer (seq-indexed)
                                                          │
              ┌────────────┬──────────────┬───────────────┼────────────┬───────────┐
           /poll     /poll-debounced   /longpoll         /sse         /ws       /metrics
        (baseline)   (server-driven    (held request,  (persistent  (persistent  (JSON for
                      backoff hint)     wake-on-event)  HTTP stream)  duplex)     the harness)
```

- Every event carries a monotonic `seq` and a high-resolution `t_emit` timestamp.
  **Latency** = `t_recv − t_emit` per event; **loss** = gaps in `seq` — computed identically for
  all five transports, which is what makes the comparison apples-to-apples.
- The producer is a **separate process** (its CPU never pollutes the server measurement) and
  publishes through Redis, so K server instances can fan out the same stream horizontally.
- A per-instance ring buffer serves poll/long-poll reads and reconnect resync
  (`?since=<lastSeq>`, or SSE's native `Last-Event-ID`), and makes buffer-overrun loss explicit.

## Load harness

`harness/run.js` runs one scenario: fresh server + fresh producer + N clients spread across
parallel worker processes (~one per core), with a connect ramp, a warmup phase excluded from
stats, then a recorded measurement window. Results land in `results/<runId>.json`.

```bash
npm run bench -- --transport=ws --clients=2000 --rate=20 --duration=20
npm run sweep -- --tier=mini    # 17 runs, ~12 min: 5 transports x {50,500,2000} clients + low-rate pair
npm run sweep -- --tier=full    # 75 runs, ~60-75 min: adds 5000/10000 clients and {1,20,100} ev/s
```

Measurement-integrity details that matter:

- **Clock** — high-resolution monotonic time, calibrated per process against the shared system
  clock (median of 301 `Date.now()` samples), because raw `performance.timeOrigin` carries
  ~0.3–1.5ms of per-process error. Cross-process agreement is then bounded at ~0.5ms — the
  instrument's resolution floor. Latencies that still measure negative are clamped to 0 and
  **counted** (`clampedSubResolution`), never silently dropped: silent dropping once censored
  31.8% of an SSE run's samples — all of them the fastest ones.
- **Generator saturation is detected, not ignored** — each load worker records its own event-loop
  delay; a run where the *generator* stalled is flagged `generatorLimited` rather than silently
  reported as server latency.
- **Byte accounting: one semantics** — the published bytes-per-client figure is socket-level wire
  bytes for every transport (HTTP responses incl. headers; SSE/WS via a `socket.bytesWritten`
  gauge incl. framing and heartbeats). App-level counters are kept alongside as a cross-check —
  the two meters agree to ~1% and imply an identical ~270B header cost per HTTP response.
- **Event-loop delay is reported as slices** — the delay histogram resets per 2s scrape, so the
  headline is labeled `sliceMax` (worst 2s slice, upward-biased by design) with the median slice
  published alongside; a single burst can't masquerade as typical behavior.
- **Orphan-proof runs** — every scenario gets a bind-probed free port, and the orchestrator asserts
  the responding server's `instanceId` matches the run it just launched — added after a leftover
  server from a killed sweep silently served (and corrupted) a later cell's measurements.
- **Serialize-once fan-out** — the server parses each Redis message once and reuses the original
  JSON string for every SSE/WS client, so results reflect transport overhead, not `JSON.stringify`.
- **Calibration** — measured short-poll latency matches the analytical model (p50 ≈ interval/2,
  p95 ≈ interval for uniform arrival), validating the instrument end to end.

## Known limits (stated up front)

- **Loopback ≠ WAN.** No real RTT or loss on localhost; results measure per-instance efficiency
  and *relative* transport behavior, not internet-scale absolute throughput. Flaky-network runs
  (loss/delay injection) require Linux `tc`/`netem` — planned via Docker.
- **Single-host clock.** Cross-process latency relies on a shared system clock; multi-host runs
  would need explicit clock-offset estimation.
- Requires Node ≥ 18 and a local Redis (`redis-cli ping` → `PONG`). Everything runs locally at $0.

## Quick start

```bash
npm install
npm run smoke   # spawns server + producer, probes all 5 transports, prints a latency table (~10 s)
npm run dev     # leaves server + producer running for manual poking
```

## Status

- [x] M1 — server: 5 transports, producer, Redis fan-out, metrics (verified end-to-end)
- [x] M2 — load harness: parallel workers, warmup/measure phases, per-run JSON + sweep CSV
- [ ] M3 — full sweep incl. flaky-network axis (netem in Docker)
- [ ] M4 — analysis + writeup: freshness-vs-cost frontier, crossover points, decision rule
