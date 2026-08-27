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
npm run sweep -- --tier=core  --reps=5   # headline cells: 5 transports x {50,500,2000} @20ev/s
npm run sweep -- --tier=rate  --reps=3   # arrival-rate axis: c=500 x {1,20,100} ev/s
npm run sweep -- --tier=scale --reps=3   # C10K frontier: {5000,10000} clients
npm run aggregate                        # median + spread per cell, with trust gating
```

**Replicates are interleaved, not blocked.** `--reps=5` runs every cell once, then every cell
again, rotating the order each round — rather than five back-to-back runs of one cell. Blocked
replicates confound a cell with whatever the machine was doing during its block; interleaving
spreads that noise across all cells so between-transport differences survive it.

`aggregate.js` reports the **median** across replicates (one contaminated run shifts a mean and
barely moves a median — the exact failure mode that invalidated an earlier sweep) together with
each cell's p99 min/max/spread. A cell is stamped `stable` only at n≥3 **and** spread ≤2×;
everything else is named in a trust summary so it cannot quietly become a claim.

## Flaky-network axis

macOS has no `tc`/`netem`, so impairment runs put **only the server** in a Linux container —
producer, clients, and Redis stay on the host so every timestamp comes from one clock.

```bash
docker compose -f docker/compose.yml up -d --build
./docker/netem.sh wifi        # clean | wifi | mobile | lossy | satellite | flapping
npm run netem -- --transport=ws --clients=500 --profile=wifi
./docker/netem.sh clear
```

Loss is **correlated** (real networks lose in bursts; uncorrelated loss unrealistically flatters
per-packet recovery). Packet reordering is deliberately off — TCP treats it as loss and it would
confound the loss axis. Shaping is egress-only (server→client), so a polling transport's request
leg stays fast: any polling disadvantage measured here is a **lower** bound.

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

- **Loopback ≠ WAN.** The unimpaired sweeps have no real RTT or loss, so they measure per-instance
  efficiency and *relative* transport behavior — not internet-scale absolute throughput. The netem
  axis above supplies impairment, with the caveat that a container's virtual NIC adds its own
  ~0.1–0.5ms and that shaping is egress-only.
- **One host, one server process.** Redis fan-out means K instances *could* run, but every number
  here is single-instance. Horizontal scaling is a design property demonstrated, not a measurement.
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

- [x] M1 - server: 5 transports, producer, Redis fan-out, metrics (verified end-to-end)
- [x] M2 - load harness: parallel workers, warmup/measure phases, per-run JSON + sweep CSV
- [x] M3a - replicated sweeps (interleaved reps, median + spread, trust gating)
- [x] M3b - flaky-network rig: containerized server + correlated-loss netem profiles
- [ ] M3c - run the impairment matrix (profile x transport) and fold it into the dataset
- [ ] M4 - analysis + writeup: freshness-vs-cost frontier, crossover points, decision rule
