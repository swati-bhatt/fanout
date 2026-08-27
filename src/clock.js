// High-resolution, wall-clock-anchored timestamp — the backbone of every latency number.
import { performance } from 'node:perf_hooks';

// Returns milliseconds since the Unix epoch as a float with sub-millisecond resolution.
//
// performance.timeOrigin is the wall-clock time (ms since epoch) captured once at process start;
// performance.now() is a high-resolution monotonic offset since then. Their sum is a high-res
// wall-clock reading that, unlike repeated Date.now() calls, is NOT perturbed by mid-run NTP steps.
//
// MEASUREMENT NOTE (put this in the paper's methodology):
//   The producer stamps t_emit; the load client stamps t_recv; end-to-end latency = t_recv - t_emit.
//   These are different PROCESSES. On a SINGLE HOST they share one system clock, so the two stamps
//   are directly comparable — the only error term is each process's calibration of timeOrigin at
//   startup, typically well under 1 ms. The moment producer and consumer live on DIFFERENT hosts,
//   this assumption breaks: you MUST add clock-offset estimation (NTP/PTP-style probe, or echo the
//   timestamp back and halve the RTT) before trusting absolute latencies. Document this boundary.
// Per-process timeOrigin is calibrated once at process start and can be off by ~0.3-1.5ms from
// the system clock (worse under load). Uncorrected, that skew makes sub-ms cross-process
// latencies go NEGATIVE (observed: 31.8% of samples in one SSE run were censored this way).
// Calibrate against Date.now() -- the one clock all local processes share -- using the median
// of many samples; residual cross-process disagreement is then bounded by Date.now() resolution
// (~0.5ms), which is the floor below which this instrument cannot resolve latency. State that
// floor in the paper.
const CAL_SAMPLES = 301;
const offsets = [];
for (let i = 0; i < CAL_SAMPLES; i++) {
  // +0.5 centers Date.now()'s floor-to-ms truncation
  offsets.push(Date.now() + 0.5 - (performance.timeOrigin + performance.now()));
}
offsets.sort((a, b) => a - b);
const CLOCK_OFFSET_MS = offsets[(CAL_SAMPLES - 1) >> 1];

export function nowMs() {
  return performance.timeOrigin + performance.now() + CLOCK_OFFSET_MS;
}
