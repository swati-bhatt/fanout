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
export function nowMs() {
  return performance.timeOrigin + performance.now();
}
