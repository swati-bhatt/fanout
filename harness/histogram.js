// Bounded log-bucket histogram — latency percentiles at 10k-client scale without unbounded memory.
// 32 buckets/decade over 1µs..100s => 256 buckets (~1KB); value error ≤ 10^(1/32) ≈ 7.5%, which is
// far below the run-to-run variance we care about. Exact count/sum/min/max kept alongside.
// Workers serialize with toJSON(); the orchestrator merges with merge() (bucket-wise add).
export const PER_DECADE = 32;
export const DECADES = 8;
export const NBUCKETS = PER_DECADE * DECADES;
export const MIN_MS = 0.001; // 1µs

export class Histogram {
  constructor() {
    this.buckets = new Uint32Array(NBUCKETS);
    this.count = 0;
    this.sum = 0;
    this.clamped = 0;
    this.min = Infinity;
    this.max = 0;
  }

  record(ms) {
    if (!Number.isFinite(ms)) return;
    if (ms < 0) {
      // Residual cross-process clock skew (see src/clock.js): clamp to 0 and COUNT it, so the
      // sample stays in the distribution and the result reports how often the resolution floor
      // was hit. Silently dropping these censored 31.8% of one SSE run -- the fastest samples.
      this.clamped++;
      ms = 0;
    }
    this.count++;
    this.sum += ms;
    if (ms < this.min) this.min = ms;
    if (ms > this.max) this.max = ms;
    const idx = Math.min(
      NBUCKETS - 1,
      Math.max(0, Math.floor(Math.log10(Math.max(ms, MIN_MS) / MIN_MS) * PER_DECADE)),
    );
    this.buckets[idx]++;
  }

  merge(other) {
    for (let i = 0; i < NBUCKETS; i++) this.buckets[i] += other.buckets[i];
    this.count += other.count;
    this.sum += other.sum;
    this.clamped += other.clamped;
    if (other.min < this.min) this.min = other.min;
    if (other.max > this.max) this.max = other.max;
  }

  percentile(p) {
    if (!this.count) return null;
    const target = Math.ceil((this.count * p) / 100);
    let c = 0;
    for (let i = 0; i < NBUCKETS; i++) {
      c += this.buckets[i];
      if (c >= target) {
        const lo = MIN_MS * 10 ** (i / PER_DECADE);
        const hi = MIN_MS * 10 ** ((i + 1) / PER_DECADE);
        return Math.min(Math.sqrt(lo * hi), this.max); // geometric midpoint, clamped to observed max
      }
    }
    return this.max;
  }

  summary() {
    if (!this.count) return { count: 0 };
    const r = (x) => (x == null ? null : +x.toFixed(3));
    return {
      count: this.count,
      clampedSubResolution: this.clamped,
      mean: r(this.sum / this.count),
      min: r(this.min),
      p50: r(this.percentile(50)),
      p90: r(this.percentile(90)),
      p95: r(this.percentile(95)),
      p99: r(this.percentile(99)),
      p999: r(this.percentile(99.9)),
      max: r(this.max),
    };
  }

  toJSON() {
    return {
      buckets: Array.from(this.buckets),
      count: this.count,
      sum: this.sum,
      clamped: this.clamped,
      min: this.min === Infinity ? null : this.min,
      max: this.max,
    };
  }

  static fromJSON(j) {
    const h = new Histogram();
    j.buckets.forEach((v, i) => (h.buckets[i] = v));
    h.count = j.count;
    h.sum = j.sum;
    h.clamped = j.clamped ?? 0;
    h.min = j.min ?? Infinity;
    h.max = j.max;
    return h;
  }
}
