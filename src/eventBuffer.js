// Per-instance ring buffer of recent events, indexed by sequence number.
//
// Every transport delivers events by `seq`, which makes them apples-to-apples AND gives the load
// client a clean way to compute the missed-update rate (gaps in the seq it receives). The buffer
// also backs reconnect/resync: a client that reconnects with `since=<lastSeq>` gets the gap.
//
// ASSUMPTION: a single producer publishes strictly monotonic, contiguous seq (0,1,2,...). That lets
// us locate any seq by arithmetic offset instead of a scan. With multiple producers you'd namespace
// seq per producer; out of scope for Milestone 1 (documented).
export class EventBuffer {
  constructor(capacity) {
    this.capacity = capacity;
    this.buf = new Array(capacity);
    this.size = 0; // number of events currently retained
    this.start = 0; // ring index of the oldest retained event
  }

  push(event) {
    const idx = (this.start + this.size) % this.capacity;
    this.buf[idx] = event;
    if (this.size < this.capacity) {
      this.size++;
    } else {
      // Full: idx === start, so we just overwrote the oldest. Advance start to the new oldest.
      this.start = (this.start + 1) % this.capacity;
    }
  }

  get headSeq() {
    return this.size ? this.buf[this.start].seq : null; // oldest retained seq
  }

  get tailSeq() {
    return this.size ? this.buf[(this.start + this.size - 1) % this.capacity].seq : null; // newest
  }

  // Return events strictly after `sinceSeq`, capped at maxBatch.
  //   sinceSeq < 0 (or absent)  -> "live tail" join: no backfill, no missed, just report tailSeq.
  //   sinceSeq fell off buffer  -> report `missed` (buffer-overrun loss for a slow poller).
  since(sinceSeq, maxBatch) {
    const tail = this.tailSeq;
    if (this.size === 0 || sinceSeq < 0 || sinceSeq >= tail) {
      return { events: [], missed: 0, tailSeq: tail };
    }
    const head = this.headSeq;
    let fromSeq = sinceSeq + 1;
    let missed = 0;
    if (fromSeq < head) {
      missed = head - fromSeq; // events evicted before this client could fetch them
      fromSeq = head;
    }
    const offset = fromSeq - head; // logical distance from the oldest retained event
    const available = this.size - offset; // events from fromSeq..tail inclusive
    const n = Math.min(available, maxBatch);
    const events = new Array(n);
    for (let i = 0; i < n; i++) {
      events[i] = this.buf[(this.start + offset + i) % this.capacity];
    }
    return { events, missed, tailSeq: tail };
  }
}
