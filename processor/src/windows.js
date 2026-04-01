'use strict';
/**
 * Event-time tumbling windows with a watermark, the core of the real-time revenue stream.
 *
 * Order events arrive out of order (mobile clients retry, store POS batches uploads). Windows are
 * keyed by (store, window start) on the event's own timestamp. A window closes when the watermark
 * (max event time seen − allowed lateness) passes its end; events for an already-closed window go to
 * a late side-output (corrections topic) instead of silently changing published numbers.
 */
class TumblingWindows {
  constructor({ sizeMs = 60_000, allowedLatenessMs = 30_000, dedupeTtlMs = 15 * 60_000 } = {}) {
    Object.assign(this, { sizeMs, allowedLatenessMs, dedupeTtlMs });
    this.open = new Map(); // key -> aggregate
    this.maxEventTime = -Infinity;
    this.seen = new Map(); // eventId -> eventTime (dedupe at-least-once redeliveries)
  }

  windowStart(ts) { return Math.floor(ts / this.sizeMs) * this.sizeMs; }
  watermark() { return this.maxEventTime - this.allowedLatenessMs; }

  /** @returns {{ closed: object[], late: object|null, duplicate: boolean }} */
  add(event) {
    if (this.seen.has(event.eventId)) return { closed: [], late: null, duplicate: true };
    this.seen.set(event.eventId, event.ts);
    const start = this.windowStart(event.ts);
    if (start + this.sizeMs <= this.watermark()) {
      return { closed: this.advance(event.ts), late: { ...event, windowStart: start }, duplicate: false };
    }
    const key = `${event.storeId}|${start}`;
    let w = this.open.get(key);
    if (!w) {
      w = { storeId: event.storeId, windowStart: start, windowEnd: start + this.sizeMs, orders: 0, revenueCents: 0, units: 0, skus: new Map() };
      this.open.set(key, w);
    }
    w.orders += 1;
    w.revenueCents += event.totalCents;
    for (const l of event.lines) {
      w.units += l.qty;
      w.skus.set(l.sku, (w.skus.get(l.sku) ?? 0) + l.qty);
    }
    return { closed: this.advance(event.ts), late: null, duplicate: false };
  }

  /** Move the watermark and emit every window that can no longer change, in event-time order. */
  advance(ts) {
    if (ts > this.maxEventTime) this.maxEventTime = ts;
    const wm = this.watermark();
    const closed = [];
    for (const [k, w] of this.open) {
      if (w.windowEnd <= wm) {
        closed.push(finalize(w));
        this.open.delete(k);
      }
    }
    for (const [id, t] of this.seen) if (t < wm - this.dedupeTtlMs) this.seen.delete(id);
    return closed.sort((a, b) => a.windowStart - b.windowStart || a.storeId.localeCompare(b.storeId));
  }

  /** Idle partitions would stall the watermark; a wall-clock tick moves it forward (bounded by idleness). */
  tick(nowMs, idleMs = 120_000) {
    if (nowMs - idleMs > this.maxEventTime) return this.advance(nowMs - idleMs);
    return [];
  }

  /** State for checkpointing alongside the committed offsets. */
  snapshot() {
    return JSON.stringify({ maxEventTime: this.maxEventTime, open: [...this.open].map(([k, w]) => [k, { ...w, skus: [...w.skus] }]), seen: [...this.seen] });
  }

  static restore(json, opts) {
    const s = JSON.parse(json);
    const t = new TumblingWindows(opts);
    t.maxEventTime = s.maxEventTime;
    t.open = new Map(s.open.map(([k, w]) => [k, { ...w, skus: new Map(w.skus) }]));
    t.seen = new Map(s.seen);
    return t;
  }
}

function finalize(w) {
  const top = [...w.skus].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])).slice(0, 5).map(([sku, units]) => ({ sku, units }));
  return { storeId: w.storeId, windowStart: new Date(w.windowStart).toISOString(), windowEnd: new Date(w.windowEnd).toISOString(),
    orders: w.orders, revenueCents: w.revenueCents, units: w.units, avgOrderCents: Math.round(w.revenueCents / w.orders), topSkus: top };
}

module.exports = { TumblingWindows };
