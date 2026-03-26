'use strict';
const { TumblingWindows } = require('./windows');

/**
 * orders.v1 → [validate → window] → store-revenue.v1 (+ orders.late.v1, orders.dlq.v1)
 *
 * Exactly-once: each batch's outputs, the window-state checkpoint and the consumed offsets are
 * written in ONE Kafka transaction (read-process-write). A crash anywhere before commit aborts the
 * transaction; consumers with isolation.level=read_committed never see partial output.
 *
 * `kafka` is an adapter (kafkajs in production, see kafka.js; in-memory in tests):
 *   transaction() -> { send(topic, messages), sendOffsets({consumerGroupId, topics}), commit(), abort() }
 */

function validate(raw) {
  let e;
  try { e = JSON.parse(raw); } catch { return { error: 'not JSON' }; }
  if (!e || typeof e.eventId !== 'string' || typeof e.storeId !== 'string') return { error: 'missing ids' };
  if (!Number.isFinite(Date.parse(e.occurredAt))) return { error: 'bad occurredAt' };
  if (!Number.isInteger(e.totalCents) || e.totalCents < 0) return { error: 'bad totalCents' };
  if (!Array.isArray(e.lines) || e.lines.some((l) => typeof l.sku !== 'string' || !Number.isInteger(l.qty) || l.qty <= 0)) return { error: 'bad lines' };
  return { event: { eventId: e.eventId, storeId: e.storeId, ts: Date.parse(e.occurredAt), totalCents: e.totalCents, lines: e.lines } };
}

class Pipeline {
  constructor({ kafka, groupId = 'store-revenue', windowOpts, state, metrics = () => {} }) {
    this.kafka = kafka;
    this.groupId = groupId;
    this.windows = state ? TumblingWindows.restore(state, windowOpts) : new TumblingWindows(windowOpts);
    this.windowOpts = windowOpts;
    this.metrics = metrics;
  }

  /** Process one polled batch from a single partition. */
  async processBatch({ topic, partition, messages }) {
    const before = this.windows.snapshot();
    const out = { revenue: [], late: [], dlq: [] };
    for (const m of messages) {
      const v = validate(m.value);
      if (v.error) { out.dlq.push({ key: m.key, value: m.value, headers: { error: v.error, source: `${topic}/${partition}/${m.offset}` } }); continue; }
      const r = this.windows.add(v.event);
      if (r.duplicate) { this.metrics('duplicate'); continue; }
      if (r.late) out.late.push({ key: r.late.storeId, value: JSON.stringify(r.late) });
      for (const w of r.closed) out.revenue.push({ key: w.storeId, value: JSON.stringify(w) });
    }

    const tx = await this.kafka.transaction();
    try {
      if (out.revenue.length) await tx.send('store-revenue.v1', out.revenue);
      if (out.late.length) await tx.send('orders.late.v1', out.late);
      if (out.dlq.length) await tx.send('orders.dlq.v1', out.dlq);
      // Window state lives in a compacted changelog topic, committed atomically with the offsets.
      await tx.send('store-revenue.state.v1', [{ key: `${topic}-${partition}`, value: this.windows.snapshot() }]);
      const next = String(Number(messages[messages.length - 1].offset) + 1);
      await tx.sendOffsets({ consumerGroupId: this.groupId, topics: [{ topic, partitions: [{ partition, offset: next }] }] });
      await tx.commit();
    } catch (e) {
      await tx.abort();
      this.windows = TumblingWindows.restore(before, this.windowOpts); // in-memory state rolls back with the transaction
      throw e;
    }
    this.metrics('batch', { revenue: out.revenue.length, late: out.late.length, dlq: out.dlq.length });
    return out;
  }
}

module.exports = { Pipeline, validate };
