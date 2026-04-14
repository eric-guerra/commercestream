'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { TumblingWindows } = require('../src/windows');
const { Pipeline, validate } = require('../src/pipeline');

const T0 = Date.parse('2025-03-01T12:00:00Z');
let n = 0;
const ev = (storeId, secs, totalCents = 1000, lines = [{ sku: 'MUG', qty: 1 }], eventId = `e${++n}`) =>
  ({ eventId, storeId, ts: T0 + secs * 1000, totalCents, lines });

test('windows close on the watermark and aggregate per store', () => {
  const w = new TumblingWindows({ sizeMs: 60_000, allowedLatenessMs: 30_000 });
  assert.deepEqual(w.add(ev('S1', 5, 1000, [{ sku: 'MUG', qty: 2 }])).closed, []);
  w.add(ev('S1', 50, 3000, [{ sku: 'TEE', qty: 1 }, { sku: 'MUG', qty: 1 }]));
  w.add(ev('S2', 20, 500));
  assert.deepEqual(w.add(ev('S1', 80)).closed, []); // watermark 50s: window [0,60) still open
  const closed = w.add(ev('S1', 95)).closed; // watermark 65s ≥ 60s
  assert.deepEqual(closed.map((c) => [c.storeId, c.orders, c.revenueCents, c.units]), [['S1', 2, 4000, 4], ['S2', 1, 500, 1]]);
  assert.deepEqual(closed[0].topSkus, [{ sku: 'MUG', units: 3 }, { sku: 'TEE', units: 1 }]);
  assert.equal(closed[0].windowStart, '2025-03-01T12:00:00.000Z');
});

test('out-of-order events within lateness are counted; later ones go to the late side-output', () => {
  const w = new TumblingWindows({ sizeMs: 60_000, allowedLatenessMs: 30_000 });
  w.add(ev('S1', 70));
  assert.equal(w.add(ev('S1', 40)).late, null, 'event at 40s arrives while watermark is 40s: still on time');
  w.add(ev('S1', 130)); // closes [0,60)
  const r = w.add(ev('S1', 10));
  assert.ok(r.late && r.late.windowStart === T0);
});

test('duplicates (at-least-once redelivery) are ignored', () => {
  const w = new TumblingWindows();
  const e = ev('S1', 1);
  w.add(e);
  assert.equal(w.add({ ...e }).duplicate, true);
});

test('idle tick moves the watermark so quiet stores still publish', () => {
  const w = new TumblingWindows({ sizeMs: 60_000, allowedLatenessMs: 30_000 });
  w.add(ev('S1', 5));
  assert.equal(w.tick(T0 + 100_000, 120_000).length, 0);
  assert.equal(w.tick(T0 + 300_000, 120_000).length, 1);
});

test('state survives snapshot/restore', () => {
  const w = new TumblingWindows();
  w.add(ev('S1', 5, 700));
  const r = TumblingWindows.restore(w.snapshot());
  const closed = r.add(ev('S1', 200)).closed;
  assert.equal(closed[0].revenueCents, 700);
});

test('validation rejects malformed events', () => {
  assert.ok(validate('nope').error);
  assert.ok(validate(JSON.stringify({ eventId: 'x', storeId: 's', occurredAt: 'yesterday', totalCents: 1, lines: [] })).error);
  assert.ok(validate(JSON.stringify({ eventId: 'x', storeId: 's', occurredAt: '2025-03-01T00:00:00Z', totalCents: -5, lines: [] })).error);
  assert.ok(validate(JSON.stringify({ eventId: 'x', storeId: 's', occurredAt: '2025-03-01T00:00:00Z', totalCents: 5, lines: [{ sku: 'a', qty: 0 }] })).error);
  assert.ok(validate(JSON.stringify({ eventId: 'x', storeId: 's', occurredAt: '2025-03-01T00:00:00Z', totalCents: 5, lines: [{ sku: 'a', qty: 1 }] })).event);
});

/** In-memory transactional Kafka: committed topics/offsets only change on commit. */
function fakeKafka({ failCommitOnce = false } = {}) {
  const committed = { topics: {}, offsets: {} };
  let fail = failCommitOnce;
  return {
    committed,
    async transaction() {
      const pending = { topics: {}, offsets: null };
      return {
        async send(topic, msgs) { (pending.topics[topic] ??= []).push(...msgs); },
        async sendOffsets(o) { pending.offsets = o; },
        async commit() {
          if (fail) { fail = false; throw new Error('broker unavailable'); }
          for (const [t, m] of Object.entries(pending.topics)) (committed.topics[t] ??= []).push(...m);
          for (const t of pending.offsets.topics) for (const p of t.partitions) committed.offsets[`${t.topic}/${p.partition}`] = p.offset;
        },
        async abort() {},
      };
    },
  };
}

const msg = (offset, o) => ({ key: o.storeId, offset: String(offset), value: JSON.stringify({ ...o, occurredAt: new Date(T0 + o.secs * 1000).toISOString() }) });

test('pipeline: outputs, DLQ and offsets commit together', async () => {
  const kafka = fakeKafka();
  const p = new Pipeline({ kafka });
  await p.processBatch({ topic: 'orders.v1', partition: 0, messages: [
    msg(10, { eventId: 'a', storeId: 'S1', secs: 1, totalCents: 1200, lines: [{ sku: 'MUG', qty: 1 }] }),
    { key: 'S1', offset: '11', value: '{broken' },
    msg(12, { eventId: 'b', storeId: 'S1', secs: 200, totalCents: 800, lines: [{ sku: 'TEE', qty: 1 }] }),
  ] });
  const rev = kafka.committed.topics['store-revenue.v1'].map((m) => JSON.parse(m.value));
  assert.deepEqual(rev.map((r) => [r.storeId, r.revenueCents]), [['S1', 1200]]);
  assert.equal(kafka.committed.topics['orders.dlq.v1'][0].headers.source, 'orders.v1/0/11');
  assert.equal(kafka.committed.offsets['orders.v1/0'], '13');
  assert.ok(kafka.committed.topics['store-revenue.state.v1']);
});

test('pipeline: a failed commit publishes nothing and state rolls back, so the retry is exact', async () => {
  const kafka = fakeKafka({ failCommitOnce: true });
  const p = new Pipeline({ kafka });
  const batch = { topic: 'orders.v1', partition: 0, messages: [
    msg(1, { eventId: 'x1', storeId: 'S9', secs: 1, totalCents: 500, lines: [{ sku: 'A', qty: 1 }] }),
    msg(2, { eventId: 'x2', storeId: 'S9', secs: 300, totalCents: 100, lines: [{ sku: 'A', qty: 1 }] }),
  ] };
  await assert.rejects(p.processBatch(batch), /broker unavailable/);
  assert.equal(kafka.committed.topics['store-revenue.v1'], undefined);
  await p.processBatch(batch); // Kafka redelivers the same batch
  const rev = kafka.committed.topics['store-revenue.v1'].map((m) => JSON.parse(m.value));
  assert.deepEqual(rev.map((r) => [r.storeId, r.orders, r.revenueCents]), [['S9', 1, 500]], 'counted once, not zero (dedupe rolled back too)');
});
