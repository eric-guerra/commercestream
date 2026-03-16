'use strict';
// kafkajs wiring: transactional producer + read_committed consumer, OpenTelemetry spans per batch.
const { Kafka, CompressionTypes } = require('kafkajs');
const { trace, context, propagation } = require('@opentelemetry/api');
const { Pipeline } = require('./pipeline');

async function main() {
  const kafka = new Kafka({ clientId: 'store-revenue', brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',') });
  const producer = kafka.producer({ transactionalId: `store-revenue-${process.env.HOSTNAME ?? 'local'}`, idempotent: true, maxInFlightRequests: 1 });
  const consumer = kafka.consumer({ groupId: 'store-revenue', readUncommitted: false });
  await Promise.all([producer.connect(), consumer.connect()]);
  await consumer.subscribe({ topic: 'orders.v1' });

  const adapter = {
    async transaction() {
      const tx = await producer.transaction();
      return {
        send: (topic, messages) => tx.send({ topic, messages, compression: CompressionTypes.ZSTD }),
        sendOffsets: (o) => tx.sendOffsets({ consumerGroupId: o.consumerGroupId, topics: o.topics }),
        commit: () => tx.commit(),
        abort: () => tx.abort(),
      };
    },
  };
  const pipeline = new Pipeline({ kafka: adapter });
  const tracer = trace.getTracer('store-revenue');

  await consumer.run({
    autoCommit: false, // offsets are committed inside the producer transaction
    eachBatch: async ({ batch, heartbeat }) => {
      const parent = propagation.extract(context.active(), batch.messages[0]?.headers ?? {});
      await tracer.startActiveSpan('process orders batch', { attributes: { 'messaging.kafka.partition': batch.partition, 'messaging.batch.message_count': batch.messages.length } }, parent, async (span) => {
        try {
          await pipeline.processBatch({ topic: batch.topic, partition: batch.partition,
            messages: batch.messages.map((m) => ({ key: m.key?.toString(), value: m.value.toString(), offset: m.offset })) });
        } finally {
          span.end();
        }
      });
      await heartbeat();
    },
  });

  const shutdown = async () => { await consumer.disconnect(); await producer.disconnect(); process.exit(0); };
  process.on('SIGTERM', shutdown);
}

if (require.main === module) main();
