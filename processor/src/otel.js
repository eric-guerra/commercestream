'use strict';
// Loaded with --require before the app: traces + metrics to the OTel Collector (OTLP), kafkajs auto-instrumented.
const { NodeSDK } = require('@opentelemetry/sdk-node');
const { getNodeAutoInstrumentations } = require('@opentelemetry/auto-instrumentations-node');

const sdk = new NodeSDK({
  serviceName: process.env.OTEL_SERVICE_NAME ?? 'store-revenue',
  instrumentations: [getNodeAutoInstrumentations({ '@opentelemetry/instrumentation-fs': { enabled: false } })],
});
sdk.start();
process.on('SIGTERM', () => sdk.shutdown().catch(() => undefined));
