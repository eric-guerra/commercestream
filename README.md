# CommerceStream

Portfolio reference implementation of a 2026 project, the platform capstone: a real-time commerce event backbone on Kubernetes. Order events from web, app and in-store POS flow through Kafka into an exactly-once stream processor that publishes per-store revenue every minute. It's deployed by GitOps and observable end to end.

**Stack:** Kafka 3.7 (Strimzi, KRaft), Node.js stream processor (kafkajs transactions), Kubernetes (EKS), KEDA, Kustomize, Argo CD ApplicationSets, Terraform, OpenTelemetry (Collector → Tempo/Mimir).

## Layout
```
processor/        store-revenue stream processor + tests
deploy/platform/  Strimzi Kafka cluster (3 brokers, zone-aware rack, KRaft)
deploy/base/      processor Deployment, PDB, NetworkPolicy, KEDA ScaledObject, KafkaTopics
deploy/overlays/  staging / prod (Kustomize)
deploy/argocd/    ApplicationSet: one Argo CD app per environment
infra/            Terraform: VPC, EKS (dedicated Kafka node group), Strimzi/KEDA/Argo CD/OTel via Helm
policy/           static checks over the manifests
```

## Stream processor (`processor/`)
- **Event-time tumbling windows** (1 min) per store, with a **watermark**: max event time − 30 s of allowed lateness.
  - Out-of-order events within the allowed lateness count normally.
  - Events for windows that have already closed go to `orders.late.v1` as corrections, instead of silently changing published numbers.
  - An idle-partition tick keeps quiet stores publishing.
- **Exactly-once (read-process-write).** One Kafka transaction holds the batch's revenue windows, late and DLQ records, the window-state checkpoint (compacted `store-revenue.state.v1`), and the consumed offsets.
  - If the commit fails, nothing is visible to `read_committed` consumers and in-memory state rolls back, so the redelivered batch is counted exactly once. A test covers this.
- **Dedupe** by event id absorbs at-least-once producers.
- **Bad events** go to `orders.dlq.v1` with the source partition and offset in headers.
- **OpenTelemetry:** an SDK bootstrap (`--require ./src/otel.js`) with kafkajs auto-instrumentation and a span per batch.

## Platform choices
- **Durable topics:** replication factor 3, `min.insync.replicas` 2, auto-create off. 12 partitions keyed by store, so each store's events stay ordered.
- **Autoscale on consumer lag (KEDA), not CPU:** lag is what users see as stale dashboards. `maxReplicaCount` equals the partition count, since extra consumers would sit idle; a policy test enforces this.
- **Safe rollouts:** `maxUnavailable: 0`, a PDB, zone spread, and a 45 s termination grace so the in-flight transaction finishes.
- **Locked down:** non-root, read-only root filesystem, all capabilities dropped, and a default-deny NetworkPolicy that allows only Kafka, the OTel collector and DNS.
- **GitOps:**
  - Staging tracks `main`; prod tracks `release`.
  - Sync is automated with prune and self-heal, and Argo CD is told to ignore `spec.replicas` because KEDA owns it.
  - Retries use backoff.
- **Infrastructure:** EKS with a private endpoint, encrypted secrets and a tainted Kafka node group. Add-ons are installed by Terraform; applications are delivered by Argo CD.

## Test
```bash
cd processor && npm test     # 8 tests: windows, lateness, dedupe, idle tick, snapshot/restore, validation, transactional commit and rollback
pytest -q policy             # 7 checks: hardening, PDB/NetworkPolicy coverage, KEDA ≤ partitions, topic durability, Argo/KEDA interplay, overlay patch targets
```
