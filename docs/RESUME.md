# Resume Bullets

Use the bullets that match the target role. The quantified values come from
the reproducible mock transport baseline in `docs/RESULTS.md`; keep the word
"mock" when citing them so they are not mistaken for model-compute benchmarks.

## Distributed Systems / Backend

- Built a Kafka-native distributed LLM inference platform with a Rust gRPC/SSE
  control plane and Python workers, delivering durable token streaming,
  status, cancellation, and topic-per-model routing at **53.57 mock tokens/s
  with 379.1 ms p95 TTFT across 2 concurrent requests**.
- Designed at-least-once inference and token-event recovery semantics with
  request/sequence de-duplication, model-isolated consumer groups, admission
  control, mTLS, and failure tests spanning worker cancellation and Kafka
  routing.

## AI / ML Engineering

- Implemented pluggable Python inference workers for deterministic mock and
  Hugging Face Transformers backends, including sampling controls, streamed
  generation, cancellation-aware stopping criteria, per-model queues, and
  reproducible TinyLLaMA benchmark hooks.
- Separated model-compute measurements from distributed transport benchmarks
  and shipped versioned JSON evidence with p50/p95/p99 TTFT and completion
  latency, throughput, failure counts, topology, hardware, timestamp, and git
  SHA.

## MLOps / Platform

- Productionized a Rust/Python/React inference stack with multi-architecture
  amd64/arm64 GHCR images, keyless Cosign signatures, SBOM/provenance
  attestations, Docker Compose, Helm, kind, Prometheus/Grafana, and KEDA-ready
  Kafka-lag autoscaling.
- Built PR and nightly CI that compiles Rust/Python/React, exercises the full
  Kafka completion/status/cancel path across two model routes, and retains a
  **1/2-worker × 1/2/4/8-concurrency** performance matrix as machine-readable
  artifacts.

## Full-Stack / Software Engineering

- Shipped a React operations console behind an nginx/Caddy same-origin proxy
  with SSE token streaming, model selection, status and cancellation controls,
  automatic HTTPS, and a one-command local stack with provisioned
  Prometheus/Grafana dashboards.
- Created a reproducible local-to-cloud delivery path from Docker Compose to a
  verified kind/Helm deployment and an OCI Always Free ARM runbook with health
  ordering, resource limits, backups, rollback, private service networks, and
  cost guardrails.
