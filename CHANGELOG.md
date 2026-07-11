# Changelog

All notable changes are documented here. Cria follows semantic versioning.

## [0.2.0] - 2026-07-11

First portfolio release.

### Added

- Rust gRPC gateway and browser BFF with streaming inference, request status,
  cancellation, admission limits, replay support, structured logs, and mTLS.
- Kafka KRaft data plane with durable lifecycle/token events, per-model request
  topics, cancellation events, and at-least-once worker semantics.
- Python mock and Hugging Face Transformers backends with model-specific
  workers and cancellation-aware generation.
- React web console behind a production nginx same-origin SSE proxy.
- One-command Compose demo with health ordering, Prometheus, provisioned
  Grafana, slim mock images, persistent development volumes, and Cloudflare
  Quick Tunnel guidance.
- End-to-end status/cancel/two-model integration tests plus versioned JSON
  benchmarks with p50/p95/p99 latency, throughput, failures, topology, host
  metadata, timestamp, and git SHA.
- Canonical Helm chart with Kafka/topic readiness, probes, optional
  persistence, model metrics Services, in-chart Prometheus, HPA/KEDA ownership,
  and scale-to-zero semantics.
- Reproducible kind bootstrap verified through the gRPC smoke suite.
- Signed multi-architecture GHCR release workflow with SBOM and provenance.
- OCI Always Free ARM production Compose deployment with Caddy HTTPS, private
  service networks, resource limits, backup, upgrade, rollback, and cost
  guardrails.

### Evidence

- 72/72 successful requests in the checked-in Apple M4 mock transport matrix.
- 53.57 mock tokens/s and 379.1 ms p95 TTFT at two workers and concurrency two.
- 1.80x throughput improvement over one worker at matched concurrency.
- 10 Rust unit tests, 8 Python tests, reproducible React build, Compose
  completion/status/cancel/model-routing smoke test, and clean kind deployment.

### Known limitations

- The public free-tier mode uses deterministic mock inference. No TinyLLaMA
  performance number is claimed until a stable device/dtype baseline is run.
- Processing is at least once; clients must de-duplicate request/sequence IDs.
- OCI account, DNS, firewall, and budget provisioning remain owner-controlled
  console steps.
- A published release should be created only after the four stacked portfolio
  PRs merge into `main`; publishing triggers the signed GHCR workflow.

[0.2.0]: https://github.com/sanjayshanmugap/cria/releases/tag/v0.2.0
