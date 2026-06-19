# Cria Architecture

Cria is a Kafka-native inference gateway. Clients connect to a Rust gRPC control plane, the control plane enqueues jobs into Kafka, and Python workers publish lifecycle and token events back to Kafka. The gateway consumes those durable token events and proxies them to the client stream.

```text
Client -> Rust gRPC Gateway -> Kafka inference_requests -> Python Workers
Client <- Rust gRPC Gateway <- Kafka inference_token_events <- Python Workers
```

## Components

| Component | Role |
| --- | --- |
| Rust control plane | Validates requests, enqueues jobs, streams token events, exposes health and metrics |
| Kafka | Durable job, token-event, and cancellation topics |
| Python workers | Consume jobs, run mock or Transformers inference, publish token events |
| Python client | Exercises Submit, GetStatus, and Cancel gRPC methods |

## Topics

| Topic | Producer | Consumer |
| --- | --- | --- |
| `inference_requests` | Control plane | Workers |
| `inference_token_events` | Workers | Control plane |
| `inference_control_events` | Control plane | Worker cancellation watchers |

Docker Compose creates these topics with the `kafka-init` service before the gateway starts. The gateway also waits for the topics during startup so Kubernetes and custom deployments do not depend only on Compose ordering.

## Failure Handling

| Failure | Behavior |
| --- | --- |
| Kafka starts slowly | Compose healthcheck and `kafka-init` hold app startup; gateway also polls metadata before subscribing |
| Worker pod dies before commit | Kafka keeps the uncommitted job for reassignment within the worker consumer group |
| Worker dies after emitting partial tokens | Partial token events remain durable in Kafka; the job may be retried and duplicate tokens are possible |
| Control plane restarts | In-flight gRPC streams are lost, but Kafka retains jobs and token events while retention allows |
| Duplicate inference | Allowed by at-least-once semantics; clients should de-duplicate by `request_id` and `sequence_number` |
| Client cancels | Gateway records cancellation and publishes a control event; workers stop when they observe it |

## Current Non-Goals

- Exactly-once inference semantics
- Service mesh integration
- Full OAuth or IAM
- Kafka TLS/SASL in the local development stack
