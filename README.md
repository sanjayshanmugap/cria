# Distributed LLM Inference Engine

A Kafka-native distributed LLM inference engine with durable token streaming, cancellation, observability, and Kubernetes-native scaling.

## Architecture

```text
Client -> Rust gRPC Gateway -> Kafka inference_requests -> Python Workers
Client <- Rust gRPC Gateway <- Kafka inference_token_events <- Python Workers
```

The gateway writes inference jobs to Kafka. Workers consume jobs, run a mock or Transformers-backed model, and publish every lifecycle event and generated token to a durable Kafka token-event topic. The gateway consumes token events and streams them back to the gRPC client.

## What Differentiates This

- Durable token events are stored in Kafka instead of sent only over transient worker callbacks.
- Requests support cancellation and status lookup.
- The worker has a backend interface with mock and Hugging Face Transformers implementations.
- The gateway exposes Prometheus metrics and structured logs.
- Kubernetes manifests include CPU HPA plus optional KEDA scaling by Kafka lag.

## Quick Start

```bash
docker compose up --build
```

In another terminal:

```bash
cd workers/python
python client.py --prompt "Explain durable Kafka token streaming" --max-tokens 32
```

The default worker backend is `mock`, so local development does not require downloading TinyLLaMA. To use TinyLLaMA, run workers with:

```bash
BACKEND=transformers MODEL_NAME=TinyLlama/TinyLlama-1.1B-Chat-v1.0 python -m inference_worker.main
```

## Kubernetes

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f infra/kafka/kafka-deployment.yaml
kubectl -n inference-system wait --for=condition=available deployment/kafka --timeout=120s
kubectl apply -f k8s/control-plane.yaml
kubectl apply -f k8s/worker.yaml
kubectl apply -f k8s/hpa.yaml
kubectl apply -f infra/keda/keda-scaledobject.yaml
kubectl -n inference-system port-forward svc/rust-control-plane 50051:50051
```

## Configuration

### Rust Gateway

| Variable | Default | Description |
| --- | --- | --- |
| `GRPC_ADDR` | `0.0.0.0:50051` | gRPC listen address |
| `METRICS_ADDR` | `0.0.0.0:9090` | Prometheus and health endpoint |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap brokers |
| `KAFKA_REQUEST_TOPIC` | `inference_requests` | Request topic |
| `KAFKA_TOKEN_TOPIC` | `inference_token_events` | Token event topic |
| `KAFKA_CONTROL_TOPIC` | `inference_control_events` | Cancellation/control topic |
| `KAFKA_GATEWAY_GROUP_ID` | unique per process | Token-event consumer group; keep unique so each gateway can observe events for its streams |
| `MAX_ACTIVE_REQUESTS` | `128` | Admission control limit |
| `MAX_PROMPT_CHARS` | `12000` | Prompt length limit |

### Python Worker

| Variable | Default | Description |
| --- | --- | --- |
| `BACKEND` | `mock` | `mock` or `transformers` |
| `MODEL_NAME` | `TinyLlama/TinyLlama-1.1B-Chat-v1.0` | Hugging Face model |
| `DEVICE` | `auto` | `auto`, `cpu`, or `cuda` |
| `DTYPE` | `float16` | `float16`, `bfloat16`, or `float32` |
| `KAFKA_GROUP_ID` | `llm-inference-workers` | Worker consumer group |
| `METRICS_PORT` | `9100` | Worker Prometheus metrics port |
| `TEMPERATURE` | `0.7` | Default sampling temperature |
| `TOP_P` | `0.9` | Default nucleus sampling |
| `TOP_K` | `50` | Default top-k sampling |

## Observability

The Rust gateway exposes `/metrics` and `/healthz` on `METRICS_ADDR`. Workers expose Prometheus metrics on `METRICS_PORT`, including active jobs, processed jobs, failures, cancellations, token count, and job duration.

## API

```protobuf
service InferenceGateway {
  rpc Submit(InferenceRequest) returns (stream TokenEvent);
  rpc Cancel(CancelRequest) returns (CancelResponse);
  rpc GetStatus(StatusRequest) returns (StatusResponse);
}
```

## Reliability Semantics

- Request processing is at least once.
- Token event delivery is at least once.
- Clients should de-duplicate by `request_id` and `sequence_number`.
- Worker crashes can cause duplicate inference.
- Token events can be replayed while Kafka retention keeps them.

## Development

```bash
cd control-plane
cargo test
cargo check

cd ../workers/python
python generate_grpc.py
python -m compileall inference_worker client.py load_test.py
```
