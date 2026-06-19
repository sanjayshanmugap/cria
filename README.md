# Distributed LLM Inference Engine

A Kafka-native distributed LLM inference engine with durable token streaming, cancellation, observability, and Kubernetes-native scaling.

## Architecture

```text
Client -> Rust gRPC Gateway -> Kafka inference_requests -> Python Workers
Client <- Rust gRPC Gateway <- Kafka inference_token_events <- Python Workers
```

The gateway writes inference jobs to Kafka. Workers consume jobs, run a mock or Transformers-backed model, and publish every lifecycle event and generated token to a durable Kafka token-event topic. The gateway consumes token events and streams them back to the gRPC client.

See [docs/architecture.md](docs/architecture.md) for topic ownership, failure behavior, and current non-goals.

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
python client.py submit --prompt "Explain durable Kafka token streaming" --max-tokens 32
```

The default worker backend is `mock`, so local development does not require downloading TinyLLaMA. To use TinyLLaMA, run workers with:

```bash
BACKEND=transformers MODEL_NAME=TinyLlama/TinyLlama-1.1B-Chat-v1.0 python -m inference_worker.main
```

Or start the optional Compose profile:

```bash
docker compose --profile llm up --build
```

Run a smoke test against a running compose stack:

```bash
python scripts/smoke_test.py
```

## Kubernetes

```bash
make build-images
# For kind clusters:
make kind-load

kubectl apply -f k8s/namespace.yaml
kubectl apply -f infra/kafka/kafka-deployment.yaml
kubectl -n inference-system wait --for=condition=available deployment/kafka --timeout=120s
kubectl apply -f k8s/control-plane.yaml
kubectl apply -f k8s/worker.yaml
kubectl apply -f k8s/hpa.yaml
kubectl apply -f infra/keda/keda-scaledobject.yaml
kubectl -n inference-system port-forward svc/rust-control-plane 50051:50051
```

The checked-in K8s manifests use local development images (`cria/control-plane:dev` and `cria/worker:dev`) with `imagePullPolicy: Never`. For a remote cluster, push images to your registry and update the image fields before applying the manifests.

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
| `MODEL_ROUTES` | `mock=inference_requests.mock,tinyllama-1.1b-chat=inference_requests.tinyllama-1.1b-chat` | Comma-separated `model_id=topic` routes |
| `DEFAULT_MODEL_ID` | `mock` | Model used when clients omit `model_id` |
| `MAX_ACTIVE_REQUESTS` | `128` | Admission control limit |
| `MAX_PROMPT_CHARS` | `12000` | Prompt length limit |
| `GRPC_TLS_CERT` / `GRPC_TLS_KEY` | unset | Enable server TLS when both are set |
| `GRPC_TLS_CLIENT_CA` | unset | Client CA for mTLS |
| `GRPC_TLS_REQUIRE_CLIENT_AUTH` | `false` | Require client certificates |

### Python Worker

| Variable | Default | Description |
| --- | --- | --- |
| `BACKEND` | `mock` | `mock` or `transformers` |
| `MODEL_NAME` | `TinyLlama/TinyLlama-1.1B-Chat-v1.0` | Hugging Face model |
| `DEVICE` | `auto` | `auto`, `cpu`, or `cuda` |
| `DTYPE` | `float16` | `float16`, `bfloat16`, or `float32` |
| `MODEL_ID` | `mock` | Model this worker serves |
| `KAFKA_GROUP_ID` | `llm-inference-workers-<MODEL_ID>` | Worker consumer group |
| `KAFKA_REQUEST_TOPIC` | `inference_requests.<MODEL_ID>` | Model-specific job topic |
| `METRICS_PORT` | `9100` | Worker Prometheus metrics port |
| `TEMPERATURE` | `0.7` | Default sampling temperature |
| `TOP_P` | `0.9` | Default nucleus sampling |
| `TOP_K` | `50` | Default top-k sampling |

## Observability

The Rust gateway exposes `/metrics` and `/healthz` on `METRICS_ADDR`. Workers expose Prometheus metrics on `METRICS_PORT`, including active jobs, processed jobs, failures, cancellations, token count, and job duration.

Docker Compose also starts Prometheus on <http://localhost:9091> using `infra/observability/prometheus.yaml`.

## Web Console

The Rust control plane exposes a lightweight BFF on `BFF_ADDR` (default `0.0.0.0:8080`) with:

- `GET /api/models`
- `POST /api/infer` as server-sent events
- `GET /api/infer/:request_id/status`
- `POST /api/infer/:request_id/cancel`

Run the React console locally:

```bash
cd web
npm install
VITE_BFF_PROXY_TARGET=http://localhost:8080 npm run dev
```

## API

```protobuf
service InferenceGateway {
  rpc Submit(InferenceRequest) returns (stream TokenEvent);
  rpc Cancel(CancelRequest) returns (CancelResponse);
  rpc GetStatus(StatusRequest) returns (StatusResponse);
}
```

The Python client supports all three RPCs:

```bash
cd workers/python
python client.py submit --prompt "Hello from Cria" --max-tokens 8
python client.py submit --model-id mock --prompt "Hello from Cria" --max-tokens 8
python client.py status --request-id <request-id>
python client.py cancel --request-id <request-id> --reason "user abort"
```

The default local model routes are `mock -> inference_requests.mock` and `tinyllama-1.1b-chat -> inference_requests.tinyllama-1.1b-chat`.

## mTLS

Generate development certificates:

```bash
scripts/gen-dev-certs.sh
```

Start the gateway with server TLS and client certificate verification by setting:

```bash
GRPC_TLS_CERT=certs/server.crt
GRPC_TLS_KEY=certs/server.key
GRPC_TLS_CLIENT_CA=certs/ca.crt
GRPC_TLS_REQUIRE_CLIENT_AUTH=true
```

Then call it with:

```bash
cd workers/python
python client.py --tls-ca ../../certs/ca.crt \
  --tls-cert ../../certs/client.crt \
  --tls-key ../../certs/client.key \
  --tls-server-name localhost \
  submit --prompt "Hello over mTLS" --max-tokens 8
```

## Reliability Semantics

- Request processing is at least once.
- Token event delivery is at least once.
- Clients should de-duplicate by `request_id` and `sequence_number`.
- Worker crashes can cause duplicate inference.
- Token events can be replayed while Kafka retention keeps them.
- `stream_options.replay_from_beginning` replays events cached by the active gateway for an existing `request_id`; cross-gateway Kafka replay is planned for a future hardening pass.

## Development

```bash
cd control-plane
cargo test
cargo check

cd ../workers/python
python generate_grpc.py
python -m pytest tests
python -m compileall inference_worker client.py load_test.py
```

## Troubleshooting

If a client prints the prompt but no tokens, confirm the compose stack was started after the `kafka-init` service was added:

```bash
docker compose down
docker compose up --build
```

The gateway waits for model-specific request topics, `inference_token_events`, and `inference_control_events` before serving requests. If it still hangs, check control-plane and worker logs for Kafka connectivity errors.
