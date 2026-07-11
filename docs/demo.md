# Local and Public Demo

## Prerequisites

- Docker Desktop with 6–8 GB of memory available
- Python 3.11+ for the CLI and smoke test
- `cloudflared` only when sharing the demo publicly

The default stack uses the deterministic mock backend. It exercises the same
Kafka, gRPC, SSE, cancellation, and metrics paths as a model worker without
requiring a model download.

## Start the complete stack

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps
python scripts/smoke_test.py
```

Open:

- Web console: <http://localhost:3000>
- Prometheus: <http://localhost:9091>
- Grafana: <http://localhost:3001> (anonymous, read-only)
- Gateway health: <http://localhost:9090/healthz>

Submit a prompt in the web console and confirm that tokens stream incrementally.
The request panel exposes the generated request ID, status lookup, and
cancellation controls.

To run two mock workers explicitly:

```bash
docker compose up --build -d --scale worker=2
```

To follow the data path while testing:

```bash
docker compose logs -f control-plane worker
```

## Share a temporary public URL

Install Cloudflare Tunnel on macOS:

```bash
brew install cloudflared
```

Expose only the same-origin web proxy:

```bash
cloudflared tunnel --url http://localhost:3000
```

Open the generated `https://*.trycloudflare.com` URL from a second device and
complete one streamed request. Quick Tunnels are temporary development links
with no uptime guarantee. Do not expose Kafka (`9092`), gRPC (`50051`), gateway
metrics (`9090`), Prometheus (`9091`), or Grafana (`3001`) through the tunnel.

## TinyLLaMA

The optional profile downloads and runs TinyLLaMA:

```bash
docker compose --profile llm up --build -d
cd workers/python
python client.py submit \
  --model-id tinyllama-1.1b-chat \
  --prompt "Explain durable token streaming" \
  --max-tokens 32
```

Model inference is intentionally not enabled in the public free-tier demo:
the mock backend keeps the demo responsive while preserving the distributed
systems behavior under evaluation.

## Stop or reset

Stop containers while retaining Kafka, Prometheus, and Grafana volumes:

```bash
docker compose down
```

Delete all local persisted demo data:

```bash
docker compose down -v
```
