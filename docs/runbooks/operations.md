# Cria Operations Runbook

## Local Health

```bash
curl http://localhost:9090/healthz
curl http://localhost:9090/metrics
curl http://localhost:9091
```

Run an end-to-end smoke test:

```bash
python scripts/smoke_test.py
```

## Kafka Lag

```bash
kubectl -n inference-system exec -it deployment/kafka -- \
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group llm-inference-workers-mock --describe
```

For model-specific workers, replace the group with `llm-inference-workers-<model_id>` and the topic with that model's request topic.

## Scaling Workers

Manual scale:

```bash
kubectl -n inference-system scale deployment llm-worker-mock --replicas=5
```

HPA is CPU-based by default. KEDA can be enabled from Helm values to scale on Kafka lag per model topic.

## Adding A Model

1. Add a `models[]` entry in `helm/cria/values.yaml`.
2. Pick a stable `id`, for example `mistral-7b-instruct`.
3. Set a unique topic, for example `inference_requests.mistral-7b-instruct`.
4. Set worker image, backend, model name, replicas, and resources.
5. Upgrade the chart:

```bash
helm upgrade cria helm/cria --namespace inference-system
```

The chart updates `MODEL_ROUTES`, creates the request topic, and deploys a model-specific worker deployment.

## mTLS Certificate Rotation

1. Generate or obtain a new CA/server certificate set.
2. Update the `cria-grpc-mtls` Secret:

```bash
kubectl -n inference-system create secret generic cria-grpc-mtls \
  --from-file=ca.crt=certs/ca.crt \
  --from-file=server.crt=certs/server.crt \
  --from-file=server.key=certs/server.key \
  --dry-run=client -o yaml | kubectl apply -f -
```

3. Restart the control plane:

```bash
kubectl -n inference-system rollout restart deployment/rust-control-plane
```

4. Distribute the matching client certificate bundle to CLI users.

## Troubleshooting Hung Streams

If a client prints its prompt but never receives tokens:

1. Check that model request topics exist.
2. Check the worker is subscribed to the same model topic as the gateway route.
3. Check `inference_token_events` for token events.
4. Restart the gateway if it was deployed before topics existed on an old version.
