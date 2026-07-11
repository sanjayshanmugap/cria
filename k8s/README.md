# Kubernetes Development

The Helm chart in `helm/cria` is the canonical deployment. Files in this
directory are minimal examples for inspecting individual resources; they are
not maintained as a second production deployment path.

## Local kind cluster

Prerequisites:

```bash
brew install kind kubectl helm
```

Docker Desktop must be running. Create the cluster, build architecture-native
images, load them into kind, and install the chart:

```bash
make kind-up
```

The script is idempotent and prints all deployed pods, jobs, and services.
The default kind install disables HPA because kind does not include
metrics-server. To test HPA after installing metrics-server:

```bash
CRIA_HPA_ENABLED=true make kind-up
```

## Verify inference

Forward the gRPC and BFF ports:

```bash
kubectl --context kind-cria -n inference-system \
  port-forward service/rust-control-plane 50051:50051 8080:8080
```

In another terminal:

```bash
python scripts/smoke_test.py
curl http://localhost:8080/api/models
```

Forward the in-cluster Prometheus instance:

```bash
kubectl --context kind-cria -n inference-system \
  port-forward service/cria-prometheus 9091:9090
```

Open <http://localhost:9091/targets> and confirm the control plane and active
mock worker services are present.

## Real-model worker

The default workflow builds only the small mock image. Build and load the
Transformers image separately:

```bash
make kind-load-llm
helm upgrade cria helm/cria \
  --namespace inference-system \
  --set models[1].replicas=1
```

TinyLLaMA needs substantially more memory and startup time than the mock
backend. The worker startup probe allows up to five minutes for model loading.

## Autoscaling

- HPA is CPU-based and requires metrics-server.
- KEDA is disabled by default and requires the KEDA operator.
- Enabling KEDA suppresses HPA resources so two controllers never fight over
  the same Deployment.
- Models configured with zero replicas retain scale-to-zero behavior under
  KEDA.

## Persistence

Kafka uses `emptyDir` by default for disposable local clusters. Enable a PVC:

```bash
helm upgrade cria helm/cria \
  --namespace inference-system \
  --set kafka.persistence.enabled=true \
  --set kafka.persistence.size=10Gi
```

## Teardown

```bash
make kind-down
```
