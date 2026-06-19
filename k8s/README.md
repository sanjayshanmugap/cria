# Kubernetes Manifests

The files in this directory are lightweight local-development manifests. For configurable deployments, use the Helm chart in `helm/cria`.

```bash
helm install cria helm/cria --namespace inference-system --create-namespace
```

The chart supports multiple models, topic initialization, HPA/KEDA templates, and optional gRPC mTLS secret mounts.
