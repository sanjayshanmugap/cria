#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_NAME="${KIND_CLUSTER:-cria}"
NAMESPACE="${CRIA_NAMESPACE:-inference-system}"
RELEASE="${CRIA_RELEASE:-cria}"

for command in docker kind kubectl helm make; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "required command not found: ${command}" >&2
    exit 1
  fi
done

if [[ "${1:-}" == "--delete" ]]; then
  kind delete cluster --name "${CLUSTER_NAME}"
  exit 0
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker is not running" >&2
  exit 1
fi

cluster_exists=false
while IFS= read -r cluster; do
  if [[ "${cluster}" == "${CLUSTER_NAME}" ]]; then
    cluster_exists=true
    break
  fi
done < <(kind get clusters)

if [[ "${cluster_exists}" != "true" ]]; then
  kind create cluster \
    --name "${CLUSTER_NAME}" \
    --config "${ROOT_DIR}/deploy/kind-config.yaml" \
    --wait 180s
fi

make -C "${ROOT_DIR}" kind-load KIND_CLUSTER="${CLUSTER_NAME}"

helm upgrade --install "${RELEASE}" "${ROOT_DIR}/helm/cria" \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  --wait \
  --timeout 5m

kubectl --context "kind-${CLUSTER_NAME}" \
  --namespace "${NAMESPACE}" \
  wait --for=condition=available deployment --all --timeout=180s

kubectl --context "kind-${CLUSTER_NAME}" \
  --namespace "${NAMESPACE}" \
  get pods,jobs,services

cat <<EOF

Cria is ready in kind-${CLUSTER_NAME}.

Gateway:
  kubectl --context kind-${CLUSTER_NAME} -n ${NAMESPACE} \
    port-forward service/rust-control-plane 50051:50051 8080:8080

Prometheus:
  kubectl --context kind-${CLUSTER_NAME} -n ${NAMESPACE} \
    port-forward service/cria-prometheus 9091:9090

Delete:
  ${ROOT_DIR}/scripts/kind-bootstrap.sh --delete
EOF
