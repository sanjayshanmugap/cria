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

## OCI Always Free Provisioning

These are account-owner steps and cannot be automated from the repository.

1. Create an OCI account and choose the home region carefully. Always Free
   compute must be created in that region, and ARM capacity may be unavailable.
2. Add an SSH public key and create an Always Free `VM.Standard.A1.Flex`
   instance with 2 OCPUs, 12 GB RAM, Ubuntu 24.04 ARM64, and a 50 GB boot volume.
3. Reserve the public IPv4 address when available.
4. In the subnet security list or NSG, allow TCP 22 only from your IP. Allow
   TCP 80 and 443 from the internet after DNS and Caddy are configured. Never
   allow 9092, 50051, 8080, 9090, 9091, or 3001.
5. Create an OCI budget with a `$1` alert and avoid resources not marked
   Always Free.
6. Point a DNS A record at the reserved IP. A free DuckDNS hostname is
   sufficient for Caddy's ACME certificate.

Oracle documents reclamation of Always Free instances that remain below its
CPU, network, and memory activity thresholds for seven days. Accept and
document this risk; do not generate artificial load to evade reclamation.

## OCI Host Preparation

SSH to the instance and confirm ARM64:

```bash
uname -m  # aarch64
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-v2 git
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
```

Log out and reconnect for the Docker group change. Configure the Ubuntu host
firewall in addition to OCI's NSG:

```bash
sudo ufw allow from YOUR_PUBLIC_IP to any port 22 proto tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw --force enable
```

Make the GHCR packages public before deploying so the VM does not need a
long-lived registry token.

## Verify Release Signatures

Install Cosign on an administrative machine and verify each digest or tag:

```bash
cosign verify ghcr.io/GHCR_OWNER/cria-control-plane:RELEASE_TAG \
  --certificate-identity-regexp \
  "https://github.com/GHCR_OWNER/cria/.github/workflows/publish-images.yaml@refs/tags/RELEASE_TAG" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

Repeat for `cria-worker-mock` and `cria-web`. The release workflow also
publishes BuildKit provenance and SBOM attestations.

## Deploy to OCI

```bash
git clone https://github.com/GHCR_OWNER/cria.git
cd cria
git checkout RELEASE_TAG
cp .env.prod.example .env.prod
```

Edit `.env.prod` with the GHCR owner, the exact release tag, DNS hostname, and
ACME email. Then:

```bash
docker compose --env-file .env.prod -f docker-compose.prod.yaml pull
docker compose --env-file .env.prod -f docker-compose.prod.yaml up -d
docker compose --env-file .env.prod -f docker-compose.prod.yaml ps
docker compose --env-file .env.prod -f docker-compose.prod.yaml \
  exec control-plane curl -fsS http://localhost:9090/healthz
curl -fsS "https://${CRIA_HOST}/api/models"
```

Complete a streamed request in an external browser. Reboot once and verify
automatic recovery:

```bash
sudo reboot
# reconnect
docker compose --env-file .env.prod -f docker-compose.prod.yaml ps
```

Only Caddy joins the public network. The web container bridges Caddy to the
private backend, and no Kafka, gRPC, Prometheus, or worker port binds to the
host.

## Backup and Restore

Create volume archives before every upgrade:

```bash
mkdir -p backups
docker run --rm \
  -v cria-prod_kafka-data:/data:ro \
  -v "$PWD/backups":/backup \
  alpine tar czf /backup/kafka-$(date +%F-%H%M).tgz -C /data .
docker run --rm \
  -v cria-prod_prometheus-data:/data:ro \
  -v "$PWD/backups":/backup \
  alpine tar czf /backup/prometheus-$(date +%F-%H%M).tgz -C /data .
```

For a restore, stop the stack, archive the current volume, clear the selected
volume, and extract the chosen backup. Restoring Kafka is destructive and must
be done while every Cria container is stopped:

```bash
docker compose --env-file .env.prod -f docker-compose.prod.yaml down
docker run --rm \
  -v cria-prod_kafka-data:/data \
  -v "$PWD/backups":/backup \
  alpine sh -ec 'rm -rf /data/* && tar xzf /backup/KAFKA_BACKUP.tgz -C /data'
docker compose --env-file .env.prod -f docker-compose.prod.yaml up -d
```

## Upgrade and Rollback

Upgrade only to a published release:

```bash
git fetch --tags
git checkout NEW_RELEASE_TAG
# Update CRIA_IMAGE_TAG in .env.prod to NEW_RELEASE_TAG.
docker compose --env-file .env.prod -f docker-compose.prod.yaml pull
docker compose --env-file .env.prod -f docker-compose.prod.yaml up -d
docker compose --env-file .env.prod -f docker-compose.prod.yaml ps
```

Rollback uses the same process with the previous Git and image tag:

```bash
git checkout PREVIOUS_RELEASE_TAG
# Restore CRIA_IMAGE_TAG=PREVIOUS_RELEASE_TAG in .env.prod.
docker compose --env-file .env.prod -f docker-compose.prod.yaml pull
docker compose --env-file .env.prod -f docker-compose.prod.yaml up -d
```

Kafka and Prometheus volumes are preserved across upgrades and rollbacks.

## Stop or Permanently Remove

Stop services while retaining data:

```bash
docker compose --env-file .env.prod -f docker-compose.prod.yaml down
```

Permanently delete containers and all persisted Cria data:

```bash
docker compose --env-file .env.prod -f docker-compose.prod.yaml down -v
```

Delete the OCI VM, reserved IP, and DNS record in the OCI/DuckDNS consoles when
the public demo is no longer needed. Confirm the OCI billing dashboard shows no
remaining non-free resources.
