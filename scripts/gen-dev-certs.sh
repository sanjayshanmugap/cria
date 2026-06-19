#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-certs}"
mkdir -p "${OUT_DIR}"

openssl genrsa -out "${OUT_DIR}/ca.key" 4096
openssl req -x509 -new -nodes \
  -key "${OUT_DIR}/ca.key" \
  -sha256 \
  -days 3650 \
  -subj "/CN=cria-dev-ca" \
  -out "${OUT_DIR}/ca.crt"

openssl genrsa -out "${OUT_DIR}/server.key" 2048
openssl req -new \
  -key "${OUT_DIR}/server.key" \
  -subj "/CN=localhost" \
  -out "${OUT_DIR}/server.csr"
cat > "${OUT_DIR}/server.ext" <<'EOF'
subjectAltName = DNS:localhost,DNS:rust-control-plane,IP:127.0.0.1
extendedKeyUsage = serverAuth
EOF
openssl x509 -req \
  -in "${OUT_DIR}/server.csr" \
  -CA "${OUT_DIR}/ca.crt" \
  -CAkey "${OUT_DIR}/ca.key" \
  -CAcreateserial \
  -out "${OUT_DIR}/server.crt" \
  -days 825 \
  -sha256 \
  -extfile "${OUT_DIR}/server.ext"

openssl genrsa -out "${OUT_DIR}/client.key" 2048
openssl req -new \
  -key "${OUT_DIR}/client.key" \
  -subj "/CN=cria-dev-client" \
  -out "${OUT_DIR}/client.csr"
cat > "${OUT_DIR}/client.ext" <<'EOF'
extendedKeyUsage = clientAuth
EOF
openssl x509 -req \
  -in "${OUT_DIR}/client.csr" \
  -CA "${OUT_DIR}/ca.crt" \
  -CAkey "${OUT_DIR}/ca.key" \
  -CAcreateserial \
  -out "${OUT_DIR}/client.crt" \
  -days 825 \
  -sha256 \
  -extfile "${OUT_DIR}/client.ext"

echo "Wrote development certificates to ${OUT_DIR}"
