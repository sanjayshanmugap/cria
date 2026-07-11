FROM rust:1.85-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake pkg-config libssl-dev libcurl4-openssl-dev libsasl2-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY proto ./proto
COPY control-plane ./control-plane
WORKDIR /app/control-plane
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/control-plane/target/release/control-plane /usr/local/bin/control-plane
EXPOSE 50051 8080 9090
HEALTHCHECK --interval=10s --timeout=3s --retries=6 \
    CMD curl --fail --silent http://127.0.0.1:9090/healthz >/dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/control-plane"]
