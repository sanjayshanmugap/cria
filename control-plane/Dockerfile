FROM rust:1.82-bookworm AS builder
WORKDIR /app
COPY proto ../proto
COPY control-plane ./control-plane
WORKDIR /app/control-plane
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/control-plane/target/release/control-plane /usr/local/bin/control-plane
EXPOSE 50051 9090
ENTRYPOINT ["/usr/local/bin/control-plane"]
