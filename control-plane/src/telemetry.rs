use anyhow::Result;
use axum::{routing::get, Router};
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, TextEncoder};
use std::net::SocketAddr;
use tracing::info;

#[derive(Clone)]
pub struct Metrics {
    pub requests_total: IntCounter,
    pub completed_requests: IntCounter,
    pub failed_requests: IntCounter,
    pub cancelled_requests: IntCounter,
    pub rejected_requests: IntCounter,
    pub tokens_total: IntCounter,
    pub active_streams: IntGauge,
    pub enqueue_latency_seconds: Histogram,
}

impl Metrics {
    pub fn new() -> Result<Self> {
        let requests_total = prometheus::register_int_counter!(
            "inference_requests_total",
            "Total inference requests accepted by the gateway"
        )?;
        let completed_requests = prometheus::register_int_counter!(
            "inference_completed_requests_total",
            "Total inference requests completed"
        )?;
        let failed_requests = prometheus::register_int_counter!(
            "inference_failed_requests_total",
            "Total inference requests failed"
        )?;
        let cancelled_requests = prometheus::register_int_counter!(
            "inference_cancelled_requests_total",
            "Total inference requests cancelled"
        )?;
        let rejected_requests = prometheus::register_int_counter!(
            "inference_rejected_requests_total",
            "Total inference requests rejected by admission control"
        )?;
        let tokens_total = prometheus::register_int_counter!(
            "inference_tokens_total",
            "Total token events routed by the gateway"
        )?;
        let active_streams = prometheus::register_int_gauge!(
            "inference_active_streams",
            "Currently active client streams"
        )?;
        let enqueue_latency_seconds = prometheus::register_histogram!(HistogramOpts::new(
            "inference_enqueue_latency_seconds",
            "Latency to validate and enqueue inference requests"
        ))?;
        Ok(Self {
            requests_total,
            completed_requests,
            failed_requests,
            cancelled_requests,
            rejected_requests,
            tokens_total,
            active_streams,
            enqueue_latency_seconds,
        })
    }
}

pub async fn spawn_metrics_server(addr: SocketAddr) -> Result<()> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(|| async { "ok" }));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(addr = %addr, "starting metrics server");
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            tracing::error!(error = %err, "metrics server failed");
        }
    });
    Ok(())
}

async fn metrics_handler() -> String {
    let encoder = TextEncoder::new();
    let families = prometheus::gather();
    let mut buffer = Vec::new();
    if encoder.encode(&families, &mut buffer).is_err() {
        return "failed to encode metrics".to_string();
    }
    String::from_utf8(buffer).unwrap_or_else(|_| "metrics encoding was not utf8".to_string())
}
