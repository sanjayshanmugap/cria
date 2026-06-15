mod api;
mod config;
mod kafka;
mod state;
mod streaming;
mod telemetry;

pub mod pb {
    tonic::include_proto!("inference");
}

use api::GatewayService;
use config::Config;
use kafka::{KafkaProducer, TokenEventConsumer};
use pb::inference_gateway_server::InferenceGatewayServer;
use state::RequestState;
use streaming::StreamRegistry;
use telemetry::Metrics;
use tonic::transport::Server;
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = Config::from_env()?;
    let metrics = Metrics::new()?;
    let state = RequestState::new();
    let registry = StreamRegistry::new();
    let producer = KafkaProducer::new(&config)?;

    let token_consumer = TokenEventConsumer::new(&config)?;
    let router_registry = registry.clone();
    let router_state = state.clone();
    let router_metrics = metrics.clone();
    tokio::spawn(async move {
        if let Err(err) = token_consumer
            .route_events(router_registry, router_state, router_metrics)
            .await
        {
            error!(error = %err, "token event router stopped");
        }
    });

    telemetry::spawn_metrics_server(config.metrics_addr).await?;

    let gateway = GatewayService::new(config.clone(), producer, state, registry, metrics);
    info!(addr = %config.grpc_addr, "starting gRPC gateway");

    Server::builder()
        .add_service(InferenceGatewayServer::new(gateway))
        .serve_with_shutdown(config.grpc_addr, shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        error!(error = %err, "failed to listen for shutdown signal");
    }
    info!("shutdown signal received");
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .init();
}
