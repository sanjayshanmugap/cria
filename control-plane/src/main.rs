mod api;
mod bff;
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
use kafka::{wait_for_topics, KafkaProducer, TokenEventConsumer};
use pb::inference_gateway_server::InferenceGatewayServer;
use state::RequestState;
use streaming::StreamRegistry;
use telemetry::Metrics;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = Config::from_env()?;
    wait_for_topics(&config).await?;
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
    bff::spawn_bff_server(
        config.clone(),
        producer.clone(),
        state.clone(),
        registry.clone(),
        metrics.clone(),
    )
    .await?;

    let gateway = GatewayService::new(config.clone(), producer, state, registry, metrics);
    info!(addr = %config.grpc_addr, "starting gRPC gateway");

    let mut server = Server::builder();
    if let Some(tls_config) = load_server_tls(&config).await? {
        server = server.tls_config(tls_config)?;
        info!("gRPC TLS enabled");
    }

    server
        .add_service(InferenceGatewayServer::new(gateway))
        .serve_with_shutdown(config.grpc_addr, shutdown_signal())
        .await?;

    Ok(())
}

async fn load_server_tls(config: &Config) -> anyhow::Result<Option<ServerTlsConfig>> {
    let (Some(cert_path), Some(key_path)) = (&config.grpc_tls_cert, &config.grpc_tls_key) else {
        return Ok(None);
    };

    let cert = tokio::fs::read(cert_path).await?;
    let key = tokio::fs::read(key_path).await?;
    let mut tls_config = ServerTlsConfig::new().identity(Identity::from_pem(cert, key));

    if config.grpc_tls_require_client_auth {
        let Some(client_ca_path) = &config.grpc_tls_client_ca else {
            anyhow::bail!("GRPC_TLS_CLIENT_CA must be set when client auth is required");
        };
        let client_ca = tokio::fs::read(client_ca_path).await?;
        tls_config = tls_config.client_ca_root(Certificate::from_pem(client_ca));
    }

    Ok(Some(tls_config))
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
