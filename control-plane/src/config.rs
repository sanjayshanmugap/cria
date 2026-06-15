use anyhow::{Context, Result};
use std::{env, net::SocketAddr, time::Duration};

#[derive(Clone, Debug)]
pub struct Config {
    pub grpc_addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    pub kafka_brokers: String,
    pub request_topic: String,
    pub token_topic: String,
    pub control_topic: String,
    pub gateway_group_id: String,
    pub max_active_requests: usize,
    pub max_prompt_chars: usize,
    pub default_max_tokens: u32,
    pub max_tokens_limit: u32,
    pub request_timeout: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            grpc_addr: parse_env("GRPC_ADDR", "0.0.0.0:50051")?,
            metrics_addr: parse_env("METRICS_ADDR", "0.0.0.0:9090")?,
            kafka_brokers: env_or("KAFKA_BROKERS", "localhost:9092"),
            request_topic: env_or("KAFKA_REQUEST_TOPIC", "inference_requests"),
            token_topic: env_or("KAFKA_TOKEN_TOPIC", "inference_token_events"),
            control_topic: env_or("KAFKA_CONTROL_TOPIC", "inference_control_events"),
            gateway_group_id: env::var("KAFKA_GATEWAY_GROUP_ID")
                .unwrap_or_else(|_| format!("llm-inference-gateway-{}", uuid::Uuid::new_v4())),
            max_active_requests: parse_env("MAX_ACTIVE_REQUESTS", "128")?,
            max_prompt_chars: parse_env("MAX_PROMPT_CHARS", "12000")?,
            default_max_tokens: parse_env("DEFAULT_MAX_TOKENS", "128")?,
            max_tokens_limit: parse_env("MAX_TOKENS_LIMIT", "1024")?,
            request_timeout: Duration::from_millis(parse_env("REQUEST_TIMEOUT_MS", "120000")?),
        })
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn parse_env<T>(key: &str, default: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    env_or(key, default)
        .parse()
        .with_context(|| format!("invalid value for {key}"))
}
