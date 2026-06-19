use anyhow::{Context, Result};
use std::{collections::HashMap, env, net::SocketAddr, time::Duration};

#[derive(Clone, Debug)]
pub struct Config {
    pub grpc_addr: SocketAddr,
    pub bff_addr: SocketAddr,
    pub metrics_addr: SocketAddr,
    pub kafka_brokers: String,
    pub model_routes: HashMap<String, String>,
    pub default_model_id: String,
    pub token_topic: String,
    pub control_topic: String,
    pub gateway_group_id: String,
    pub max_active_requests: usize,
    pub max_prompt_chars: usize,
    pub default_max_tokens: u32,
    pub max_tokens_limit: u32,
    pub request_timeout: Duration,
    pub grpc_tls_cert: Option<String>,
    pub grpc_tls_key: Option<String>,
    pub grpc_tls_client_ca: Option<String>,
    pub grpc_tls_require_client_auth: bool,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let request_topic = env_or("KAFKA_REQUEST_TOPIC", "inference_requests.mock");
        let default_model_id = env_or("DEFAULT_MODEL_ID", "mock");
        let model_routes = parse_model_routes(
            &env_or(
                "MODEL_ROUTES",
                &format!(
                    "mock={request_topic},tinyllama-1.1b-chat=inference_requests.tinyllama-1.1b-chat"
                ),
            ),
            &default_model_id,
            &request_topic,
        )?;
        Ok(Self {
            grpc_addr: parse_env("GRPC_ADDR", "0.0.0.0:50051")?,
            bff_addr: parse_env("BFF_ADDR", "0.0.0.0:8080")?,
            metrics_addr: parse_env("METRICS_ADDR", "0.0.0.0:9090")?,
            kafka_brokers: env_or("KAFKA_BROKERS", "localhost:9092"),
            model_routes,
            default_model_id,
            token_topic: env_or("KAFKA_TOKEN_TOPIC", "inference_token_events"),
            control_topic: env_or("KAFKA_CONTROL_TOPIC", "inference_control_events"),
            gateway_group_id: env::var("KAFKA_GATEWAY_GROUP_ID")
                .unwrap_or_else(|_| format!("llm-inference-gateway-{}", uuid::Uuid::new_v4())),
            max_active_requests: parse_env("MAX_ACTIVE_REQUESTS", "128")?,
            max_prompt_chars: parse_env("MAX_PROMPT_CHARS", "12000")?,
            default_max_tokens: parse_env("DEFAULT_MAX_TOKENS", "128")?,
            max_tokens_limit: parse_env("MAX_TOKENS_LIMIT", "1024")?,
            request_timeout: Duration::from_millis(parse_env("REQUEST_TIMEOUT_MS", "120000")?),
            grpc_tls_cert: optional_env("GRPC_TLS_CERT"),
            grpc_tls_key: optional_env("GRPC_TLS_KEY"),
            grpc_tls_client_ca: optional_env("GRPC_TLS_CLIENT_CA"),
            grpc_tls_require_client_auth: parse_env("GRPC_TLS_REQUIRE_CLIENT_AUTH", "false")?,
        })
    }

    pub fn request_topic_for_model(&self, model_id: &str) -> Option<&str> {
        self.model_routes.get(model_id).map(String::as_str)
    }

    pub fn model_ids(&self) -> Vec<String> {
        let mut ids = self.model_routes.keys().cloned().collect::<Vec<_>>();
        ids.sort();
        ids
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn optional_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.trim().is_empty())
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

fn parse_model_routes(
    raw: &str,
    default_model_id: &str,
    default_topic: &str,
) -> Result<HashMap<String, String>> {
    let mut routes = HashMap::new();
    for entry in raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let Some((model_id, topic)) = entry.split_once('=') else {
            anyhow::bail!("invalid MODEL_ROUTES entry: {entry}");
        };
        let model_id = model_id.trim();
        let topic = topic.trim();
        if model_id.is_empty() || topic.is_empty() {
            anyhow::bail!("invalid MODEL_ROUTES entry: {entry}");
        }
        routes.insert(model_id.to_string(), topic.to_string());
    }
    routes
        .entry(default_model_id.to_string())
        .or_insert_with(|| default_topic.to_string());
    Ok(routes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_model_routes() {
        let routes = parse_model_routes(
            "mock=inference_requests.mock,llama=inference_requests.llama",
            "mock",
            "fallback",
        )
        .expect("valid routes");

        assert_eq!(
            routes.get("mock").map(String::as_str),
            Some("inference_requests.mock")
        );
        assert_eq!(
            routes.get("llama").map(String::as_str),
            Some("inference_requests.llama")
        );
    }

    #[test]
    fn inserts_default_route_when_missing() {
        let routes = parse_model_routes(
            "llama=inference_requests.llama",
            "mock",
            "inference_requests.mock",
        )
        .expect("valid routes");

        assert_eq!(
            routes.get("mock").map(String::as_str),
            Some("inference_requests.mock")
        );
    }
}
