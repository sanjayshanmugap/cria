use crate::{
    config::Config,
    pb::{SamplingOptions, StreamOptions, TokenEvent, TokenEventType},
    state::{RequestState, RequestStatusInternal},
    streaming::StreamRegistry,
    telemetry::Metrics,
};
use anyhow::{Context, Result};
use futures::StreamExt;
use rdkafka::{
    consumer::{BaseConsumer, Consumer, StreamConsumer},
    message::Message,
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

pub async fn wait_for_topics(config: &Config) -> Result<()> {
    let mut topics = config
        .model_routes
        .values()
        .map(String::as_str)
        .collect::<Vec<_>>();
    topics.push(config.token_topic.as_str());
    topics.push(config.control_topic.as_str());
    topics.sort_unstable();
    topics.dedup();
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set(
            "group.id",
            format!("{}-topic-wait", config.gateway_group_id),
        )
        .create()
        .context("failed to create Kafka metadata consumer")?;
    let started_at = Instant::now();

    loop {
        match consumer.fetch_metadata(None, Duration::from_secs(5)) {
            Ok(metadata)
                if topics
                    .iter()
                    .all(|topic| metadata.topics().iter().any(|found| found.name() == *topic)) =>
            {
                info!(topics = ?topics, "Kafka topics are available");
                return Ok(());
            }
            Ok(_) => {
                debug!(topics = ?topics, "waiting for Kafka topics");
            }
            Err(err) => {
                debug!(error = %err, "waiting for Kafka broker metadata");
            }
        }

        if started_at.elapsed() > Duration::from_secs(60) {
            anyhow::bail!("timed out waiting for Kafka topics: {topics:?}");
        }
        sleep(Duration::from_secs(1)).await;
    }
}

#[derive(Clone)]
pub struct KafkaProducer {
    producer: FutureProducer,
    request_topics: HashMap<String, String>,
    control_topic: String,
}

impl KafkaProducer {
    pub fn new(config: &Config) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka_brokers)
            .set("message.timeout.ms", "5000")
            .set("queue.buffering.max.ms", "5")
            .create()
            .context("failed to create Kafka producer")?;
        Ok(Self {
            producer,
            request_topics: config.model_routes.clone(),
            control_topic: config.control_topic.clone(),
        })
    }

    pub async fn produce_request(&self, job: &InferenceJob) -> Result<()> {
        let topic = self
            .request_topics
            .get(&job.model_id)
            .with_context(|| format!("unknown model_id {}", job.model_id))?;
        let payload = serde_json::to_string(job)?;
        self.producer
            .send(
                FutureRecord::to(topic)
                    .key(&job.request_id)
                    .payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| anyhow::anyhow!(err))?;
        Ok(())
    }

    pub async fn produce_cancellation(&self, event: &CancellationEvent) -> Result<()> {
        let payload = serde_json::to_string(event)?;
        self.producer
            .send(
                FutureRecord::to(&self.control_topic)
                    .key(&event.request_id)
                    .payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| anyhow::anyhow!(err))?;
        Ok(())
    }
}

pub struct TokenEventConsumer {
    consumer: StreamConsumer,
    token_topic: String,
}

impl TokenEventConsumer {
    pub fn new(config: &Config) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka_brokers)
            .set("group.id", &config.gateway_group_id)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "latest")
            .create()
            .context("failed to create Kafka token consumer")?;
        consumer
            .subscribe(&[&config.token_topic])
            .context("failed to subscribe to token topic")?;
        Ok(Self {
            consumer,
            token_topic: config.token_topic.clone(),
        })
    }

    pub async fn route_events(
        self,
        streams: StreamRegistry,
        state: RequestState,
        metrics: Metrics,
    ) -> Result<()> {
        info!(topic = %self.token_topic, "routing token events from Kafka");
        let mut stream = self.consumer.stream();
        while let Some(message) = stream.next().await {
            match message {
                Ok(message) => {
                    let Some(payload) = message.payload() else {
                        continue;
                    };
                    match serde_json::from_slice::<TokenEventEnvelope>(payload) {
                        Ok(event) => route_one(event, &streams, &state, &metrics).await,
                        Err(err) => warn!(error = %err, "invalid token event payload"),
                    }
                }
                Err(err) => error!(error = %err, "Kafka token consumer error"),
            }
        }
        Ok(())
    }
}

async fn route_one(
    event: TokenEventEnvelope,
    streams: &StreamRegistry,
    state: &RequestState,
    metrics: &Metrics,
) {
    let proto = event.into_proto();
    let request_id = proto.request_id.clone();
    let event_type =
        TokenEventType::try_from(proto.event_type).unwrap_or(TokenEventType::Unspecified);
    state.record_event(proto.clone());

    match event_type {
        TokenEventType::Started => state.mark_running(&request_id, proto.worker_id.clone()),
        TokenEventType::Token => {
            state.note_token(&request_id, proto.sequence_number, proto.worker_id.clone());
            metrics.tokens_total.inc();
        }
        TokenEventType::Completed => {
            state.finish_completed(&request_id, proto.sequence_number, proto.worker_id.clone());
            metrics.completed_requests.inc();
            metrics.active_streams.dec();
        }
        TokenEventType::Failed => {
            state.finish_failed(&request_id, &proto.error_message);
            metrics.failed_requests.inc();
            metrics.active_streams.dec();
        }
        TokenEventType::Cancelled => {
            state.finish_cancelled(&request_id, proto.error_message.clone());
            metrics.cancelled_requests.inc();
            metrics.active_streams.dec();
        }
        TokenEventType::Unspecified => {
            debug!(request_id = %request_id, "ignoring unspecified token event")
        }
    }

    let is_terminal = matches!(
        event_type,
        TokenEventType::Completed | TokenEventType::Failed | TokenEventType::Cancelled
    );
    if let Err(err) = streams.send(&request_id, Ok(proto)).await {
        debug!(request_id = %request_id, error = %err, "no active stream for token event");
    }
    if is_terminal {
        streams.unregister(&request_id);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceJob {
    pub request_id: String,
    pub model_id: String,
    pub prompt: String,
    pub max_tokens: u32,
    pub sampling: Option<SamplingJobOptions>,
    pub stream_options: Option<StreamJobOptions>,
    pub deadline_ms: u64,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplingJobOptions {
    pub temperature: f32,
    pub top_p: f32,
    pub top_k: u32,
    pub seed: u64,
}

impl From<SamplingOptions> for SamplingJobOptions {
    fn from(value: SamplingOptions) -> Self {
        Self {
            temperature: value.temperature,
            top_p: value.top_p,
            top_k: value.top_k,
            seed: value.seed,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamJobOptions {
    pub include_probabilities: bool,
    pub replay_from_beginning: bool,
}

impl From<StreamOptions> for StreamJobOptions {
    fn from(value: StreamOptions) -> Self {
        Self {
            include_probabilities: value.include_probabilities,
            replay_from_beginning: value.replay_from_beginning,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancellationEvent {
    pub request_id: String,
    pub reason: String,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TokenEventKind {
    Started,
    Token,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenEventEnvelope {
    pub request_id: String,
    pub sequence_number: u32,
    pub token: String,
    pub probability: f32,
    pub event_type: TokenEventKind,
    pub worker_id: String,
    pub error_message: Option<String>,
    pub timestamp_ms: u64,
}

impl TokenEventEnvelope {
    pub fn into_proto(self) -> TokenEvent {
        let event_type = match self.event_type {
            TokenEventKind::Started => TokenEventType::Started,
            TokenEventKind::Token => TokenEventType::Token,
            TokenEventKind::Completed => TokenEventType::Completed,
            TokenEventKind::Failed => TokenEventType::Failed,
            TokenEventKind::Cancelled => TokenEventType::Cancelled,
        };
        TokenEvent {
            request_id: self.request_id,
            sequence_number: self.sequence_number,
            token: self.token,
            probability: self.probability,
            event_type: event_type as i32,
            worker_id: self.worker_id,
            error_message: self.error_message.unwrap_or_default(),
            timestamp_ms: self.timestamp_ms,
        }
    }
}

impl From<RequestStatusInternal> for crate::pb::RequestStatus {
    fn from(value: RequestStatusInternal) -> Self {
        match value {
            RequestStatusInternal::Queued => crate::pb::RequestStatus::Queued,
            RequestStatusInternal::Running => crate::pb::RequestStatus::Running,
            RequestStatusInternal::Completed => crate::pb::RequestStatus::Completed,
            RequestStatusInternal::Failed => crate::pb::RequestStatus::Failed,
            RequestStatusInternal::Cancelled => crate::pb::RequestStatus::Cancelled,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_python_token_event_payload() {
        let payload = r#"{
            "request_id": "req-1",
            "sequence_number": 7,
            "token": "hello ",
            "probability": 1.0,
            "event_type": "TOKEN",
            "worker_id": "worker-a",
            "error_message": null,
            "timestamp_ms": 1234
        }"#;

        let envelope: TokenEventEnvelope =
            serde_json::from_str(payload).expect("valid token event");
        let proto = envelope.into_proto();

        assert_eq!(proto.request_id, "req-1");
        assert_eq!(proto.sequence_number, 7);
        assert_eq!(proto.token, "hello ");
        assert_eq!(proto.event_type, TokenEventType::Token as i32);
        assert_eq!(proto.error_message, "");
    }

    #[test]
    fn parses_terminal_failed_event_payload() {
        let payload = r#"{
            "request_id": "req-1",
            "sequence_number": 0,
            "token": "",
            "probability": 0.0,
            "event_type": "FAILED",
            "worker_id": "worker-a",
            "error_message": "boom",
            "timestamp_ms": 1234
        }"#;

        let proto = serde_json::from_str::<TokenEventEnvelope>(payload)
            .expect("valid token event")
            .into_proto();

        assert_eq!(proto.event_type, TokenEventType::Failed as i32);
        assert_eq!(proto.error_message, "boom");
    }
}
