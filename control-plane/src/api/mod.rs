use crate::{
    config::Config,
    kafka::{CancellationEvent, InferenceJob, KafkaProducer, SamplingJobOptions, StreamJobOptions},
    pb::{
        inference_gateway_server::InferenceGateway, CancelRequest, CancelResponse,
        InferenceRequest, RequestStatus, StatusRequest, StatusResponse, TokenEvent, TokenEventType,
    },
    state::{RequestRecord, RequestState},
    streaming::StreamRegistry,
    telemetry::Metrics,
};
use std::{pin::Pin, time::Instant};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};
use uuid::Uuid;

pub struct GatewayService {
    config: Config,
    producer: KafkaProducer,
    state: RequestState,
    streams: StreamRegistry,
    metrics: Metrics,
}

impl GatewayService {
    pub fn new(
        config: Config,
        producer: KafkaProducer,
        state: RequestState,
        streams: StreamRegistry,
        metrics: Metrics,
    ) -> Self {
        Self {
            config,
            producer,
            state,
            streams,
            metrics,
        }
    }
}

#[tonic::async_trait]
impl InferenceGateway for GatewayService {
    type SubmitStream = Pin<Box<dyn Stream<Item = Result<TokenEvent, Status>> + Send + 'static>>;

    async fn submit(
        &self,
        request: Request<InferenceRequest>,
    ) -> Result<Response<Self::SubmitStream>, Status> {
        let started_at = Instant::now();
        let request = request.into_inner();
        let prompt = request.prompt.trim().to_string();

        if prompt.is_empty() {
            return Err(Status::invalid_argument("prompt must not be empty"));
        }
        if prompt.len() > self.config.max_prompt_chars {
            return Err(Status::invalid_argument("prompt exceeds MAX_PROMPT_CHARS"));
        }
        if self.state.active_count() >= self.config.max_active_requests {
            self.metrics.rejected_requests.inc();
            return Err(Status::resource_exhausted("too many active requests"));
        }

        let max_tokens = normalize_max_tokens(
            request.max_tokens,
            self.config.default_max_tokens,
            self.config.max_tokens_limit,
        )?;
        let request_id = if request.request_id.trim().is_empty() {
            Uuid::new_v4().to_string()
        } else {
            request.request_id.trim().to_string()
        };
        let replay_from_beginning = request
            .stream_options
            .as_ref()
            .is_some_and(|options| options.replay_from_beginning);
        if replay_from_beginning {
            return self.replay_cached_events(request_id).await;
        }
        let model_id = if request.model_id.trim().is_empty() {
            self.config.default_model_id.clone()
        } else {
            request.model_id.trim().to_string()
        };
        if self.config.request_topic_for_model(&model_id).is_none() {
            return Err(Status::invalid_argument(format!(
                "unknown model_id {model_id}; available models: {}",
                self.config.model_ids().join(", ")
            )));
        }

        let record = RequestRecord::queued(request_id.clone());
        self.state.insert(record);
        self.metrics.active_streams.inc();

        let receiver = self.streams.register(request_id.clone()).map_err(|_| {
            self.state
                .finish_failed(&request_id, "request id already has an active stream");
            self.metrics.active_streams.dec();
            Status::already_exists("request id already has an active stream")
        })?;

        let job = InferenceJob {
            request_id: request_id.clone(),
            model_id: model_id.clone(),
            prompt,
            max_tokens,
            sampling: request.sampling.map(SamplingJobOptions::from),
            stream_options: request.stream_options.map(StreamJobOptions::from),
            deadline_ms: if request.deadline_ms == 0 {
                self.config.request_timeout.as_millis() as u64
            } else {
                request.deadline_ms
            },
            timestamp_ms: crate::state::now_ms(),
        };

        if let Err(err) = self.producer.produce_request(&job).await {
            error!(request_id = %request_id, error = %err, "failed to enqueue inference request");
            self.streams.unregister(&request_id);
            self.state
                .finish_failed(&request_id, "failed to enqueue inference request");
            self.metrics.failed_requests.inc();
            self.metrics.active_streams.dec();
            return Err(Status::unavailable("failed to enqueue inference request"));
        }

        self.metrics.requests_total.inc();
        self.metrics
            .enqueue_latency_seconds
            .observe(started_at.elapsed().as_secs_f64());
        info!(request_id = %request_id, model_id = %model_id, max_tokens, "enqueued inference request");

        Ok(Response::new(
            Box::pin(ReceiverStream::new(receiver)) as Self::SubmitStream
        ))
    }

    async fn cancel(
        &self,
        request: Request<CancelRequest>,
    ) -> Result<Response<CancelResponse>, Status> {
        let request = request.into_inner();
        let request_id = request.request_id.trim().to_string();
        if request_id.is_empty() {
            return Err(Status::invalid_argument("request_id must not be empty"));
        }

        let accepted = self.state.cancel(&request_id, request.reason.clone());
        if accepted {
            self.metrics.cancelled_requests.inc();
            let event = CancellationEvent {
                request_id: request_id.clone(),
                reason: request.reason,
                timestamp_ms: crate::state::now_ms(),
            };
            if let Err(err) = self.producer.produce_cancellation(&event).await {
                warn!(request_id = %request_id, error = %err, "failed to publish cancellation event");
            }
        }

        Ok(Response::new(CancelResponse {
            request_id,
            accepted,
            message: if accepted {
                "cancellation requested".to_string()
            } else {
                "request not found or already terminal".to_string()
            },
        }))
    }

    async fn get_status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let request_id = request.into_inner().request_id;
        let Some(record) = self.state.get(&request_id) else {
            return Err(Status::not_found("request not found"));
        };

        Ok(Response::new(StatusResponse {
            request_id: record.request_id,
            status: RequestStatus::from(record.status) as i32,
            emitted_tokens: record.emitted_tokens,
            worker_id: record.worker_id.unwrap_or_default(),
            error_message: record.error_message.unwrap_or_default(),
            created_at_ms: record.created_at_ms,
            updated_at_ms: record.updated_at_ms,
        }))
    }
}

impl GatewayService {
    async fn replay_cached_events(
        &self,
        request_id: String,
    ) -> Result<Response<<Self as InferenceGateway>::SubmitStream>, Status> {
        if request_id.trim().is_empty() {
            return Err(Status::invalid_argument(
                "request_id is required when replay_from_beginning is true",
            ));
        }
        if self.state.get(&request_id).is_none() {
            return Err(Status::not_found("request not found"));
        }

        let receiver = self
            .streams
            .register(request_id.clone())
            .map_err(|_| Status::already_exists("request id already has an active stream"))?;
        let streams = self.streams.clone();
        let events = self.state.events(&request_id);
        tokio::spawn(async move {
            let mut terminal = false;
            for event in events {
                let event_type = TokenEventType::try_from(event.event_type)
                    .unwrap_or(TokenEventType::Unspecified);
                terminal = matches!(
                    event_type,
                    TokenEventType::Completed | TokenEventType::Failed | TokenEventType::Cancelled
                );
                let _ = streams.send(&request_id, Ok(event)).await;
            }
            if terminal {
                streams.unregister(&request_id);
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(receiver)) as <Self as InferenceGateway>::SubmitStream
        ))
    }
}

#[allow(clippy::result_large_err)]
fn normalize_max_tokens(value: u32, default: u32, limit: u32) -> Result<u32, Status> {
    let value = if value == 0 { default } else { value };
    if value > limit {
        return Err(Status::invalid_argument(format!(
            "max_tokens exceeds limit of {limit}"
        )));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn max_tokens_uses_default_for_zero() {
        assert_eq!(normalize_max_tokens(0, 64, 128).expect("valid"), 64);
    }

    #[test]
    fn max_tokens_accepts_values_at_limit() {
        assert_eq!(normalize_max_tokens(128, 64, 128).expect("valid"), 128);
    }

    #[test]
    fn max_tokens_rejects_values_above_limit() {
        let err = normalize_max_tokens(129, 64, 128).expect_err("invalid");
        assert_eq!(err.code(), Code::InvalidArgument);
    }
}
