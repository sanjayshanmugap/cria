use crate::{
    config::Config,
    kafka::{InferenceJob, KafkaProducer, SamplingJobOptions, StreamJobOptions},
    pb::{RequestStatus, TokenEventType},
    state::{now_ms, RequestRecord, RequestState},
    streaming::StreamRegistry,
    telemetry::Metrics,
};
use anyhow::Result;
use axum::{
    extract::{Path, State},
    response::{
        sse::{Event, Sse},
        IntoResponse,
    },
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::Status;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Clone)]
struct BffState {
    config: Config,
    producer: KafkaProducer,
    state: RequestState,
    streams: StreamRegistry,
    metrics: Metrics,
}

pub async fn spawn_bff_server(
    config: Config,
    producer: KafkaProducer,
    state: RequestState,
    streams: StreamRegistry,
    metrics: Metrics,
) -> Result<()> {
    let addr = config.bff_addr;
    let app_state = Arc::new(BffState {
        config,
        producer,
        state,
        streams,
        metrics,
    });
    let app = Router::new()
        .route("/api/models", get(models))
        .route("/api/infer", post(infer))
        .route("/api/infer/:request_id/status", get(status))
        .route("/api/infer/:request_id/cancel", post(cancel))
        .with_state(app_state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(addr = %addr, "starting BFF server");
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            error!(error = %err, "BFF server failed");
        }
    });
    Ok(())
}

async fn models(State(state): State<Arc<BffState>>) -> Json<ModelsResponse> {
    Json(ModelsResponse {
        models: state.config.model_ids(),
        default_model_id: state.config.default_model_id.clone(),
    })
}

async fn infer(
    State(state): State<Arc<BffState>>,
    Json(request): Json<InferRequest>,
) -> axum::response::Result<impl IntoResponse> {
    let prompt = request.prompt.trim().to_string();
    if prompt.is_empty() {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            "prompt must not be empty",
        )
            .into());
    }

    let model_id = request
        .model_id
        .filter(|model_id| !model_id.trim().is_empty())
        .unwrap_or_else(|| state.config.default_model_id.clone());
    if state.config.request_topic_for_model(&model_id).is_none() {
        return Err((axum::http::StatusCode::BAD_REQUEST, "unknown model_id").into());
    }

    let request_id = request
        .request_id
        .filter(|request_id| !request_id.trim().is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let max_tokens = request
        .max_tokens
        .unwrap_or(state.config.default_max_tokens)
        .min(state.config.max_tokens_limit);

    state
        .state
        .insert(RequestRecord::queued(request_id.clone()));
    state.metrics.active_streams.inc();
    let receiver = state.streams.register(request_id.clone()).map_err(|_| {
        (
            axum::http::StatusCode::CONFLICT,
            "request_id already active",
        )
    })?;

    let job = InferenceJob {
        request_id: request_id.clone(),
        model_id,
        prompt,
        max_tokens,
        sampling: None::<SamplingJobOptions>,
        stream_options: None::<StreamJobOptions>,
        deadline_ms: state.config.request_timeout.as_millis() as u64,
        timestamp_ms: now_ms(),
    };

    state.producer.produce_request(&job).await.map_err(|_| {
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "failed to enqueue request",
        )
    })?;
    state.metrics.requests_total.inc();

    let stream = ReceiverStream::new(receiver).map(move |event| {
        let event = match event {
            Ok(event) => BffTokenEvent {
                request_id: event.request_id,
                sequence_number: event.sequence_number,
                token: event.token,
                event_type: TokenEventType::try_from(event.event_type)
                    .unwrap_or(TokenEventType::Unspecified)
                    .as_str_name()
                    .to_string(),
                error_message: event.error_message,
            },
            Err(status) => BffTokenEvent::from_status(status),
        };
        Ok::<_, Infallible>(Event::default().json_data(event).expect("SSE JSON event"))
    });

    Ok(Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(10))
            .text("keep-alive"),
    ))
}

async fn status(
    State(state): State<Arc<BffState>>,
    Path(request_id): Path<String>,
) -> axum::response::Result<Json<StatusResponse>> {
    let Some(record) = state.state.get(&request_id) else {
        return Err((axum::http::StatusCode::NOT_FOUND, "request not found").into());
    };
    let status = RequestStatus::from(record.status).as_str_name().to_string();
    Ok(Json(StatusResponse {
        request_id: record.request_id,
        status,
        emitted_tokens: record.emitted_tokens,
        worker_id: record.worker_id.unwrap_or_default(),
        error_message: record.error_message.unwrap_or_default(),
    }))
}

async fn cancel(
    State(state): State<Arc<BffState>>,
    Path(request_id): Path<String>,
) -> Json<CancelResponse> {
    let accepted = state
        .state
        .cancel(&request_id, "cancelled from web UI".to_string());
    if accepted {
        let _ = state
            .producer
            .produce_cancellation(&crate::kafka::CancellationEvent {
                request_id: request_id.clone(),
                reason: "cancelled from web UI".to_string(),
                timestamp_ms: now_ms(),
            })
            .await;
    }
    Json(CancelResponse {
        request_id,
        accepted,
        message: if accepted {
            "cancellation requested".to_string()
        } else {
            "request not found or already terminal".to_string()
        },
    })
}

#[derive(Serialize)]
struct ModelsResponse {
    models: Vec<String>,
    default_model_id: String,
}

#[derive(Deserialize)]
struct InferRequest {
    request_id: Option<String>,
    model_id: Option<String>,
    prompt: String,
    max_tokens: Option<u32>,
}

#[derive(Serialize)]
struct BffTokenEvent {
    request_id: String,
    sequence_number: u32,
    token: String,
    event_type: String,
    error_message: String,
}

impl BffTokenEvent {
    fn from_status(status: Status) -> Self {
        Self {
            request_id: String::new(),
            sequence_number: 0,
            token: String::new(),
            event_type: "TOKEN_EVENT_TYPE_FAILED".to_string(),
            error_message: status.message().to_string(),
        }
    }
}

#[derive(Serialize)]
struct StatusResponse {
    request_id: String,
    status: String,
    emitted_tokens: u32,
    worker_id: String,
    error_message: String,
}

#[derive(Serialize)]
struct CancelResponse {
    request_id: String,
    accepted: bool,
    message: String,
}
