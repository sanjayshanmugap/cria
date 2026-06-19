from __future__ import annotations

import os
import socket
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerConfig:
    kafka_brokers: str
    request_topic: str
    token_topic: str
    control_topic: str
    group_id: str
    worker_id: str
    model_id: str
    backend: str
    model_name: str
    device: str
    dtype: str
    default_temperature: float
    default_top_p: float
    default_top_k: int
    poll_timeout_s: float
    mock_token_delay_s: float
    metrics_port: int


def load_config() -> WorkerConfig:
    model_id = os.getenv("MODEL_ID", "mock")
    request_topic = os.getenv("KAFKA_REQUEST_TOPIC", f"inference_requests.{model_id}")
    return WorkerConfig(
        kafka_brokers=os.getenv("KAFKA_BROKERS", "localhost:9092"),
        request_topic=request_topic,
        token_topic=os.getenv("KAFKA_TOKEN_TOPIC", "inference_token_events"),
        control_topic=os.getenv("KAFKA_CONTROL_TOPIC", "inference_control_events"),
        group_id=os.getenv("KAFKA_GROUP_ID", f"llm-inference-workers-{model_id}"),
        worker_id=os.getenv("WORKER_ID", socket.gethostname()),
        model_id=model_id,
        backend=os.getenv("BACKEND", "mock"),
        model_name=os.getenv("MODEL_NAME", "TinyLlama/TinyLlama-1.1B-Chat-v1.0"),
        device=os.getenv("DEVICE", "auto"),
        dtype=os.getenv("DTYPE", "float16"),
        default_temperature=float(os.getenv("TEMPERATURE", "0.7")),
        default_top_p=float(os.getenv("TOP_P", "0.9")),
        default_top_k=int(os.getenv("TOP_K", "50")),
        poll_timeout_s=float(os.getenv("POLL_TIMEOUT_S", "1.0")),
        mock_token_delay_s=float(os.getenv("MOCK_TOKEN_DELAY_S", "0.03")),
        metrics_port=int(os.getenv("METRICS_PORT", "9100")),
    )
