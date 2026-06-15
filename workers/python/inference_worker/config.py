from __future__ import annotations

import os
import socket
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerConfig:
    kafka_brokers: str = os.getenv("KAFKA_BROKERS", "localhost:9092")
    request_topic: str = os.getenv("KAFKA_REQUEST_TOPIC", "inference_requests")
    token_topic: str = os.getenv("KAFKA_TOKEN_TOPIC", "inference_token_events")
    control_topic: str = os.getenv("KAFKA_CONTROL_TOPIC", "inference_control_events")
    group_id: str = os.getenv("KAFKA_GROUP_ID", "llm-inference-workers")
    worker_id: str = os.getenv("WORKER_ID", socket.gethostname())
    backend: str = os.getenv("BACKEND", "mock")
    model_name: str = os.getenv("MODEL_NAME", "TinyLlama/TinyLlama-1.1B-Chat-v1.0")
    device: str = os.getenv("DEVICE", "auto")
    dtype: str = os.getenv("DTYPE", "float16")
    default_temperature: float = float(os.getenv("TEMPERATURE", "0.7"))
    default_top_p: float = float(os.getenv("TOP_P", "0.9"))
    default_top_k: int = int(os.getenv("TOP_K", "50"))
    poll_timeout_s: float = float(os.getenv("POLL_TIMEOUT_S", "1.0"))
    mock_token_delay_s: float = float(os.getenv("MOCK_TOKEN_DELAY_S", "0.03"))
    metrics_port: int = int(os.getenv("METRICS_PORT", "9100"))


def load_config() -> WorkerConfig:
    return WorkerConfig()
