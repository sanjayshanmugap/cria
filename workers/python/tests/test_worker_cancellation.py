from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from inference_worker.backends.mock_backend import MockBackend
from inference_worker.events import InferenceJob, SamplingOptions
from inference_worker.main import process_job


class FakeKafka:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.flush_count = 0

    def publish_token_event(self, event: dict[str, Any]) -> None:
        self.events.append(event)

    def flush(self) -> None:
        self.flush_count += 1


class CancellationAfterFirstToken:
    def __init__(self) -> None:
        self.checks = 0

    def is_cancelled(self, request_id: str) -> bool:
        assert request_id == "cancel-me"
        self.checks += 1
        return self.checks >= 3


def test_process_job_emits_cancelled_when_backend_stops() -> None:
    kafka = FakeKafka()
    cancellations = CancellationAfterFirstToken()
    job = InferenceJob(
        request_id="cancel-me",
        model_id="mock",
        prompt="cancel after one token",
        max_tokens=16,
        sampling=SamplingOptions(),
    )

    process_job(
        job,
        SimpleNamespace(worker_id="test-worker"),
        MockBackend(token_delay_s=0),
        kafka,
        cancellations,
    )

    assert [event["event_type"] for event in kafka.events] == [
        "STARTED",
        "TOKEN",
        "CANCELLED",
    ]
    assert kafka.events[-1]["error_message"] == "cancelled by client"
    assert kafka.flush_count == 1
