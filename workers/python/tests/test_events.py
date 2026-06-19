from __future__ import annotations

from inference_worker.events import InferenceJob, SamplingOptions, token_event


def test_inference_job_uses_defaults_for_missing_sampling() -> None:
    defaults = SamplingOptions(temperature=0.2, top_p=0.8, top_k=25, seed=7)
    job = InferenceJob.from_payload(
        {
            "request_id": "req-1",
            "prompt": "hello",
            "max_tokens": 8,
        },
        defaults,
    )

    assert job.request_id == "req-1"
    assert job.model_id == "mock"
    assert job.prompt == "hello"
    assert job.max_tokens == 8
    assert job.sampling == defaults


def test_inference_job_parses_sampling_overrides() -> None:
    job = InferenceJob.from_payload(
        {
            "request_id": "req-1",
            "prompt": "hello",
            "max_tokens": 8,
            "sampling": {
                "temperature": 0.4,
                "top_p": 0.7,
                "top_k": 10,
                "seed": 123,
            },
        },
        SamplingOptions(),
    )

    assert job.sampling.temperature == 0.4
    assert job.sampling.top_p == 0.7
    assert job.sampling.top_k == 10
    assert job.sampling.seed == 123


def test_token_event_shape_matches_gateway_envelope() -> None:
    event = token_event(
        request_id="req-1",
        sequence_number=2,
        event_type="TOKEN",
        worker_id="worker-a",
        token="hi ",
        probability=1.0,
    )

    assert event["request_id"] == "req-1"
    assert event["sequence_number"] == 2
    assert event["event_type"] == "TOKEN"
    assert event["worker_id"] == "worker-a"
    assert event["error_message"] is None
    assert isinstance(event["timestamp_ms"], int)
