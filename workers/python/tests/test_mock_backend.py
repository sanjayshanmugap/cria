from __future__ import annotations

from inference_worker.backends.mock_backend import MockBackend
from inference_worker.events import SamplingOptions


def test_mock_backend_generates_requested_token_count() -> None:
    backend = MockBackend(token_delay_s=0)

    tokens = list(
        backend.generate(
            prompt="hello",
            max_tokens=5,
            sampling=SamplingOptions(),
            should_cancel=lambda: False,
        )
    )

    assert [token.text for token in tokens] == ["This ", "is ", "a ", "mock ", "distributed "]
    assert all(token.probability == 1.0 for token in tokens)


def test_mock_backend_stops_when_cancelled() -> None:
    backend = MockBackend(token_delay_s=0)
    calls = 0

    def should_cancel() -> bool:
        nonlocal calls
        calls += 1
        return calls > 3

    tokens = list(
        backend.generate(
            prompt="hello",
            max_tokens=10,
            sampling=SamplingOptions(),
            should_cancel=should_cancel,
        )
    )

    assert len(tokens) == 3
