from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol

from inference_worker.events import SamplingOptions


@dataclass(frozen=True)
class GeneratedToken:
    text: str
    probability: float = 0.0


class InferenceBackend(Protocol):
    def generate(
        self,
        *,
        prompt: str,
        max_tokens: int,
        sampling: SamplingOptions,
        should_cancel: Callable[[], bool],
    ) -> list[GeneratedToken] | object:
        """Yield GeneratedToken values for a single request."""
        ...
