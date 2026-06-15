from __future__ import annotations

import time
from typing import Callable, Iterator

from inference_worker.backends.base import GeneratedToken
from inference_worker.events import SamplingOptions


class MockBackend:
    def __init__(self, token_delay_s: float = 0.03) -> None:
        self.token_delay_s = token_delay_s

    def generate(
        self,
        *,
        prompt: str,
        max_tokens: int,
        sampling: SamplingOptions,
        should_cancel: Callable[[], bool],
    ) -> Iterator[GeneratedToken]:
        del sampling
        words = (
            "This is a mock distributed inference response for: " + prompt[:120]
        ).split()
        for idx in range(max_tokens):
            if should_cancel():
                return
            time.sleep(self.token_delay_s)
            yield GeneratedToken(text=words[idx % len(words)] + " ", probability=1.0)
