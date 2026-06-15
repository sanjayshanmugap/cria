from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Literal

TokenEventType = Literal["STARTED", "TOKEN", "COMPLETED", "FAILED", "CANCELLED"]


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True)
class SamplingOptions:
    temperature: float = 0.7
    top_p: float = 0.9
    top_k: int = 50
    seed: int = 0

    @classmethod
    def from_payload(cls, payload: dict[str, Any], defaults: SamplingOptions) -> SamplingOptions:
        raw = payload.get("sampling") or {}
        return cls(
            temperature=float(raw.get("temperature") or defaults.temperature),
            top_p=float(raw.get("top_p") or defaults.top_p),
            top_k=int(raw.get("top_k") or defaults.top_k),
            seed=int(raw.get("seed") or defaults.seed),
        )


@dataclass(frozen=True)
class InferenceJob:
    request_id: str
    prompt: str
    max_tokens: int
    sampling: SamplingOptions
    deadline_ms: int = 0

    @classmethod
    def from_payload(cls, payload: dict[str, Any], defaults: SamplingOptions) -> InferenceJob:
        return cls(
            request_id=str(payload["request_id"]),
            prompt=str(payload["prompt"]),
            max_tokens=int(payload.get("max_tokens") or 128),
            sampling=SamplingOptions.from_payload(payload, defaults),
            deadline_ms=int(payload.get("deadline_ms") or 0),
        )


def token_event(
    *,
    request_id: str,
    sequence_number: int,
    event_type: TokenEventType,
    worker_id: str,
    token: str = "",
    probability: float = 0.0,
    error_message: str | None = None,
) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "sequence_number": sequence_number,
        "token": token,
        "probability": probability,
        "event_type": event_type,
        "worker_id": worker_id,
        "error_message": error_message,
        "timestamp_ms": now_ms(),
    }
