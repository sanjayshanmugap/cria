from __future__ import annotations

import argparse
import datetime as dt
import os
import platform
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RunResult:
    success: bool
    ttft_seconds: float
    total_seconds: float
    tokens: int
    error: str = ""


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def latency_summary(values: list[float]) -> dict[str, float]:
    return {
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
    }


def git_sha() -> str:
    if sha := os.getenv("GITHUB_SHA"):
        return sha
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[3],
            text=True,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def build_report(
    *,
    args: argparse.Namespace,
    results: list[RunResult],
    wall_seconds: float,
    prompt_count: int,
) -> dict[str, Any]:
    successful = [result for result in results if result.success]
    failed = [result for result in results if not result.success]
    token_count = sum(result.tokens for result in successful)
    return {
        "schema_version": 1,
        "timestamp_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "git_sha": git_sha(),
        "environment": {
            "runner_os": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "cpu_count": os.cpu_count(),
        },
        "configuration": {
            "addr": args.addr,
            "backend": args.backend,
            "model_id": args.model_id,
            "parallel": args.parallel,
            "max_tokens": args.max_tokens,
            "prompt_count": prompt_count,
            "repetitions": args.repetitions,
            "worker_replicas": args.worker_replicas,
            "kafka_partitions": args.kafka_partitions,
        },
        "results": {
            "requests_total": len(results),
            "requests_succeeded": len(successful),
            "requests_failed": len(failed),
            "success_rate": len(successful) / len(results) if results else 0.0,
            "tokens_total": token_count,
            "wall_seconds": wall_seconds,
            "requests_per_second": len(successful) / wall_seconds if wall_seconds else 0.0,
            "tokens_per_second": token_count / wall_seconds if wall_seconds else 0.0,
            "ttft_seconds": latency_summary(
                [result.ttft_seconds for result in successful]
            ),
            "total_latency_seconds": latency_summary(
                [result.total_seconds for result in successful]
            ),
        },
        "failures": [asdict(result) for result in failed],
    }
