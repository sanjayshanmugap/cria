from __future__ import annotations

import argparse

from inference_worker.benchmark import RunResult, build_report, percentile


def test_percentile_interpolates_small_samples() -> None:
    values = [1.0, 2.0, 3.0, 4.0]

    assert percentile(values, 0.50) == 2.5
    assert percentile(values, 0.95) == 3.8499999999999996
    assert percentile([], 0.95) == 0.0


def test_report_separates_successes_and_failures() -> None:
    args = argparse.Namespace(
        addr="localhost:50051",
        backend="mock",
        model_id="mock",
        parallel=2,
        max_tokens=8,
        repetitions=1,
        worker_replicas=2,
        kafka_partitions=3,
    )
    results = [
        RunResult(True, 0.1, 0.4, 8),
        RunResult(False, 0.2, 0.3, 1, "connection lost"),
    ]

    report = build_report(
        args=args,
        results=results,
        wall_seconds=0.5,
        prompt_count=2,
    )

    assert report["results"]["requests_succeeded"] == 1
    assert report["results"]["requests_failed"] == 1
    assert report["results"]["success_rate"] == 0.5
    assert report["results"]["tokens_per_second"] == 16.0
    assert report["failures"][0]["error"] == "connection lost"
