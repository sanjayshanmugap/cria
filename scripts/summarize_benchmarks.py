from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Render Cria benchmark JSON reports as Markdown"
    )
    parser.add_argument("directory", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    reports = [json.loads(path.read_text()) for path in sorted(args.directory.glob("*.json"))]
    if not reports:
        raise SystemExit(f"no benchmark JSON files found in {args.directory}")

    markdown = render_markdown(reports)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(markdown)
    else:
        print(markdown, end="")
    return 0


def render_markdown(reports: list[dict[str, Any]]) -> str:
    lines = [
        "## Cria nightly benchmark",
        "",
        "| Workers | Concurrency | Requests | Success | TTFT p50 (ms) | "
        "TTFT p95 (ms) | Total p95 (ms) | Tokens/s |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for report in sorted(
        reports,
        key=lambda item: (
            item["configuration"]["worker_replicas"],
            item["configuration"]["parallel"],
        ),
    ):
        config = report["configuration"]
        result = report["results"]
        lines.append(
            "| {workers} | {parallel} | {requests} | {success:.1%} | "
            "{ttft_p50:.1f} | {ttft_p95:.1f} | {total_p95:.1f} | {tokens:.2f} |".format(
                workers=config["worker_replicas"],
                parallel=config["parallel"],
                requests=result["requests_total"],
                success=result["success_rate"],
                ttft_p50=result["ttft_seconds"]["p50"] * 1000,
                ttft_p95=result["ttft_seconds"]["p95"] * 1000,
                total_p95=result["total_latency_seconds"]["p95"] * 1000,
                tokens=result["tokens_per_second"],
            )
        )
    first = reports[0]
    lines.extend(
        [
            "",
            f"Git SHA: `{first['git_sha']}`  ",
            f"Runner: `{first['environment']['runner_os']}`  ",
            "Backend: deterministic mock; values measure distributed transport and "
            "scheduling overhead, not model quality or GPU inference.",
            "",
        ]
    )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
