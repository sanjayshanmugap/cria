from __future__ import annotations

import argparse
import concurrent.futures
import json
import time
from pathlib import Path

import grpc

import client
import inference_pb2  # type: ignore
import inference_pb2_grpc  # type: ignore
from inference_worker.benchmark import RunResult, build_report


def run_one(
    addr: str,
    prompt: str,
    max_tokens: int,
    model_id: str,
    timeout_seconds: float,
) -> RunResult:
    start = time.perf_counter()
    first_token_at = None
    tokens = 0
    terminal_event = None
    try:
        with grpc.insecure_channel(addr) as channel:
            stub = inference_pb2_grpc.InferenceGatewayStub(channel)
            request = inference_pb2.InferenceRequest(
                prompt=prompt,
                max_tokens=max_tokens,
                model_id=model_id,
            )
            for event in stub.Submit(request, timeout=timeout_seconds):
                if event.event_type == inference_pb2.TOKEN_EVENT_TYPE_TOKEN:
                    tokens += 1
                    first_token_at = first_token_at or time.perf_counter()
                elif event.event_type in (
                    inference_pb2.TOKEN_EVENT_TYPE_COMPLETED,
                    inference_pb2.TOKEN_EVENT_TYPE_FAILED,
                    inference_pb2.TOKEN_EVENT_TYPE_CANCELLED,
                ):
                    terminal_event = event
        end = time.perf_counter()
        if terminal_event is None:
            raise RuntimeError("stream closed without a terminal event")
        if terminal_event.event_type != inference_pb2.TOKEN_EVENT_TYPE_COMPLETED:
            raise RuntimeError(
                inference_pb2.TokenEventType.Name(terminal_event.event_type)
                + (f": {terminal_event.error_message}" if terminal_event.error_message else "")
            )
        return RunResult(
            success=True,
            ttft_seconds=(first_token_at or end) - start,
            total_seconds=end - start,
            tokens=tokens,
        )
    except Exception as exc:
        end = time.perf_counter()
        return RunResult(
            success=False,
            ttft_seconds=(first_token_at or end) - start,
            total_seconds=end - start,
            tokens=tokens,
            error=str(exc),
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark Cria streaming inference")
    parser.add_argument("--addr", default="localhost:50051")
    parser.add_argument("--file", type=Path, default=Path("../../examples/prompts.txt"))
    parser.add_argument("--parallel", type=int, default=4)
    parser.add_argument("--max-tokens", type=int, default=64)
    parser.add_argument("--model-id", default="mock")
    parser.add_argument("--backend", default="mock")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--worker-replicas", type=int, default=1)
    parser.add_argument("--kafka-partitions", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    prompts = list(client.load_prompts(None, args.file))
    work = prompts * args.repetitions
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.parallel) as pool:
        results = list(
            pool.map(
                lambda prompt: run_one(
                    args.addr,
                    prompt,
                    args.max_tokens,
                    args.model_id,
                    args.timeout_seconds,
                ),
                work,
            )
        )
    wall_seconds = time.perf_counter() - started
    report = build_report(
        args=args,
        results=results,
        wall_seconds=wall_seconds,
        prompt_count=len(prompts),
    )
    summary = report["results"]
    print(
        f"requests={summary['requests_total']} "
        f"succeeded={summary['requests_succeeded']} "
        f"failed={summary['requests_failed']} tokens={summary['tokens_total']}"
    )
    print(
        f"ttft_p50={summary['ttft_seconds']['p50']:.3f}s "
        f"ttft_p95={summary['ttft_seconds']['p95']:.3f}s "
        f"total_p50={summary['total_latency_seconds']['p50']:.3f}s "
        f"total_p95={summary['total_latency_seconds']['p95']:.3f}s"
    )
    print(
        f"requests_per_second={summary['requests_per_second']:.2f} "
        f"tokens_per_second={summary['tokens_per_second']:.2f}"
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(f"report={args.output}")
    return 1 if summary["requests_failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
