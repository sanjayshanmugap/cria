from __future__ import annotations

import argparse
import concurrent.futures
import statistics
import time
from pathlib import Path

import grpc

import client
import inference_pb2  # type: ignore
import inference_pb2_grpc  # type: ignore


def run_one(addr: str, prompt: str, max_tokens: int) -> tuple[float, float, int]:
    start = time.perf_counter()
    first_token_at = None
    tokens = 0
    with grpc.insecure_channel(addr) as channel:
        stub = inference_pb2_grpc.InferenceGatewayStub(channel)
        for event in stub.Submit(inference_pb2.InferenceRequest(prompt=prompt, max_tokens=max_tokens)):
            if event.event_type == inference_pb2.TOKEN_EVENT_TYPE_TOKEN:
                tokens += 1
                first_token_at = first_token_at or time.perf_counter()
    end = time.perf_counter()
    return (first_token_at or end) - start, end - start, tokens


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--addr", default="localhost:50051")
    parser.add_argument("--file", type=Path, default=Path("../../examples/prompts.txt"))
    parser.add_argument("--parallel", type=int, default=4)
    parser.add_argument("--max-tokens", type=int, default=64)
    args = parser.parse_args()

    prompts = list(client.load_prompts(None, args.file))
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.parallel) as pool:
        results = list(pool.map(lambda p: run_one(args.addr, p, args.max_tokens), prompts))
    ttft = [r[0] for r in results]
    total = [r[1] for r in results]
    token_count = sum(r[2] for r in results)
    print(f"requests={len(results)} tokens={token_count}")
    print(f"ttft_p50={statistics.median(ttft):.3f}s total_p50={statistics.median(total):.3f}s")
    print(f"tokens_per_second={token_count / sum(total):.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
