from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Iterable

import grpc

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
if not (HERE / "inference_pb2.py").exists():
    subprocess.check_call([sys.executable, str(HERE / "generate_grpc.py")])

import inference_pb2  # type: ignore  # noqa: E402
import inference_pb2_grpc  # type: ignore  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Stream inference responses from the Rust gateway")
    parser.add_argument("--addr", default="localhost:50051")
    parser.add_argument("--prompt")
    parser.add_argument("--file", type=Path)
    parser.add_argument("--max-tokens", type=int, default=64)
    args = parser.parse_args()

    prompts = list(load_prompts(args.prompt, args.file))
    with grpc.insecure_channel(args.addr) as channel:
        stub = inference_pb2_grpc.InferenceGatewayStub(channel)
        for prompt in prompts:
            print(f"\n>>> {prompt}")
            request = inference_pb2.InferenceRequest(prompt=prompt, max_tokens=args.max_tokens)
            for event in stub.Submit(request):
                if event.event_type == inference_pb2.TOKEN_EVENT_TYPE_TOKEN:
                    print(event.token, end="", flush=True)
                elif event.event_type in (
                    inference_pb2.TOKEN_EVENT_TYPE_COMPLETED,
                    inference_pb2.TOKEN_EVENT_TYPE_FAILED,
                    inference_pb2.TOKEN_EVENT_TYPE_CANCELLED,
                ):
                    print(f"\n[{inference_pb2.TokenEventType.Name(event.event_type)}]")
    return 0


def load_prompts(prompt: str | None, path: Path | None) -> Iterable[str]:
    if prompt:
        yield prompt
    if path:
        yield from (line.strip() for line in path.read_text().splitlines() if line.strip())
    if not prompt and not path:
        yield "Explain Kafka-backed token streaming in one paragraph."


if __name__ == "__main__":
    raise SystemExit(main())
