from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

import grpc

ROOT = Path(__file__).resolve().parents[1]
PY_WORKER = ROOT / "workers" / "python"
if not (PY_WORKER / "inference_pb2.py").exists():
    subprocess.check_call([sys.executable, str(PY_WORKER / "generate_grpc.py")])
sys.path.insert(0, str(PY_WORKER))

import inference_pb2  # type: ignore  # noqa: E402
import inference_pb2_grpc  # type: ignore  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test a running Cria gateway")
    parser.add_argument("--addr", default="localhost:50051")
    parser.add_argument("--prompt", default="Smoke test Kafka-backed streaming")
    parser.add_argument("--max-tokens", type=int, default=8)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    args = parser.parse_args()

    deadline = time.monotonic() + args.timeout_seconds
    token_count = 0
    completed = False

    with grpc.insecure_channel(args.addr) as channel:
        grpc.channel_ready_future(channel).result(timeout=args.timeout_seconds)
        stub = inference_pb2_grpc.InferenceGatewayStub(channel)
        request = inference_pb2.InferenceRequest(prompt=args.prompt, max_tokens=args.max_tokens)
        for event in stub.Submit(request, timeout=max(1.0, deadline - time.monotonic())):
            if event.event_type == inference_pb2.TOKEN_EVENT_TYPE_TOKEN:
                token_count += 1
            elif event.event_type == inference_pb2.TOKEN_EVENT_TYPE_COMPLETED:
                completed = True
                break
            elif event.event_type in (
                inference_pb2.TOKEN_EVENT_TYPE_FAILED,
                inference_pb2.TOKEN_EVENT_TYPE_CANCELLED,
            ):
                raise RuntimeError(
                    f"request ended with {inference_pb2.TokenEventType.Name(event.event_type)}: "
                    f"{event.error_message}"
                )

    if not completed:
        raise RuntimeError("request did not complete")
    if token_count == 0:
        raise RuntimeError("request completed without token events")

    print(f"smoke test passed: tokens={token_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
