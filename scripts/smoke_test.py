from __future__ import annotations

import argparse
import subprocess
import sys
import time
import uuid
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
    parser.add_argument(
        "--model-id",
        action="append",
        dest="model_ids",
        help="model route to test; repeat for multiple models (default: mock)",
    )
    parser.add_argument(
        "--skip-cancel",
        action="store_true",
        help="skip the cancellation path (useful for very fast real models)",
    )
    args = parser.parse_args()

    model_ids = args.model_ids or ["mock"]
    with grpc.insecure_channel(args.addr) as channel:
        grpc.channel_ready_future(channel).result(timeout=args.timeout_seconds)
        stub = inference_pb2_grpc.InferenceGatewayStub(channel)
        for model_id in model_ids:
            run_completion(
                stub,
                model_id=model_id,
                prompt=args.prompt,
                max_tokens=args.max_tokens,
                timeout_seconds=args.timeout_seconds,
            )
        if not args.skip_cancel:
            run_cancellation(
                stub,
                model_id=model_ids[0],
                prompt=args.prompt,
                timeout_seconds=args.timeout_seconds,
            )

    print(
        "smoke test passed: "
        f"models={','.join(model_ids)} completion=status=cancel=verified"
    )
    return 0


def run_completion(
    stub: inference_pb2_grpc.InferenceGatewayStub,
    *,
    model_id: str,
    prompt: str,
    max_tokens: int,
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    request_id = str(uuid.uuid4())
    token_count = 0
    started = False
    completed = False
    request = inference_pb2.InferenceRequest(
        request_id=request_id,
        model_id=model_id,
        prompt=prompt,
        max_tokens=max_tokens,
    )
    for event in stub.Submit(request, timeout=max(1.0, deadline - time.monotonic())):
        if event.event_type == inference_pb2.TOKEN_EVENT_TYPE_STARTED:
            started = True
        elif event.event_type == inference_pb2.TOKEN_EVENT_TYPE_TOKEN:
            token_count += 1
        elif event.event_type == inference_pb2.TOKEN_EVENT_TYPE_COMPLETED:
            completed = True
            break
        elif event.event_type in (
            inference_pb2.TOKEN_EVENT_TYPE_FAILED,
            inference_pb2.TOKEN_EVENT_TYPE_CANCELLED,
        ):
            raise RuntimeError(
                f"{model_id} request ended with "
                f"{inference_pb2.TokenEventType.Name(event.event_type)}: "
                f"{event.error_message}"
            )

    if not started:
        raise RuntimeError(f"{model_id} request completed without a STARTED event")
    if not completed:
        raise RuntimeError(f"{model_id} request did not complete")
    if token_count == 0:
        raise RuntimeError(f"{model_id} request completed without token events")

    status = wait_for_status(
        stub,
        request_id,
        inference_pb2.REQUEST_STATUS_COMPLETED,
        deadline,
    )
    if status.emitted_tokens != token_count:
        raise RuntimeError(
            f"{model_id} status token count mismatch: "
            f"stream={token_count} status={status.emitted_tokens}"
        )
    if not status.worker_id:
        raise RuntimeError(f"{model_id} completed without a worker_id")
    print(
        f"completion passed: model={model_id} request_id={request_id} "
        f"worker={status.worker_id} tokens={token_count}"
    )


def run_cancellation(
    stub: inference_pb2_grpc.InferenceGatewayStub,
    *,
    model_id: str,
    prompt: str,
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    request_id = str(uuid.uuid4())
    cancellation_sent = False
    cancelled = False
    request = inference_pb2.InferenceRequest(
        request_id=request_id,
        model_id=model_id,
        prompt=f"{prompt} cancellation path",
        max_tokens=128,
    )

    for event in stub.Submit(request, timeout=max(1.0, deadline - time.monotonic())):
        if (
            event.event_type == inference_pb2.TOKEN_EVENT_TYPE_TOKEN
            and not cancellation_sent
        ):
            response = stub.Cancel(
                inference_pb2.CancelRequest(
                    request_id=request_id,
                    reason="integration smoke test",
                ),
                timeout=max(1.0, deadline - time.monotonic()),
            )
            if not response.accepted:
                raise RuntimeError(f"cancellation was rejected: {response.message}")
            cancellation_sent = True
        elif event.event_type == inference_pb2.TOKEN_EVENT_TYPE_CANCELLED:
            cancelled = True
            break
        elif event.event_type in (
            inference_pb2.TOKEN_EVENT_TYPE_COMPLETED,
            inference_pb2.TOKEN_EVENT_TYPE_FAILED,
        ):
            raise RuntimeError(
                "cancelled request ended with "
                f"{inference_pb2.TokenEventType.Name(event.event_type)}"
            )

    if not cancellation_sent or not cancelled:
        raise RuntimeError("cancellation path did not emit CANCELLED")
    wait_for_status(
        stub,
        request_id,
        inference_pb2.REQUEST_STATUS_CANCELLED,
        deadline,
    )
    print(f"cancellation passed: model={model_id} request_id={request_id}")


def wait_for_status(
    stub: inference_pb2_grpc.InferenceGatewayStub,
    request_id: str,
    expected_status: int,
    deadline: float,
) -> inference_pb2.StatusResponse:
    last_status = inference_pb2.REQUEST_STATUS_UNSPECIFIED
    while time.monotonic() < deadline:
        response = stub.GetStatus(
            inference_pb2.StatusRequest(request_id=request_id),
            timeout=max(1.0, deadline - time.monotonic()),
        )
        last_status = response.status
        if response.status == expected_status:
            return response
        time.sleep(0.05)
    raise RuntimeError(
        f"request {request_id} status did not become "
        f"{inference_pb2.RequestStatus.Name(expected_status)}; "
        f"last={inference_pb2.RequestStatus.Name(last_status)}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
