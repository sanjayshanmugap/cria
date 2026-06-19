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
    parser = build_parser()
    args = parser.parse_args()

    with open_channel(args) as channel:
        stub = inference_pb2_grpc.InferenceGatewayStub(channel)
        command = args.command or "submit"
        if command == "submit":
            submit(stub, args)
        elif command == "status":
            show_status(stub, args.request_id)
        elif command == "cancel":
            cancel(stub, args.request_id, args.reason)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Interact with the Rust inference gateway")
    parser.add_argument("--addr", default="localhost:50051")
    add_tls_args(parser)
    parser.add_argument("--prompt")
    parser.add_argument("--file", type=Path)
    parser.add_argument("--max-tokens", type=int, default=64)
    parser.add_argument("--request-id", default="")
    parser.add_argument("--model-id", default="")

    subparsers = parser.add_subparsers(dest="command")
    submit_parser = subparsers.add_parser("submit", help="submit prompts and stream token events")
    add_tls_args(submit_parser)
    submit_parser.add_argument("--prompt")
    submit_parser.add_argument("--file", type=Path)
    submit_parser.add_argument("--max-tokens", type=int, default=64)
    submit_parser.add_argument("--request-id", default="")
    submit_parser.add_argument("--model-id", default="")

    status_parser = subparsers.add_parser("status", help="fetch request status")
    add_tls_args(status_parser)
    status_parser.add_argument("--request-id", required=True)

    cancel_parser = subparsers.add_parser("cancel", help="cancel an active request")
    add_tls_args(cancel_parser)
    cancel_parser.add_argument("--request-id", required=True)
    cancel_parser.add_argument("--reason", default="cancelled by client")
    return parser


def add_tls_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--tls-ca", type=Path)
    parser.add_argument("--tls-cert", type=Path)
    parser.add_argument("--tls-key", type=Path)
    parser.add_argument("--tls-server-name")


def open_channel(args: argparse.Namespace) -> grpc.Channel:
    if not args.tls_ca:
        return grpc.insecure_channel(args.addr)

    root_certificates = args.tls_ca.read_bytes()
    certificate_chain = args.tls_cert.read_bytes() if args.tls_cert else None
    private_key = args.tls_key.read_bytes() if args.tls_key else None
    credentials = grpc.ssl_channel_credentials(
        root_certificates=root_certificates,
        private_key=private_key,
        certificate_chain=certificate_chain,
    )
    options = []
    if args.tls_server_name:
        options.append(("grpc.ssl_target_name_override", args.tls_server_name))
    return grpc.secure_channel(args.addr, credentials, options=options)


def submit(stub: inference_pb2_grpc.InferenceGatewayStub, args: argparse.Namespace) -> None:
    prompts = list(load_prompts(args.prompt, args.file))
    for prompt in prompts:
        print(f"\n>>> {prompt}")
        request = inference_pb2.InferenceRequest(
            request_id=args.request_id,
            model_id=args.model_id,
            prompt=prompt,
            max_tokens=args.max_tokens,
        )
        for event in stub.Submit(request):
            if event.event_type == inference_pb2.TOKEN_EVENT_TYPE_STARTED:
                print(f"[request_id={event.request_id}]")
            elif event.event_type == inference_pb2.TOKEN_EVENT_TYPE_TOKEN:
                print(event.token, end="", flush=True)
            elif event.event_type in (
                inference_pb2.TOKEN_EVENT_TYPE_COMPLETED,
                inference_pb2.TOKEN_EVENT_TYPE_FAILED,
                inference_pb2.TOKEN_EVENT_TYPE_CANCELLED,
            ):
                print(f"\n[{inference_pb2.TokenEventType.Name(event.event_type)}]")


def show_status(stub: inference_pb2_grpc.InferenceGatewayStub, request_id: str) -> None:
    response = stub.GetStatus(inference_pb2.StatusRequest(request_id=request_id))
    print(f"request_id={response.request_id}")
    print(f"status={inference_pb2.RequestStatus.Name(response.status)}")
    print(f"emitted_tokens={response.emitted_tokens}")
    print(f"worker_id={response.worker_id}")
    if response.error_message:
        print(f"error_message={response.error_message}")


def cancel(stub: inference_pb2_grpc.InferenceGatewayStub, request_id: str, reason: str) -> None:
    response = stub.Cancel(inference_pb2.CancelRequest(request_id=request_id, reason=reason))
    print(f"request_id={response.request_id}")
    print(f"accepted={response.accepted}")
    print(f"message={response.message}")


def load_prompts(prompt: str | None, path: Path | None) -> Iterable[str]:
    if prompt:
        yield prompt
    if path:
        yield from (line.strip() for line in path.read_text().splitlines() if line.strip())
    if not prompt and not path:
        yield "Explain Kafka-backed token streaming in one paragraph."


if __name__ == "__main__":
    raise SystemExit(main())
