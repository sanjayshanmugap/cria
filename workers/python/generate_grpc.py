from __future__ import annotations

from pathlib import Path

from grpc_tools import protoc

ROOT = Path(__file__).resolve().parents[2]
PROTO = ROOT / "proto" / "inference.proto"
OUT = Path(__file__).resolve().parent

raise SystemExit(
    protoc.main(
        [
            "grpc_tools.protoc",
            f"-I{PROTO.parent}",
            f"--python_out={OUT}",
            f"--grpc_python_out={OUT}",
            str(PROTO),
        ]
    )
)
