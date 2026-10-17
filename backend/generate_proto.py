from pathlib import Path
from grpc_tools import protoc

ROOT = Path(__file__).parent
OUTPUT = ROOT / "app" / "proto_generated"
OUTPUT.mkdir(parents=True, exist_ok=True)
result = protoc.main([
    "grpc_tools.protoc",
    f"-I{ROOT / 'proto'}",
    f"--python_out={OUTPUT}",
    f"--grpc_python_out={OUTPUT}",
    str(ROOT / "proto" / "order_system.proto"),
])
if result:
    raise SystemExit(result)
(OUTPUT / "__init__.py").touch()
print(f"Generated protobuf modules in {OUTPUT}")
