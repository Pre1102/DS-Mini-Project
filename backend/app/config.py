import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]

load_dotenv(ROOT / ".env")

DATA_DIR = Path(os.getenv("DATA_DIR", ROOT / "data"))
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
EVENT_EXCHANGE = "order_events"
SERVICE_NAME = os.getenv("SERVICE_NAME", "order")
API_PORTS = {"order": 8001, "payment": 8002, "inventory": 8003, "notification": 8004}
GRPC_PORTS = {"order": 50051, "payment": 50052, "inventory": 50053, "notification": 50054}
GRPC_TARGETS = {
    name: os.getenv(f"{name.upper()}_GRPC", f"127.0.0.1:{port}")
    for name, port in GRPC_PORTS.items()
}
