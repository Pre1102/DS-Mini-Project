import json
import logging
import uuid

import grpc
import aio_pika
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .config import DATA_DIR, GRPC_TARGETS, RABBITMQ_URL
from .event_bus import replay_event
from .rpc_services import pb, pb_grpc
from .store import Store

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("order-platform.gateway")
outbox_store = Store("order", DATA_DIR)
app = FastAPI(title="Distributed Order Platform API", version="1.0.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=False,
    allow_methods=["*"], allow_headers=["*"],
)


class CreateOrderBody(BaseModel):
    product_id: str
    quantity: int = Field(ge=1, le=100)
    payment_success: bool = True
    idempotency_key: str | None = None


def as_dict(message):
    from google.protobuf.json_format import MessageToDict
    return MessageToDict(message, preserving_proto_field_name=True)


def rpc_call(target: str, method: str, request, timeout: float = 5):
    channel = grpc.insecure_channel(GRPC_TARGETS[target])
    try:
        stub_type, method_name = {
            ("order", "CreateOrder"): (pb_grpc.OrdersStub, "CreateOrder"),
            ("order", "ListOrders"): (pb_grpc.OrdersStub, "ListOrders"),
            ("inventory", "ListProducts"): (pb_grpc.InventoryStub, "ListProducts"),
            ("notification", "ListEvents"): (pb_grpc.NotificationsStub, "ListEvents"),
            ("payment", "Health"): (pb_grpc.PaymentsStub, "Health"),
            ("inventory", "Health"): (pb_grpc.InventoryStub, "Health"),
            ("order", "Health"): (pb_grpc.OrdersStub, "Health"),
            ("notification", "Health"): (pb_grpc.NotificationsStub, "Health"),
        }[(target, method)]
        return getattr(stub_type(channel), method_name)(request, timeout=timeout)
    except grpc.RpcError as error:
        if error.code() == grpc.StatusCode.INVALID_ARGUMENT:
            raise HTTPException(status_code=422, detail=error.details()) from error
        if error.code() == grpc.StatusCode.NOT_FOUND:
            raise HTTPException(status_code=404, detail=error.details()) from error
        if error.code() == grpc.StatusCode.UNAVAILABLE:
            raise HTTPException(status_code=503, detail=error.details()) from error
        if error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            raise HTTPException(status_code=504, detail=error.details()) from error
        logger.error("gRPC %s/%s failed: %s", target, method, error)
        raise HTTPException(status_code=502, detail=f"{target} service call failed: {error.details()}") from error
    finally:
        channel.close()


@app.get("/api/health")
async def health():
    services = {}
    for name in ("order", "payment", "inventory", "notification"):
        try:
            reply = rpc_call(name, "Health", pb.Empty(), timeout=1.5)
            services[name] = {"status": reply.status, "grpc": GRPC_TARGETS[name]}
        except HTTPException:
            services[name] = {"status": "unavailable", "grpc": GRPC_TARGETS[name]}
    try:
        connection = await aio_pika.connect(RABBITMQ_URL, timeout=1.5)
        await connection.close()
        services["rabbitmq"] = {"status": "ok", "url": "amqp://localhost"}
    except (aio_pika.AMQPException, OSError, TimeoutError) as error:
        logger.info("RabbitMQ health check failed: %s", error)
        services["rabbitmq"] = {"status": "unavailable", "url": "amqp://localhost"}
    return {"status": "ok" if all(x["status"] == "ok" for x in services.values()) else "degraded",
            "services": services}


@app.websocket("/ws/events")
async def websocket_events(websocket: WebSocket):
    import asyncio

    await websocket.accept()
    last_signature = ""
    try:
        while True:
            try:
                result = await asyncio.to_thread(rpc_call, "notification", "ListEvents", pb.Empty(), 2)
                snapshot = {
                    "type": "events",
                    "events": [as_dict(item) | {"data": json.loads(item.data_json or "{}")} for item in result.events],
                    "duplicate_count": result.duplicate_count,
                }
                signature = json.dumps(snapshot, sort_keys=True)
                if signature != last_signature:
                    await websocket.send_json(snapshot)
                    last_signature = signature
            except HTTPException as error:
                await websocket.send_json({"type": "status", "status": "degraded", "detail": error.detail})
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        logger.info("Dashboard event stream disconnected")


@app.get("/api/products")
def products():
    result = rpc_call("inventory", "ListProducts", pb.Empty())
    return [as_dict(item) for item in result.products]


@app.get("/api/orders")
def orders():
    result = rpc_call("order", "ListOrders", pb.Empty())
    return [as_dict(item) for item in result.orders]


@app.post("/api/orders", status_code=201)
def create_order(body: CreateOrderBody):
    result = rpc_call(
        "order", "CreateOrder",
        pb.OrderRequest(
            product_id=body.product_id, quantity=body.quantity,
            idempotency_key=body.idempotency_key or str(uuid.uuid4()),
            payment_success=body.payment_success,
        ),
        timeout=15,
    )
    return as_dict(result)


@app.get("/api/events")
def events():
    result = rpc_call("notification", "ListEvents", pb.Empty())
    return {"events": [as_dict(event) | {"data": json.loads(event.data_json or "{}")} for event in result.events],
            "duplicate_count": result.duplicate_count}


@app.post("/api/demo/duplicate-event/{event_id}")
async def duplicate_event(event_id: str):
    try:
        found = await replay_event(outbox_store, event_id)
    except (aio_pika.AMQPException, OSError, TimeoutError) as error:
        detail = f"RabbitMQ unavailable ({type(error).__name__}): {error!r}"
        logger.warning("Could not replay event %s: %s", event_id, detail)
        raise HTTPException(status_code=503, detail=detail) from error
    if not found:
        raise HTTPException(status_code=404, detail="Event not found in the order event log")
    return {"message": "Event replayed; notification idempotency will discard the duplicate",
            "event_id": event_id}


@app.get("/api/demo/scenarios")
def scenarios():
    return [
        {"id": "normal", "title": "Successful order", "description": "Reserve stock, approve payment, confirm order."},
        {"id": "payment-failure", "title": "Payment failure", "description": "Decline payment and compensate by restoring reserved stock."},
        {"id": "service-recovery", "title": "Service downtime and recovery", "description": "Stop Inventory, observe degraded status, restart it, then retry."},
        {"id": "duplicate-event", "title": "Duplicate event", "description": "Replay an event; its event ID prevents it being processed twice."},
    ]


@app.get("/health")
def root_health():
    return {"service": "api-gateway", "status": "ok"}
