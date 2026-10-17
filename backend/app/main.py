import asyncio
import logging
from concurrent import futures
from contextlib import asynccontextmanager

import grpc
from fastapi import FastAPI

from .config import API_PORTS, DATA_DIR, GRPC_PORTS, SERVICE_NAME
from .event_bus import consume_events, publish_outbox
from .rpc_services import RpcHandlers, add_service
from .store import Store

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
service = SERVICE_NAME
if service not in API_PORTS:
    raise RuntimeError(f"SERVICE_NAME must be one of {', '.join(API_PORTS)}")
store = Store(service, DATA_DIR)
stop_event = asyncio.Event()
background_tasks = []
grpc_server = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global grpc_server
    stop_event.clear()
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=12))
    add_service(grpc_server, service, RpcHandlers(service))
    grpc_server.add_insecure_port(f"0.0.0.0:{GRPC_PORTS[service]}")
    grpc_server.start()
    if service != "notification":
        background_tasks.append(asyncio.create_task(publish_outbox(store, stop_event)))
    if service == "notification":
        background_tasks.append(asyncio.create_task(consume_events(store, stop_event)))
    yield
    stop_event.set()
    for task in background_tasks:
        task.cancel()
    if background_tasks:
        await asyncio.gather(*background_tasks, return_exceptions=True)
    grpc_server.stop(grace=2).wait()
    background_tasks.clear()


app = FastAPI(title=f"{service.title()} Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health():
    with store.connect() as db:
        db.execute("SELECT 1").fetchone()
    return {"service": service, "status": "ok", "grpc_port": GRPC_PORTS[service]}


@app.get("/")
def root():
    return {"service": service, "message": f"{service.title()} service is running"}
