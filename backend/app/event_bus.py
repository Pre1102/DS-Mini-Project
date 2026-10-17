import asyncio
import json
import logging
from typing import Callable

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from .config import EVENT_EXCHANGE, RABBITMQ_URL
from .store import Store, now_iso

logger = logging.getLogger("order-platform.events")


async def publish_outbox(store: Store, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            with store.connect() as db:
                row = db.execute(
                    "SELECT id,payload FROM outbox WHERE sent_at IS NULL ORDER BY created_at LIMIT 1"
                ).fetchone()
            if row is None:
                await asyncio.sleep(1)
                continue
            connection = await aio_pika.connect_robust(RABBITMQ_URL, timeout=3)
            async with connection:
                channel = await connection.channel()
                exchange = await channel.declare_exchange(EVENT_EXCHANGE, aio_pika.ExchangeType.FANOUT, durable=True)
                await exchange.publish(
                    aio_pika.Message(
                        body=row["payload"].encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        message_id=row["id"],
                        content_type="application/json",
                    ),
                    routing_key="",
                )
            with store.connect() as db:
                db.execute("UPDATE outbox SET sent_at=?,last_error=NULL WHERE id=?", (now_iso(), row["id"]))
            logger.info("Published outbox event %s", row["id"])
        except (aio_pika.AMQPException, OSError, asyncio.TimeoutError) as error:
            logger.warning("Event delivery deferred; outbox will retry (%s): %r", type(error).__name__, error)
            if "row" in locals() and row is not None:
                with store.connect() as db:
                    db.execute(
                        "UPDATE outbox SET attempts=attempts+1,last_error=? WHERE id=?",
                        (f"{type(error).__name__}: {error!r}"[:500], row["id"]),
                    )
            await asyncio.sleep(3)


async def consume_events(store: Store, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_URL, timeout=3)
            async with connection:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=20)
                exchange = await channel.declare_exchange(EVENT_EXCHANGE, aio_pika.ExchangeType.FANOUT, durable=True)
                queue = await channel.declare_queue("notification_events", durable=True)
                await queue.bind(exchange)
                logger.info("Notification consumer connected to RabbitMQ")
                async with queue.iterator() as iterator:
                    async for message in iterator:
                        if stop.is_set():
                            break
                        await process_event(store, message)
        except (aio_pika.AMQPException, OSError, asyncio.TimeoutError) as error:
            logger.warning("Notification consumer disconnected; retrying (%s): %r", type(error).__name__, error)
            await asyncio.sleep(3)


async def process_event(store: Store, message: AbstractIncomingMessage) -> None:
    event = json.loads(message.body.decode())
    with store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        exists = db.execute("SELECT 1 FROM events WHERE id=?", (event["id"],)).fetchone()
        if exists:
            db.execute(
                "INSERT INTO duplicates(event_id,count) VALUES(?,1) "
                "ON CONFLICT(event_id) DO UPDATE SET count=count+1",
                (event["id"],),
            )
            logger.info("Ignored duplicate event %s", event["id"])
        else:
            row = db.execute("SELECT value FROM metadata WHERE key='lamport_clock'").fetchone()
            clock = max(int(row[0]), int(event["clock"])) + 1
            db.execute("UPDATE metadata SET value=? WHERE key='lamport_clock'", (clock,))
            db.execute(
                "INSERT INTO events(id,service,event_type,entity_id,clock,timestamp,data_json,received_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (event["id"], event["service"], event["event_type"], event["entity_id"],
                 clock, event["timestamp"],
                 json.dumps(event.get("data", {})), now_iso()),
            )
    await message.ack()


async def replay_event(store: Store, event_id: str) -> bool:
    if store.service != "order":
        return False
    with store.connect() as db:
        row = db.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
    if row is None:
        return False
    event = {
        "id": row["id"], "service": row["service"], "event_type": row["event_type"],
        "entity_id": row["entity_id"], "clock": row["clock"], "timestamp": row["timestamp"],
        "data": json.loads(row["data_json"]),
    }
    connection = await aio_pika.connect_robust(RABBITMQ_URL, timeout=3)
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(EVENT_EXCHANGE, aio_pika.ExchangeType.FANOUT, durable=True)
        await exchange.publish(
            aio_pika.Message(body=json.dumps(event).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                             message_id=event_id, content_type="application/json"),
            routing_key="",
        )
    return True
