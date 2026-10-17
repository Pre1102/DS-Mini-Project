import argparse
import asyncio
import logging
import sqlite3

import aio_pika

from app.config import DATA_DIR, RABBITMQ_URL

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("order-platform.reset")

PRODUCTS = (
    ("p-100", "Wireless Headphones", 79.99, 12),
    ("p-200", "Mechanical Keyboard", 109.50, 8),
    ("p-300", "USB-C Hub", 34.95, 20),
    ("p-400", "Desk Lamp", 42.00, 10),
)
TABLES = {
    "order": ("orders", "events", "outbox"),
    "payment": ("payments", "events", "outbox"),
    "inventory": ("reservations", "events", "outbox"),
    "notification": ("events", "duplicates", "outbox"),
}


def reset_database(service: str) -> dict[str, int]:
    path = DATA_DIR / f"{service}.db"
    if not path.exists():
        logger.info("Skipping %s: no database exists yet", path.name)
        return {}

    counts = {}
    db = sqlite3.connect(path, timeout=10)
    try:
        db.execute("PRAGMA busy_timeout=10000")
        db.execute("BEGIN IMMEDIATE")
        existing = {
            row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        for table in TABLES[service]:
            if table in existing:
                counts[table] = db.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                db.execute(f'DELETE FROM "{table}"')
        if "metadata" in existing:
            db.execute("UPDATE metadata SET value=0 WHERE key='lamport_clock'")
        if service == "inventory":
            db.execute("DELETE FROM products")
            db.executemany(
                "INSERT INTO products(id,name,price,stock) VALUES(?,?,?,?)",
                PRODUCTS,
            )
        db.commit()
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        db.close()
    return counts


async def purge_rabbitmq_queue() -> bool:
    connection = await aio_pika.connect(RABBITMQ_URL, timeout=2)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue("notification_events", durable=True)
        result = await queue.purge()
        logger.info("Removed %s queued RabbitMQ notifications", result.message_count)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clear demo orders, payments, inventory reservations, and event history."
    )
    parser.add_argument(
        "--yes", action="store_true",
        help="confirm resetting the four service databases to their original demo state",
    )
    args = parser.parse_args()
    if not args.yes:
        parser.error("Database reset is destructive. Rerun with --yes to confirm.")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for service in ("order", "payment", "inventory", "notification"):
        counts = reset_database(service)
        if counts:
            logger.info("%s reset: %s", service.title(), ", ".join(
                f"{table}={count} cleared" for table, count in counts.items()
            ))

    try:
        asyncio.run(purge_rabbitmq_queue())
    except (aio_pika.AMQPException, OSError, TimeoutError) as error:
        logger.warning(
            "RabbitMQ unavailable; its queue could not be purged (%s: %r). "
            "If the broker was previously used, start it and rerun this command with --yes "
            "to clear any persistent queued messages.",
            type(error).__name__, error,
        )
    logger.info("Demo data reset complete; inventory is back to its original seed quantities.")


if __name__ == "__main__":
    main()
