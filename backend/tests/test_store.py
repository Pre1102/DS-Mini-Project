import json
import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.event_bus import process_event
from app.store import Store
from reset_demo_data import reset_database


class FakeMessage:
    def __init__(self, body):
        self.body = body
        self.acknowledged = False

    async def ack(self):
        self.acknowledged = True


class StoreTests(unittest.TestCase):
    def test_event_clock_increases_and_outbox_is_persistent(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store("order", Path(directory))
            first = store.record_event("ORDER_CREATED", "order-1", {"quantity": 1})
            second = store.record_event("ORDER_CONFIRMED", "order-1", {"quantity": 1})

            self.assertGreater(second["clock"], first["clock"])
            with store.connect() as db:
                rows = db.execute("SELECT id,payload,sent_at FROM outbox ORDER BY created_at").fetchall()
            self.assertEqual([row["id"] for row in rows], [first["id"], second["id"]])
            self.assertEqual(json.loads(rows[0]["payload"])["clock"], first["clock"])
            self.assertIsNone(rows[0]["sent_at"])

    def test_inventory_seed_data_exists_in_its_own_store(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store("inventory", Path(directory))
            with store.connect() as db:
                products = db.execute("SELECT id,stock FROM products ORDER BY id").fetchall()
            self.assertEqual(len(products), 4)
            self.assertTrue(all(product["stock"] > 0 for product in products))
            self.assertEqual(store.path.name, "inventory.db")

    def test_clock_observes_remote_logical_time(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store("payment", Path(directory))
            self.assertEqual(store.next_clock(17), 18)
            self.assertEqual(store.next_clock(), 19)

    def test_event_and_outbox_rollback_together(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store("order", Path(directory))
            with self.assertRaises(RuntimeError):
                with store.connect() as db:
                    store.record_event("ORDER_CREATED", "order-1", {}, connection=db)
                    raise RuntimeError("simulate crash before commit")
            with store.connect() as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM events").fetchone()[0], 0)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 0)

    def test_notification_consumer_deduplicates_event_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store("notification", Path(directory))
            event = {
                "id": "event-1", "service": "order", "event_type": "ORDER_CONFIRMED",
                "entity_id": "order-1", "clock": 7, "timestamp": "2025-01-01T00:00:00+00:00",
                "data": {"status": "CONFIRMED"},
            }
            first = FakeMessage(json.dumps(event).encode())
            duplicate = FakeMessage(json.dumps(event).encode())
            asyncio.run(process_event(store, first))
            asyncio.run(process_event(store, duplicate))
            self.assertTrue(first.acknowledged and duplicate.acknowledged)
            with store.connect() as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM events").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT SUM(count) FROM duplicates").fetchone()[0], 1)

    def test_demo_reset_clears_history_and_restores_seed_stock(self):
        with tempfile.TemporaryDirectory() as directory, patch("reset_demo_data.DATA_DIR", Path(directory)):
            for service in ("order", "payment", "inventory", "notification"):
                store = Store(service, Path(directory))
                with store.connect() as db:
                    if service == "notification":
                        db.execute(
                            "INSERT INTO events VALUES(?,?,?,?,?,?,?,?)",
                            ("demo-event", "order", "ORDER_CONFIRMED", "demo-1", 1,
                             "2025-01-01", "{}", "2025-01-01"),
                        )
                    else:
                        store.record_event("DEMO_EVENT", "demo-1", {}, connection=db)
                    if service == "order":
                        db.execute(
                            "INSERT INTO orders VALUES(?,?,?,?,?,?,?,?,?)",
                            ("demo-1", "p-100", "Wireless Headphones", 1, 79.99, "CONFIRMED",
                             "demo-key", 1, "2025-01-01T00:00:00+00:00"),
                        )
                    elif service == "payment":
                        db.execute(
                            "INSERT INTO payments VALUES(?,?,?,?,?,?)",
                            ("demo-1", 79.99, 1, "approved", "transaction-1", "2025-01-01"),
                        )
                    elif service == "inventory":
                        db.execute("UPDATE products SET stock=stock-1 WHERE id='p-100'")
                        db.execute(
                            "INSERT INTO reservations VALUES(?,?,?,?,?)",
                            ("demo-1", "p-100", 1, "RESERVED", "2025-01-01"),
                        )
                    elif service == "notification":
                        db.execute("INSERT INTO duplicates VALUES(?,?)", ("demo-event", 1))

            for service in ("order", "payment", "inventory", "notification"):
                reset_database(service)

            for service in ("order", "payment", "notification"):
                store = Store(service, Path(directory))
                with store.connect() as db:
                    for table in ("events", "outbox"):
                        self.assertEqual(db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)
                    self.assertEqual(
                        db.execute("SELECT value FROM metadata WHERE key='lamport_clock'").fetchone()[0], 0
                    )
            with Store("order", Path(directory)).connect() as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 0)
            with Store("payment", Path(directory)).connect() as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM payments").fetchone()[0], 0)
            with Store("inventory", Path(directory)).connect() as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM reservations").fetchone()[0], 0)
                stock = db.execute("SELECT id,stock FROM products ORDER BY id").fetchall()
                self.assertEqual([(row["id"], row["stock"]) for row in stock],
                                 [("p-100", 12), ("p-200", 8), ("p-300", 20), ("p-400", 10)])
            with Store("notification", Path(directory)).connect() as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM duplicates").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
