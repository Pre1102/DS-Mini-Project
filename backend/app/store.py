import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, service: str, directory: Path):
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / f"{service}.db"
        self.service = service
        self._initialize()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=10)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA foreign_keys=ON")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
                INSERT OR IGNORE INTO metadata(key, value) VALUES ('lamport_clock', 0);
                CREATE TABLE IF NOT EXISTS outbox (
                    id TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at TEXT NOT NULL,
                    sent_at TEXT, attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT
                );
            """)
            if self.service == "order":
                db.executescript("""
                    CREATE TABLE IF NOT EXISTS orders (
                        id TEXT PRIMARY KEY, product_id TEXT NOT NULL, product_name TEXT NOT NULL,
                        quantity INTEGER NOT NULL, total REAL NOT NULL, status TEXT NOT NULL,
                        idempotency_key TEXT NOT NULL UNIQUE, clock INTEGER NOT NULL, created_at TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS events (
                        id TEXT PRIMARY KEY, service TEXT NOT NULL, event_type TEXT NOT NULL,
                        entity_id TEXT NOT NULL, clock INTEGER NOT NULL, timestamp TEXT NOT NULL, data_json TEXT NOT NULL
                    );
                """)
            elif self.service == "payment":
                db.executescript("""
                    CREATE TABLE IF NOT EXISTS payments (
                        order_id TEXT PRIMARY KEY, amount REAL NOT NULL, success INTEGER NOT NULL,
                        message TEXT NOT NULL, transaction_id TEXT NOT NULL, created_at TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS events (
                        id TEXT PRIMARY KEY, service TEXT NOT NULL, event_type TEXT NOT NULL,
                        entity_id TEXT NOT NULL, clock INTEGER NOT NULL, timestamp TEXT NOT NULL, data_json TEXT NOT NULL
                    );
                """)
            elif self.service == "inventory":
                db.executescript("""
                    CREATE TABLE IF NOT EXISTS products (
                        id TEXT PRIMARY KEY, name TEXT NOT NULL, price REAL NOT NULL, stock INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS reservations (
                        order_id TEXT PRIMARY KEY, product_id TEXT NOT NULL, quantity INTEGER NOT NULL,
                        state TEXT NOT NULL, created_at TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS events (
                        id TEXT PRIMARY KEY, service TEXT NOT NULL, event_type TEXT NOT NULL,
                        entity_id TEXT NOT NULL, clock INTEGER NOT NULL, timestamp TEXT NOT NULL, data_json TEXT NOT NULL
                    );
                """)
                count = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
                if count == 0:
                    db.executemany(
                        "INSERT INTO products(id,name,price,stock) VALUES(?,?,?,?)",
                        [
                            ("p-100", "Wireless Headphones", 79.99, 12),
                            ("p-200", "Mechanical Keyboard", 109.50, 8),
                            ("p-300", "USB-C Hub", 34.95, 20),
                            ("p-400", "Desk Lamp", 42.00, 10),
                        ],
                    )
            elif self.service == "notification":
                db.executescript("""
                    CREATE TABLE IF NOT EXISTS events (
                        id TEXT PRIMARY KEY, service TEXT NOT NULL, event_type TEXT NOT NULL,
                        entity_id TEXT NOT NULL, clock INTEGER NOT NULL, timestamp TEXT NOT NULL,
                        data_json TEXT NOT NULL, received_at TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS duplicates (
                        event_id TEXT PRIMARY KEY, count INTEGER NOT NULL
                    );
                """)

    def next_clock(self, observed: int | None = None, connection: sqlite3.Connection | None = None) -> int:
        if connection is not None:
            row = connection.execute("SELECT value FROM metadata WHERE key='lamport_clock'").fetchone()
            clock = max(int(row[0]), observed or 0) + 1
            connection.execute("UPDATE metadata SET value=? WHERE key='lamport_clock'", (clock,))
            return clock
        with self.connect() as db:
            return self.next_clock(observed, db)

    def record_event(
        self,
        event_type: str,
        entity_id: str,
        data: dict[str, Any],
        observed_clock: int | None = None,
        connection: sqlite3.Connection | None = None,
    ) -> dict[str, Any]:
        if connection is not None:
            return self._record_event(connection, event_type, entity_id, data, observed_clock)
        with self.connect() as db:
            return self._record_event(db, event_type, entity_id, data, observed_clock)

    def _record_event(
        self,
        db: sqlite3.Connection,
        event_type: str,
        entity_id: str,
        data: dict[str, Any],
        observed_clock: int | None,
    ) -> dict[str, Any]:
        event = {
            "id": str(uuid.uuid4()), "service": self.service, "event_type": event_type,
            "entity_id": entity_id, "clock": self.next_clock(observed_clock, db),
            "timestamp": now_iso(), "data": data,
        }
        db.execute(
            "INSERT INTO events(id,service,event_type,entity_id,clock,timestamp,data_json) VALUES(?,?,?,?,?,?,?)",
            (event["id"], self.service, event_type, entity_id, event["clock"], event["timestamp"], json.dumps(data)),
        )
        db.execute(
            "INSERT INTO outbox(id,payload,created_at) VALUES(?,?,?)",
            (event["id"], json.dumps(event), event["timestamp"]),
        )
        return event

    def events(self, limit: int = 100) -> list[dict[str, Any]]:
        if self.service != "order":
            return []
        with self.connect() as db:
            rows = db.execute("SELECT * FROM events ORDER BY clock DESC LIMIT ?", (limit,)).fetchall()
        return [dict(row) | {"data": json.loads(row["data_json"])} for row in rows]
