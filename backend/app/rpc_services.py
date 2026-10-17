import json
import logging
import sqlite3
import sys
import uuid
from pathlib import Path

import grpc

from .config import DATA_DIR, GRPC_TARGETS, SERVICE_NAME
from .store import Store, now_iso

GENERATED = Path(__file__).parent / "proto_generated"
sys.path.insert(0, str(GENERATED))
import order_system_pb2 as pb
import order_system_pb2_grpc as pb_grpc

logger = logging.getLogger("order-platform.rpc")


def event_payload(row) -> dict:
    return {
        "id": row["id"], "service": row["service"], "event_type": row["event_type"],
        "entity_id": row["entity_id"], "clock": row["clock"], "timestamp": row["timestamp"],
        "data": json.loads(row["data_json"]),
    }


class RpcHandlers:
    def __init__(self, service: str):
        self.service = service
        self.store = Store(service, DATA_DIR)

    def Health(self, request, context):
        return pb.HealthReply(service=self.service, status="ok")

    def ListProducts(self, request, context):
        with self.store.connect() as db:
            rows = db.execute("SELECT * FROM products ORDER BY id").fetchall()
        return pb.ProductList(products=[
            pb.Product(id=row["id"], name=row["name"], price=row["price"], stock=row["stock"])
            for row in rows
        ])

    def ReserveStock(self, request, context):
        if request.quantity <= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Quantity must be greater than zero")
        with self.store.connect() as db:
            existing = db.execute("SELECT * FROM reservations WHERE order_id=?", (request.order_id,)).fetchone()
            if existing:
                same = existing["product_id"] == request.product_id and existing["quantity"] == request.quantity
                return pb.StockReply(success=same and existing["state"] == "RESERVED",
                                     message="Reservation already exists" if same else "Order reservation conflicts")
            product = db.execute("SELECT * FROM products WHERE id=?", (request.product_id,)).fetchone()
            if product is None:
                return pb.StockReply(success=False, message="Product not found")
            if product["stock"] < request.quantity:
                return pb.StockReply(success=False, message="Insufficient stock")
            db.execute("UPDATE products SET stock=stock-? WHERE id=?", (request.quantity, request.product_id))
            db.execute(
                "INSERT INTO reservations(order_id,product_id,quantity,state,created_at) VALUES(?,?,?,'RESERVED',?)",
                (request.order_id, request.product_id, request.quantity, now_iso()),
            )
            self.store.record_event(
                "STOCK_RESERVED", request.order_id,
                {"product_id": request.product_id, "quantity": request.quantity},
                request.clock, connection=db,
            )
        return pb.StockReply(success=True, message="Stock reserved")

    def ReleaseStock(self, request, context):
        with self.store.connect() as db:
            reservation = db.execute("SELECT * FROM reservations WHERE order_id=?", (request.order_id,)).fetchone()
            if reservation is None:
                return pb.StockReply(success=True, message="No reservation to release")
            if reservation["state"] == "RELEASED":
                return pb.StockReply(success=True, message="Reservation already released")
            db.execute("UPDATE products SET stock=stock+? WHERE id=?",
                       (reservation["quantity"], reservation["product_id"]))
            db.execute("UPDATE reservations SET state='RELEASED' WHERE order_id=?", (request.order_id,))
            self.store.record_event(
                "STOCK_RELEASED", request.order_id,
                {"product_id": reservation["product_id"], "quantity": reservation["quantity"]},
                request.clock, connection=db,
            )
        return pb.StockReply(success=True, message="Reserved stock restored")

    def ProcessPayment(self, request, context):
        with self.store.connect() as db:
            row = db.execute("SELECT * FROM payments WHERE order_id=?", (request.order_id,)).fetchone()
            if row is None:
                success = bool(request.succeed)
                transaction_id = str(uuid.uuid4()) if success else ""
                message = "Payment approved" if success else "Payment declined by demo scenario"
                db.execute(
                    "INSERT INTO payments(order_id,amount,success,message,transaction_id,created_at) VALUES(?,?,?,?,?,?)",
                    (request.order_id, request.amount, int(success), message, transaction_id, now_iso()),
                )
                row = db.execute("SELECT * FROM payments WHERE order_id=?", (request.order_id,)).fetchone()
                self.store.record_event(
                    "PAYMENT_APPROVED" if row["success"] else "PAYMENT_DECLINED",
                    request.order_id, {"amount": request.amount, "message": row["message"]},
                    request.clock, connection=db,
                )
        return pb.PaymentReply(success=bool(row["success"]), message=row["message"],
                               transaction_id=row["transaction_id"])

    def CreateOrder(self, request, context):
        if request.quantity < 1 or request.quantity > 100:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Quantity must be between 1 and 100")
        if not request.idempotency_key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "An idempotency key is required")
        with self.store.connect() as db:
            previous = db.execute("SELECT * FROM orders WHERE idempotency_key=?",
                                  (request.idempotency_key,)).fetchone()
            if previous:
                return self._order_message(previous)

        inventory = grpc.insecure_channel(GRPC_TARGETS["inventory"])
        payment = grpc.insecure_channel(GRPC_TARGETS["payment"])
        try:
            products = pb_grpc.InventoryStub(inventory).ListProducts(pb.Empty(), timeout=4).products
        except grpc.RpcError as error:
            context.abort(grpc.StatusCode.UNAVAILABLE, f"Inventory service unavailable: {error.details()}")
        product = next((item for item in products if item.id == request.product_id), None)
        if product is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "Product not found")
        order_id = str(uuid.uuid4())
        total = round(product.price * request.quantity, 2)
        clock = self.store.next_clock()
        created = now_iso()
        try:
            with self.store.connect() as db:
                db.execute(
                    "INSERT INTO orders(id,product_id,product_name,quantity,total,status,idempotency_key,clock,created_at) "
                    "VALUES(?,?,?,?,?,'PROCESSING',?,?,?)",
                    (order_id, product.id, product.name, request.quantity, total,
                     request.idempotency_key, clock, created),
                )
        except sqlite3.IntegrityError:
            with self.store.connect() as db:
                previous = db.execute(
                    "SELECT * FROM orders WHERE idempotency_key=?", (request.idempotency_key,)
                ).fetchone()
            if previous:
                return self._order_message(previous)
            context.abort(grpc.StatusCode.ALREADY_EXISTS, "Idempotency key already exists")
        except sqlite3.Error as error:
            logger.exception("Could not persist new order")
            context.abort(grpc.StatusCode.INTERNAL, f"Could not persist order: {error}")

        outcome, detail = "FAILED", ""
        reserved = False
        try:
            stock = pb_grpc.InventoryStub(inventory).ReserveStock(
                pb.StockRequest(order_id=order_id, product_id=product.id, quantity=request.quantity,
                                clock=clock), timeout=4
            )
            if stock.success:
                reserved = True
                payment_clock = self.store.next_clock(clock)
                charge = pb_grpc.PaymentsStub(payment).ProcessPayment(
                    pb.PaymentRequest(order_id=order_id, amount=total, succeed=request.payment_success,
                                      clock=payment_clock), timeout=4
                )
                if charge.success:
                    outcome, detail = "CONFIRMED", charge.message
                else:
                    release = pb_grpc.InventoryStub(inventory).ReleaseStock(
                        pb.StockRequest(order_id=order_id, clock=self.store.next_clock(payment_clock)), timeout=4
                    )
                    outcome = "FAILED_COMPENSATED" if release.success else "COMPENSATION_REQUIRED"
                    detail = f"{charge.message}; {release.message}"
            else:
                outcome, detail = "REJECTED", stock.message
        except grpc.RpcError as error:
            detail = f"{error.code().name}: {error.details()}"
            if reserved:
                try:
                    release = pb_grpc.InventoryStub(inventory).ReleaseStock(
                        pb.StockRequest(order_id=order_id, clock=self.store.next_clock(clock)), timeout=4
                    )
                    outcome = "FAILED_COMPENSATED" if release.success else "COMPENSATION_REQUIRED"
                    detail += f"; compensation: {release.message}"
                except grpc.RpcError as compensation_error:
                    outcome = "COMPENSATION_REQUIRED"
                    detail += f"; compensation retry required: {compensation_error.details()}"
            else:
                outcome = "FAILED"
        finally:
            inventory.close()
            payment.close()

        with self.store.connect() as db:
            db.execute("UPDATE orders SET status=? WHERE id=?", (outcome, order_id))
            event = self.store.record_event(
                f"ORDER_{outcome}", order_id,
                {"order_id": order_id, "product_id": product.id, "product_name": product.name,
                 "quantity": request.quantity, "total": total, "status": outcome, "detail": detail},
                connection=db,
            )
        logger.info("Order %s finished with %s (Lamport clock %s)", order_id, outcome, event["clock"])
        with self.store.connect() as db:
            row = db.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
        return self._order_message(row)

    @staticmethod
    def _order_message(row):
        return pb.Order(
            id=row["id"], product_id=row["product_id"], product_name=row["product_name"],
            quantity=row["quantity"], total=row["total"], status=row["status"],
            idempotency_key=row["idempotency_key"], clock=row["clock"], created_at=row["created_at"],
        )

    def ListOrders(self, request, context):
        with self.store.connect() as db:
            rows = db.execute("SELECT * FROM orders ORDER BY created_at DESC LIMIT 100").fetchall()
        return pb.OrderList(orders=[self._order_message(row) for row in rows])

    def ListEvents(self, request, context):
        with self.store.connect() as db:
            rows = db.execute(
                "SELECT * FROM events ORDER BY received_at DESC LIMIT 100"
            ).fetchall()
            duplicates = db.execute("SELECT COALESCE(SUM(count),0) FROM duplicates").fetchone()[0]
        events = []
        for row in rows:
            events.append(pb.Event(
                id=row["id"], service=row["service"], event_type=row["event_type"],
                entity_id=row["entity_id"], clock=row["clock"], timestamp=row["timestamp"],
                data_json=row["data_json"],
            ))
        return pb.EventList(events=events, duplicate_count=duplicates)


def add_service(server, name: str, handlers: RpcHandlers) -> None:
    registrations = {
        "order": pb_grpc.add_OrdersServicer_to_server,
        "payment": pb_grpc.add_PaymentsServicer_to_server,
        "inventory": pb_grpc.add_InventoryServicer_to_server,
        "notification": pb_grpc.add_NotificationsServicer_to_server,
    }
    registrations[name](handlers, server)
