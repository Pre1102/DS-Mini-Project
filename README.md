# Orderly — Distributed E-Commerce Order Processing

A beginner-friendly Distributed Systems project demonstrating a small e-commerce Saga across four independent services. React + Vite provides the dashboard; a FastAPI gateway exposes HTTP/JSON and WebSocket endpoints; the gateway and services communicate synchronously over gRPC/Protocol Buffers; services publish durable asynchronous events through RabbitMQ; and each service persists its own state in a separate SQLite database.

This project deliberately uses **local processes, not Docker**. Payment is simulated; no real payment provider or customer data is involved.

## Features

- Product catalogue and stock levels stored by the Inventory service.
- Idempotent order creation with a persistent client idempotency key.
- Inventory reservation followed by simulated payment approval/decline.
- Saga compensation: if payment is declined after a reservation, Inventory restores the reserved units and the order ends as `FAILED_COMPENSATED`.
- Separate Order, Payment, Inventory, and Notification service processes, each hosting a REST health endpoint and its gRPC server.
- FastAPI gateway with a dashboard API, a health overview, and a WebSocket event stream.
- Durable RabbitMQ fan-out exchange, a persistent Order/Payment/Inventory outbox, retry-on-reconnect publishing, and idempotent notification consumption.
- Lamport logical clocks attached to events and propagated over the order Saga's gRPC calls.
- Event history and duplicate-event counts. The dashboard can replay an event to demonstrate duplicate suppression.
- Service health, inventory, order statuses, event feed, and demo scenarios in a responsive React dashboard.
- Optional Cloudflare Quick Tunnel for an external classroom demo.

## Architecture

```text
React/Vite :5173  -- HTTP + WebSocket -->  FastAPI Gateway :8000
                                                |
                       synchronous gRPC / Protocol Buffers
                  +-------------+---------------+-------------+
                  |             |               |             |
             Order :50051  Payment :50052  Inventory :50053  Notification :50054
                  |             |               |             ^
                  +------ durable event outboxes ------------+
                                        RabbitMQ :5672

Each service has its own REST health port (:8001-:8004) and SQLite file:
data/order.db, payment.db, inventory.db, notification.db
```

The Order service orchestrates the Saga: it checks the catalogue and creates an idempotent order, requests an Inventory reservation, then requests a simulated Payment. A failed payment triggers a stock release. Events are written to each producing service's SQLite outbox before being asynchronously delivered; if RabbitMQ is temporarily unavailable, the publisher retries the pending outbox after the broker returns. A process crash after publishing but before marking an outbox row sent can cause redelivery; Notification's unique event ID makes that delivery idempotent.

Lamport clocks advance on local events and incorporate the latest remote clock on gRPC calls and event receipt. They express causal ordering, not wall-clock time; the UI shows the logical clock alongside event timestamps.

## Prerequisites (Windows)

Install:

1. **Python 3.11 or newer**
2. **Node.js 20 or newer** (npm is included)
3. **Erlang/OTP and RabbitMQ Server** (RabbitMQ is not a Python package and runs as a local Windows service)
4. Optional: **cloudflared** for a public demo URL

Install RabbitMQ and its Erlang prerequisite from the official installers:

- Erlang/OTP: <https://www.erlang.org/downloads>
- RabbitMQ: <https://www.rabbitmq.com/docs/install-windows>

Install Erlang/OTP and RabbitMQ Server using the official Windows installers linked above. Open a new PowerShell after installation. RabbitMQ normally registers a Windows service. Start it and check its status:

```powershell
Get-Service RabbitMQ
Start-Service RabbitMQ
Get-Service RabbitMQ
```

If you do not have administrator access, use RabbitMQ's official Windows archive and run the broker as a local foreground process instead of installing a Windows service. After installing a compatible Erlang/OTP version and extracting the Windows archive, open a **separate PowerShell window** and set the installation paths:

```powershell
$env:ERLANG_HOME = Join-Path $env:LOCALAPPDATA "Programs\ErlangOTP28"
$env:Path = "$env:ERLANG_HOME\bin;$env:Path"
$env:RABBITMQ_BASE = Join-Path $env:LOCALAPPDATA "RabbitMQDemo"
Set-Location (Join-Path $env:LOCALAPPDATA "Programs\rabbitmq_server-4.3.6\rabbitmq_server-4.3.6\sbin")
.\rabbitmq-server.bat
```

Keep that terminal open while using the demo. Verify that the broker is listening with `Test-NetConnection localhost -Port 5672`, or use `.\rabbitmq-diagnostics.bat -q ping` in the same `sbin` folder. The demo's health endpoint and dashboard report RabbitMQ as online when the broker is reachable.

The application uses the default local AMQP address `amqp://guest:guest@localhost/`. The RabbitMQ management UI is optional; enable it from the RabbitMQ `sbin` directory with `rabbitmq-plugins.bat enable rabbitmq_management`, then browse to `http://localhost:15672` (default login `guest` / `guest`).

## Setup

Run these commands from the project root in PowerShell:

```powershell
Set-Location "C:\path\to\DS Mini project\backend"
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
python generate_proto.py
```

If PowerShell blocks virtual-environment activation, either permit it for the current user (`Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`) or invoke the environment's Python directly as `.\.venv\Scripts\python.exe`.

Install frontend packages in another PowerShell:

```powershell
Set-Location "C:\path\to\DS Mini project\frontend"
npm install
```

`generate_proto.py` generates the Python gRPC modules from `backend\proto\order_system.proto`. Rerun it after changing the schema. SQLite databases and their seed products are created automatically on first start beneath `backend\data`.

## Run the system

Keep RabbitMQ running. Start each command below in its **own PowerShell window**. Every backend window must use the backend folder and activate the virtual environment. The four services are separate processes, even though each process hosts both its REST health endpoint and its gRPC server.

**1. Order service**

```powershell
Set-Location "C:\path\to\DS Mini project\backend"
.\.venv\Scripts\Activate.ps1
$env:SERVICE_NAME = "order"
python -m uvicorn app.main:app --host 0.0.0.0 --port 8001
```

**2. Payment service**

```powershell
Set-Location "C:\path\to\DS Mini project\backend"
.\.venv\Scripts\Activate.ps1
$env:SERVICE_NAME = "payment"
python -m uvicorn app.main:app --host 0.0.0.0 --port 8002
```

**3. Inventory service**

```powershell
Set-Location "C:\path\to\DS Mini project\backend"
.\.venv\Scripts\Activate.ps1
$env:SERVICE_NAME = "inventory"
python -m uvicorn app.main:app --host 0.0.0.0 --port 8003
```

**4. Notification service**

```powershell
Set-Location "C:\path\to\DS Mini project\backend"
.\.venv\Scripts\Activate.ps1
$env:SERVICE_NAME = "notification"
python -m uvicorn app.main:app --host 0.0.0.0 --port 8004
```

**5. API gateway**

```powershell
Set-Location "C:\path\to\DS Mini project\backend"
.\.venv\Scripts\Activate.ps1
python -m uvicorn app.gateway:app --host 0.0.0.0 --port 8000
```

**6. React dashboard**

```powershell
Set-Location "C:\path\to\DS Mini project\frontend"
npm run dev
```

Open <http://localhost:5173>. Vite proxies `/api` and `/ws` to the gateway on port 8000. The gateway's interactive API docs are at <http://localhost:8000/docs>. Each service has a health endpoint at `http://localhost:8001/health` through `8004/health`.

> Run a single instance of each service for the default local ports and SQLite files. To clear the demo orders, payments, reservations, and event history and restore the original stock, first stop the services and then follow **Reset demo data** below.

### Reset demo data

From `backend`, with the project's virtual environment active and all four service processes stopped:

```powershell
python reset_demo_data.py --yes
```

This clears records and outboxes from the four service databases, resets Lamport clocks, and restores the original product stock (`12`, `8`, `20`, and `10` units). The `--yes` flag is required because the operation is destructive. If RabbitMQ is running, the script also purges queued notification events. If RabbitMQ was stopped, no broker queue can be cleared; start the broker and run the reset command again if it contains messages from a previous demo.

## Demo scenarios

### 1. Normal order

In the dashboard, select a product and quantity, leave **Simulate payment failure** off, and choose **Place order**. The order service reserves stock via gRPC, the payment service approves the simulated payment, and the order becomes `CONFIRMED`. Watch stock, the order table, and the live RabbitMQ event timeline update.

Equivalent API request:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8000/api/orders `
  -ContentType "application/json" `
  -Body '{"product_id":"p-100","quantity":1,"payment_success":true,"idempotency_key":"demo-normal-001"}'
```

Submitting the same `idempotency_key` again returns the original order without reserving or charging again.

### 2. Payment failure and compensation

Turn on **Simulate payment failure** in the dashboard. Inventory reserves first; Payment declines; Order asks Inventory to release the reservation. The order is `FAILED_COMPENSATED`, and stock returns to its prior quantity.

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8000/api/orders `
  -ContentType "application/json" `
  -Body '{"product_id":"p-200","quantity":1,"payment_success":false,"idempotency_key":"demo-failure-001"}'
```

### 3. Service downtime and recovery

Stop the Inventory service by pressing **Ctrl+C** in its terminal. The dashboard marks it offline and a new order request reports an unavailable service rather than silently succeeding. Restart the Inventory command above; health recovers automatically and the dashboard retries its status polling. Submit a new order using a fresh idempotency key.

### 4. Duplicate-event handling

Create an order and wait for its events to show up. Click **Replay** on an Order service event in the event timeline. The gateway republishes the same event ID through RabbitMQ; Notification recognizes the ID in its SQLite event log, acknowledges but does not insert it a second time, and increments the duplicate counter. The dashboard displays the updated count.

## API quick reference

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/health` | gRPC health of four services plus RabbitMQ broker connectivity |
| `GET` | `/api/products` | Inventory catalogue and current stock |
| `GET` | `/api/orders` | Recent orders and Saga statuses |
| `POST` | `/api/orders` | Create an order (`product_id`, `quantity`, optional `payment_success`, optional `idempotency_key`) |
| `GET` | `/api/events` | Notification event history and duplicate count |
| `POST` | `/api/demo/duplicate-event/{event_id}` | Replay a known Order outbox event |
| `GET` | `/api/demo/scenarios` | Built-in scenario descriptions |
| `WS` | `/ws/events` | Live event history snapshots for the dashboard |

The dashboard's **Dashboard Live** indicator reports only the browser-to-gateway WebSocket connection. The **Services** page reports RabbitMQ separately; when the broker is offline, the other services continue accepting supported requests and retain events in their SQLite outboxes for retry.

## Project layout

```text
backend/
  app/
    config.py             Environment and port configuration
    store.py              Per-service SQLite schema, clocks, and outbox
    rpc_services.py       gRPC handlers and Order Saga orchestration
    event_bus.py          RabbitMQ publisher, consumer, and event replay
    main.py               Independent service REST/gRPC process
    gateway.py            FastAPI REST/WebSocket API gateway
  proto/order_system.proto
  generate_proto.py
  requirements.txt
  .env.example
  tests/test_store.py
frontend/
  src/App.jsx
  src/styles.css
  vite.config.js
```

## Tests

With the backend virtual environment active:

```powershell
Set-Location "C:\path\to\DS Mini project\backend"
.\.venv\Scripts\Activate.ps1
python -m unittest discover -s tests -v
```

Build the frontend:

```powershell
Set-Location "C:\path\to\DS Mini project\frontend"
npm run build
```

## Optional Cloudflare Tunnel

Install `cloudflared` from <https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/>. Keep the dashboard, gateway, and all services running, then start a Quick Tunnel:

```powershell
cloudflared tunnel --url http://localhost:5173
```

Open the temporary `https://*.trycloudflare.com` URL printed by cloudflared. The public browser reaches Vite; its API and WebSocket proxy remain connected to the local gateway. Quick Tunnels are for temporary demos and may change URL or disconnect. Anyone with the URL can use this unauthenticated demo, so do not expose it with real customer/payment data. For a stable address, use a named Cloudflare Tunnel configured in your Cloudflare account and point it at the local Vite port.
