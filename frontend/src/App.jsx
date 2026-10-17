import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Activity, ArrowDownRight, ArrowRight, ArrowUpRight, Boxes, Check, CircleCheck, CircleX,
  Clock3, CreditCard, Layers3, LayoutDashboard, Package, Radio, RefreshCw, RotateCcw,
  Server, ShoppingBag, ShoppingCart, Wifi, WifiOff, Zap,
} from "lucide-react";

const PAGES = [
  { id: "overview", label: "Overview", icon: LayoutDashboard },
  { id: "products", label: "Products", icon: Package },
  { id: "create-order", label: "Create order", icon: ShoppingCart },
  { id: "orders", label: "Orders", icon: ShoppingBag },
  { id: "events", label: "Events", icon: Clock3 },
  { id: "services", label: "Services", icon: Activity },
];

const PAGE_CONTENT = {
  overview: ["Your store at a glance", "A clear overview of orders, inventory, and the services behind your store."],
  products: ["Products & inventory", "Browse the catalogue and see the stock held by Inventory."],
  "create-order": ["Create an order", "Choose a product and follow its journey through the system."],
  orders: ["Orders", "Every order and its current Saga workflow outcome."],
  events: ["Event timeline", "Live messages received from the RabbitMQ event stream."],
  services: ["Services & connections", "Check each process and understand how the parts communicate."],
};

const money = (amount) => new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format(amount || 0);
const relativeTime = (value) => {
  if (!value) return "just now";
  const seconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  return `${Math.floor(seconds / 3600)}h ago`;
};
const titleCase = (value = "") => value.replaceAll("_", " ").toLowerCase().replace(/\b\w/g, (letter) => letter.toUpperCase());

async function getJson(path) {
  const response = await fetch(path);
  if (!response.ok) {
    const result = await response.json().catch(() => ({}));
    throw new Error(result.detail || `Request failed (${response.status})`);
  }
  return response.json();
}

function pageFromHash() {
  const page = window.location.hash.slice(1);
  return PAGES.some((item) => item.id === page) ? page : "overview";
}

function App() {
  const [products, setProducts] = useState([]);
  const [orders, setOrders] = useState([]);
  const [events, setEvents] = useState([]);
  const [duplicateCount, setDuplicateCount] = useState(0);
  const [services, setServices] = useState({});
  const [page, setPage] = useState(pageFromHash);
  const [selected, setSelected] = useState("");
  const [quantity, setQuantity] = useState(1);
  const [failPayment, setFailPayment] = useState(false);
  const [busy, setBusy] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [live, setLive] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(null);

  const refresh = useCallback(async () => {
    setRefreshing(true);
    try {
      const results = await Promise.allSettled([
          getJson("/api/products"),
          getJson("/api/orders"),
          getJson("/api/events"),
          getJson("/api/health"),
      ]);
      const [productResult, orderResult, eventResult, healthResult] = results;
      if (productResult.status === "fulfilled") {
        setProducts(productResult.value);
        setSelected((current) => current || productResult.value[0]?.id || "");
      }
      if (orderResult.status === "fulfilled") setOrders(orderResult.value);
      if (eventResult.status === "fulfilled") {
        setEvents(eventResult.value.events || []);
        setDuplicateCount(eventResult.value.duplicate_count || 0);
      }
      if (healthResult.status === "fulfilled") setServices(healthResult.value.services || {});
      const failures = results
        .filter((result) => result.status === "rejected")
        .map((result) => result.reason?.message || "A dashboard service is temporarily unavailable.");
      setLastUpdated(new Date());
      setError(failures.length ? [...new Set(failures)].join(" · ") : "");
    } finally {
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    refresh();
    const timer = window.setInterval(refresh, 6000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  useEffect(() => {
    const syncPage = () => setPage(pageFromHash());
    window.addEventListener("hashchange", syncPage);
    return () => window.removeEventListener("hashchange", syncPage);
  }, []);

  useEffect(() => {
    let socket;
    let retryTimer;
    let closed = false;
    const connect = () => {
      const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
      socket = new WebSocket(`${protocol}//${window.location.host}/ws/events`);
      socket.onopen = () => setLive(true);
      socket.onmessage = (message) => {
        const payload = JSON.parse(message.data);
        if (payload.type === "events") {
          setEvents(payload.events || []);
          setDuplicateCount(payload.duplicate_count || 0);
          setLastUpdated(new Date());
        }
      };
      socket.onclose = () => {
        setLive(false);
        if (!closed) retryTimer = window.setTimeout(connect, 2000);
      };
      socket.onerror = () => socket.close();
    };
    connect();
    return () => {
      closed = true;
      window.clearTimeout(retryTimer);
      socket?.close();
    };
  }, []);

  const selectedProduct = products.find((product) => product.id === selected);
  const confirmedOrders = orders.filter((order) => order.status === "CONFIRMED").length;
  const inventoryUnits = products.reduce((sum, product) => sum + product.stock, 0);
  const onlineServices = ["order", "payment", "inventory", "notification"]
    .filter((name) => services[name]?.status === "ok").length;
  const brokerConnected = services.rabbitmq?.status === "ok";
  const coreServicesOnline = ["order", "payment", "inventory"].every((name) => services[name]?.status === "ok");
  const sortedEvents = useMemo(() => [...events].sort((a, b) => b.clock - a.clock), [events]);

  function navigateTo(nextPage) {
    setError("");
    setNotice("");
    if (page === nextPage) return;
    window.location.hash = nextPage;
  }

  async function placeOrder(event) {
    event.preventDefault();
    if (!selected) return;
    setBusy(true);
    setError("");
    setNotice("");
    try {
      const response = await fetch("/api/orders", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ product_id: selected, quantity: Number(quantity), payment_success: !failPayment }),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.detail || "Could not create order.");
      setNotice(`Order ${result.id.slice(0, 8)} · ${titleCase(result.status)}. See it in Orders.`);
      await refresh();
    } catch (requestError) {
      setError(requestError.message);
    } finally {
      setBusy(false);
    }
  }

  async function replayEvent(eventId) {
    setError("");
    setNotice("");
    try {
      const response = await fetch(`/api/demo/duplicate-event/${eventId}`, { method: "POST" });
      const result = await response.json();
      if (!response.ok) throw new Error(result.detail || "Could not replay event.");
      setNotice("Event replayed. Notification will recognize and ignore the duplicate.");
      window.setTimeout(refresh, 800);
    } catch (requestError) {
      setError(requestError.message);
    }
  }

  const pageContent = {
    overview: <Overview
      orders={orders}
      products={products}
      services={services}
      events={sortedEvents}
      confirmedOrders={confirmedOrders}
      inventoryUnits={inventoryUnits}
      onlineServices={onlineServices}
      duplicateCount={duplicateCount}
      live={live}
      brokerConnected={brokerConnected}
      onNavigate={navigateTo}
    />,
    products: <ProductsPage products={products} onShop={(productId) => {
      if (productId) setSelected(productId);
      navigateTo("create-order");
    }} />,
    "create-order": <CreateOrderPage
      products={products}
      selected={selected}
      setSelected={setSelected}
      selectedProduct={selectedProduct}
      quantity={quantity}
      setQuantity={setQuantity}
      failPayment={failPayment}
      setFailPayment={setFailPayment}
      busy={busy}
      coreServicesOnline={coreServicesOnline}
      brokerConnected={brokerConnected}
      onSubmit={placeOrder}
    />,
    orders: <OrdersPage orders={orders} onCreateOrder={() => navigateTo("create-order")} />,
    events: <EventsPage events={sortedEvents} duplicateCount={duplicateCount} onReplay={replayEvent} live={live} brokerConnected={brokerConnected} />,
    services: <ServicesPage services={services} live={live} />,
  }[page];
  const [heading, description] = PAGE_CONTENT[page];

  return (
    <main className="page-shell">
      <header className="topbar">
        <a className="brand" href="#overview" aria-label="Orderly overview" onClick={() => navigateTo("overview")}>
          <span className="brand-mark"><Layers3 size={21} /></span>
          <span>orderly<span className="brand-dot">.</span></span>
        </a>
        <div className="topbar-right">
          <div className={`live-pill ${live ? "is-live" : ""}`} title="WebSocket connection from this dashboard to the API gateway">
            <span className="live-dot" />{live ? "DASHBOARD LIVE" : "RECONNECTING"}
          </div>
          <div className={`broker-pill ${brokerConnected ? "broker-online" : "broker-offline"}`} title="RabbitMQ connectivity for service-to-service event delivery">
            <span />{brokerConnected ? "BROKER ONLINE" : "BROKER OFFLINE"}
          </div>
          <button className="refresh-button" onClick={refresh} disabled={refreshing}
            aria-label="Refresh dashboard data" title="Fetch the latest orders, products, events, and service status">
            <RefreshCw className={refreshing ? "spin" : ""} size={15} />
            <span>{refreshing ? "Refreshing" : "Refresh data"}</span>
          </button>
        </div>
      </header>

      <nav className="primary-nav" aria-label="Main navigation">
        {PAGES.map(({ id, label, icon: Icon }) => (
          <button key={id} className={`nav-item ${page === id ? "nav-active" : ""}`}
            onClick={() => navigateTo(id)} aria-current={page === id ? "page" : undefined}>
            <Icon size={16} /><span>{label}</span>
            {id === "orders" && orders.length > 0 && <span className="nav-count">{orders.length}</span>}
          </button>
        ))}
      </nav>

      <section className="intro page-intro">
        <div>
          <div className="eyebrow"><span className="eyebrow-line" /> DISTRIBUTED SYSTEMS DEMO</div>
          <h1>{heading}</h1>
          <p className="intro-copy">{description}</p>
        </div>
        <div className="updated"><span className="status-pulse" /> Synced {relativeTime(lastUpdated)}</div>
      </section>

      {(error || notice) && <div className={`toast ${error ? "toast-error" : "toast-success"}`}>
        {error ? <CircleX size={17} /> : <CircleCheck size={17} />}
        <span>{error || notice}</span>
        <button onClick={() => { setError(""); setNotice(""); }} aria-label="Dismiss">×</button>
      </div>}

      <div className="page-content" key={page}>{pageContent}</div>

      <footer className="footer"><span>ORDERLY <i>·</i> DISTRIBUTED E-COMMERCE DEMO</span><span>FastAPI <i>·</i> gRPC <i>·</i> RabbitMQ <i>·</i> SQLite</span></footer>
    </main>
  );
}

function Overview({ orders, products, services, events, confirmedOrders, inventoryUnits, onlineServices, duplicateCount, live, brokerConnected, onNavigate }) {
  const recent = orders.slice(0, 4);
  const latestEvents = events.slice(0, 4);
  return <>
    <section className="metric-grid" aria-label="System overview">
      <Metric icon={<ShoppingBag />} label="Total orders" value={orders.length} foot={`${confirmedOrders} confirmed`} accent="violet" onClick={() => onNavigate("orders")} />
      <Metric icon={<Boxes />} label="Units in stock" value={inventoryUnits} foot={`${products.length} products tracked`} accent="blue" onClick={() => onNavigate("products")} />
      <Metric icon={<Activity />} label="Services online" value={`${onlineServices}/4`} foot="Independent processes" accent="green" onClick={() => onNavigate("services")} />
      <Metric icon={<Radio />} label="Events received" value={events.length} foot={`${duplicateCount} duplicates ignored`} accent="orange" onClick={() => onNavigate("events")} />
    </section>

    <section className="overview-grid">
      <div className="panel overview-panel">
        <PanelHeading icon={<ShoppingBag size={18} />} title="Recent orders" subtitle="Your latest order activity" />
        {recent.length ? <div className="overview-order-list">
          {recent.map((order) => <div className="overview-order-row" key={order.id}>
            <span className="overview-order-icon"><Package size={16} /></span>
            <span className="overview-order-name"><strong>{order.product_name}</strong><small>#{order.id.slice(0, 8)} · Qty {order.quantity}</small></span>
            <span className="overview-order-total">{money(order.total)}</span>
            <StatusBadge status={order.status} />
          </div>)}
        </div> : <Empty>No orders yet. Create one to see it here.</Empty>}
        <button className="text-link" onClick={() => onNavigate("orders")}>View all orders <ArrowRight size={14} /></button>
      </div>

      <div className="panel overview-panel">
        <PanelHeading icon={<Activity size={18} />} title="System activity" subtitle="Quick look at the live event feed" />
        {latestEvents.length ? <EventList events={latestEvents} /> : <Empty>{brokerConnected ? "No events yet. Place an order to start the feed." : "RabbitMQ is offline. Events will queue until it reconnects."}</Empty>}
        <button className="text-link" onClick={() => onNavigate("events")}>Open event timeline <ArrowRight size={14} /></button>
      </div>

      <div className="panel getting-started-panel">
        <div className="getting-started-copy">
          <span className="getting-started-kicker">GETTING STARTED</span>
          <h2>What would you like to do?</h2>
          <p>Each screen focuses on one part of the order-processing system.</p>
        </div>
        <div className="quick-links">
          <QuickLink icon={<Package size={17} />} title="Browse products" description="Check prices and stock" onClick={() => onNavigate("products")} />
          <QuickLink icon={<ShoppingCart size={17} />} title="Place an order" description="Try success or payment failure" onClick={() => onNavigate("create-order")} />
          <QuickLink icon={<Server size={17} />} title="Check services" description={`${onlineServices}/4 services · RabbitMQ ${brokerConnected ? "online" : "offline"}`} onClick={() => onNavigate("services")} />
        </div>
      </div>
    </section>
  </>;
}

function ProductsPage({ products, onShop }) {
  return <>
    <section className="metric-grid product-metrics">
      <Metric icon={<Package />} label="Products" value={products.length} foot="Available in catalogue" accent="violet" />
      <Metric icon={<Boxes />} label="Units on hand" value={products.reduce((sum, item) => sum + item.stock, 0)} foot="Across all products" accent="blue" />
      <Metric icon={<Check />} label="In stock" value={products.filter((item) => item.stock > 0).length} foot="Products ready to order" accent="green" />
      <Metric icon={<ShoppingCart />} label="Ordering" value="Ready" foot="Select a product to begin" accent="orange" onClick={onShop} />
    </section>
    <section className="panel catalogue-page-panel">
      <PanelHeading icon={<Package size={18} />} title="Product catalogue" subtitle="Stock is reserved by the Inventory service during order processing" tag={`${products.length} PRODUCTS`} />
      {products.length ? <div className="catalogue-grid">{products.map((product, index) => (
        <article className="catalogue-card" key={product.id}>
          <div className={`catalogue-art art-${index % 4}`}><Package size={29} /></div>
          <div className="catalogue-card-info"><span className="catalogue-id">{product.id}</span><h3>{product.name}</h3><strong>{money(product.price)}</strong></div>
          <div className="catalogue-card-footer"><span className={product.stock > 0 ? "stock-available" : "stock-empty"}><i />{product.stock > 0 ? `${product.stock} units available` : "Out of stock"}</span><button className="small-action" disabled={!product.stock} onClick={() => onShop(product.id)}>Order <ArrowUpRight size={14} /></button></div>
        </article>
      ))}</div> : <Empty>Waiting for Inventory service…</Empty>}
    </section>
  </>;
}

function CreateOrderPage({ products, selected, setSelected, selectedProduct, quantity, setQuantity, failPayment, setFailPayment, busy, coreServicesOnline, brokerConnected, onSubmit }) {
  return <section className="create-order-layout">
    <div className="panel order-panel order-page-panel">
      <PanelHeading icon={<ShoppingCart size={18} />} title="Order details" subtitle="Pick an item, quantity, and payment scenario" />
      <form onSubmit={onSubmit}>
        <label className="field-label" htmlFor="product">PRODUCT</label>
        <select id="product" value={selected} onChange={(event) => setSelected(event.target.value)} required>
          {products.map((product) => <option value={product.id} key={product.id} disabled={!product.stock}>{product.name} — {money(product.price)} ({product.stock} available)</option>)}
        </select>
        <div className="form-split">
          <div className="quantity-field"><label className="field-label" htmlFor="quantity">QUANTITY</label>
            <div className="quantity-control">
              <button type="button" onClick={() => setQuantity((value) => Math.max(1, Number(value) - 1))} aria-label="Decrease quantity">−</button>
              <input id="quantity" type="number" min="1" max={Math.min(100, selectedProduct?.stock || 100)} required value={quantity} onChange={(event) => setQuantity(event.target.value)} />
              <button type="button" onClick={() => setQuantity((value) => Math.min(100, Number(value) + 1))} aria-label="Increase quantity">+</button>
            </div>
          </div>
          <div className="total-field"><span className="field-label">ESTIMATED TOTAL</span><strong>{money((selectedProduct?.price || 0) * (Number(quantity) || 0))}</strong></div>
        </div>
        {selectedProduct && Number(quantity) > selectedProduct.stock && <p className="form-hint">Only {selectedProduct.stock} units are currently available.</p>}
        <button type="button" className={`scenario-toggle ${failPayment ? "scenario-on" : ""}`} onClick={() => setFailPayment((value) => !value)} aria-pressed={failPayment}>
          <span className={`toggle-track ${failPayment ? "toggle-active" : ""}`}><span /></span>
          <span className="scenario-text"><strong>Simulate payment failure</strong><small>Order Saga restores stock after the payment is declined.</small></span>
          <CreditCard size={18} className="scenario-icon" />
        </button>
        <button className="submit-button" disabled={busy || !products.length || !selectedProduct?.stock || Number(quantity) < 1 || Number(quantity) > (selectedProduct?.stock || 0) || !coreServicesOnline}>
          {busy ? <><RefreshCw className="spin" size={17} /> Processing order…</> : <><Zap size={17} /> Place order <ArrowUpRight size={16} /></>}
        </button>
        {!coreServicesOnline && <p className="form-hint">Start the Order, Payment, and Inventory services to enable ordering.</p>}
        {coreServicesOnline && !brokerConnected && <p className="broker-notice"><Radio size={14} /> RabbitMQ is offline. Orders still work; events will be queued for delivery when it reconnects.</p>}
      </form>
    </div>

    <aside className="panel order-explainer">
      <div className="explainer-icon"><Layers3 size={21} /></div>
      <span className="getting-started-kicker">WHAT HAPPENS NEXT</span>
      <h2>One order, four services.</h2>
      <p>The Order service coordinates the work. If payment fails, the Saga restores reserved stock.</p>
      <ol className="saga-steps">
        <li><span>1</span><div><strong>Reserve stock</strong><small>Inventory checks and holds the units.</small></div></li>
        <li><span>2</span><div><strong>Process payment</strong><small>Payment approves or declines the demo transaction.</small></div></li>
        <li><span>3</span><div><strong>Confirm or compensate</strong><small>Order records the result; RabbitMQ delivers the event.</small></div></li>
      </ol>
      <span className="clock-note"><Clock3 size={14} /> Each event carries a Lamport logical clock.</span>
    </aside>
  </section>;
}

function OrdersPage({ orders, onCreateOrder }) {
  return <section className="panel full-page-panel">
    <PanelHeading icon={<ShoppingBag size={18} />} title="All orders" subtitle="The Order service records the outcome of each Saga" tag={`${orders.length} TOTAL`} />
    {orders.length ? <OrdersTable orders={orders} /> : <Empty>No orders yet. <button className="inline-link" onClick={onCreateOrder}>Create your first order</button></Empty>}
  </section>;
}

function EventsPage({ events, duplicateCount, onReplay, live, brokerConnected }) {
  return <>
    <section className="event-summary-grid">
      <Metric icon={<Radio />} label="Events received" value={events.length} foot="Stored by Notification service" accent="violet" />
      <Metric icon={<Clock3 />} label="Ordering" value="Lamport" foot="Logical clock shows causal sequence" accent="blue" />
      <Metric icon={<RotateCcw />} label="Duplicate messages" value={duplicateCount} foot="Seen and safely ignored" accent="green" />
      <Metric icon={brokerConnected ? <Radio /> : <WifiOff />} label="RabbitMQ broker" value={brokerConnected ? "Connected" : "Offline"} foot={live ? "Dashboard WebSocket connected" : "Dashboard reconnecting"} accent="orange" />
    </section>
    <section className="panel full-page-panel events-page-panel">
      <PanelHeading icon={<Clock3 size={18} />} title="Event history" subtitle="Sorted by Lamport clock · newest first" tag={<><span className={brokerConnected ? "event-live-dot" : "event-offline-dot"} />{brokerConnected ? "BROKER CONNECTED" : "WAITING FOR RABBITMQ"}</>} />
      {events.length ? <EventList events={events} onReplay={onReplay} showService /> : <Empty>{brokerConnected ? "No events yet. Place an order to start the feed." : "RabbitMQ is offline. Events stay safely queued and will appear when the broker reconnects."}</Empty>}
      <div className="event-footer"><span><span className={brokerConnected ? "rabbit-indicator" : "rabbit-offline-indicator"} /> {brokerConnected ? "RabbitMQ fan-out stream connected" : "RabbitMQ disconnected · publisher retries active"}</span><span>Duplicate messages ignored: <strong>{duplicateCount}</strong></span></div>
    </section>
  </>;
}

function ServicesPage({ services, live }) {
  const details = [
    { name: "order", role: "Coordinates the Saga and owns order records.", restPort: 8001, grpcPort: 50051, icon: ShoppingBag },
    { name: "payment", role: "Simulates payment approval and decline.", restPort: 8002, grpcPort: 50052, icon: CreditCard },
    { name: "inventory", role: "Owns the catalogue, stock, and reservations.", restPort: 8003, grpcPort: 50053, icon: Boxes },
    { name: "notification", role: "Consumes RabbitMQ events and ignores duplicates.", restPort: 8004, grpcPort: 50054, icon: Radio },
  ];
  const brokerConnected = services.rabbitmq?.status === "ok";
  return <>
    <section className="service-overview-bar">
      <div className="service-overview-copy"><span className="getting-started-kicker">SYSTEM STATUS</span><strong>{details.filter(({ name }) => services[name]?.status === "ok").length} of 4 services online</strong></div>
      <span className={`service-stream-state ${live ? "stream-connected" : ""}`}><i />{live ? "Dashboard WebSocket connected" : "Dashboard WebSocket reconnecting"}</span>
    </section>
    <section className="service-detail-grid">{details.map(({ name, role, restPort, grpcPort, icon: Icon }) => {
      const online = services[name]?.status === "ok";
      return <article className="panel service-detail-card" key={name}>
        <div className="service-detail-heading"><span className={`service-icon service-${name}`}><Icon size={19} /></span><span><h2>{titleCase(name)}</h2><p>{role}</p></span><span className={`service-status ${online ? "online" : "offline"}`}><i />{online ? "Online" : "Offline"}</span></div>
        <div className="service-ports"><span><span>REST health</span><code>:{restPort}/health</code></span><span><span>gRPC</span><code>:{grpcPort}</code></span></div>
        <div className="service-database"><Boxes size={14} /> Separate SQLite database <strong>{name}.db</strong></div>
      </article>;
    })}
      <article className="panel service-detail-card broker-service-card">
        <div className="service-detail-heading"><span className="service-icon service-notification"><Radio size={19} /></span><span><h2>RabbitMQ broker</h2><p>Durable fan-out event exchange and asynchronous delivery.</p></span><span className={`service-status ${brokerConnected ? "online" : "offline"}`}><i />{brokerConnected ? "Online" : "Offline"}</span></div>
        <div className="service-ports"><span><span>AMQP</span><code>localhost:5672</code></span><span><span>Exchange</span><code>order_events</code></span></div>
        <div className="service-database"><RefreshCw size={14} /> Pending events retry automatically after recovery.</div>
      </article>
    </section>
    <section className="panel communication-panel">
      <PanelHeading icon={<Layers3 size={18} />} title="How the services communicate" subtitle="Each service owns its database; the gateway is the browser's single entry point." />
      <div className="communication-flow">
        <span className="flow-node flow-gateway"><LayoutDashboard size={17} /><strong>React dashboard</strong><small>HTTP + WebSocket</small></span>
        <ArrowRight className="flow-arrow" size={18} />
        <span className="flow-node flow-gateway"><Server size={17} /><strong>FastAPI gateway</strong><small>REST API</small></span>
        <ArrowRight className="flow-arrow" size={18} />
        <span className="flow-node flow-grpc"><Zap size={17} /><strong>gRPC services</strong><small>Synchronous calls</small></span>
        <span className="flow-down">↓</span>
        <span className="flow-node flow-rabbit"><Radio size={17} /><strong>RabbitMQ events</strong><small>Persistent outbox · retries · deduplication</small></span>
      </div>
    </section>
  </>;
}

function ProductsPageContent() {
  return null;
}

function OrdersTable({ orders }) {
  return <div className="table-wrap">
    <table>
      <thead><tr><th>ORDER</th><th>PRODUCT</th><th>AMOUNT</th><th>STATUS</th><th>LAMPORT CLOCK</th><th>CREATED</th></tr></thead>
      <tbody>{orders.map((order) => <tr key={order.id}>
        <td><span className="order-id">#{order.id.slice(0, 8)}</span></td>
        <td><span className="order-product">{order.product_name}</span><small>Qty {order.quantity}</small></td>
        <td className="amount">{money(order.total)}</td>
        <td><StatusBadge status={order.status} /></td>
        <td><span className="lamport-tag">L{order.clock}</span></td>
        <td className="time-cell" title={order.created_at}>{relativeTime(order.created_at)}</td>
      </tr>)}</tbody>
    </table>
  </div>;
}

function EventList({ events, onReplay, showService = false }) {
  return <div className="event-list">
    {events.map((event) => <article className="event-row" key={event.id}>
      <span className={`event-marker marker-${(event.service || "").toLowerCase()}`}><ArrowDownRight size={15} /></span>
      <div className="event-copy">
        <strong>{titleCase(event.event_type)}</strong>
        <span>{showService ? `${titleCase(event.service)} service · ` : ""}{event.entity_id.slice(0, 8)}{event.timestamp ? ` · ${new Date(event.timestamp).toLocaleTimeString()}` : ""}</span>
      </div>
      <span className="lamport-tag">L{event.clock}</span>
      <span className="event-time">{relativeTime(event.timestamp)}</span>
      {event.service === "order" && onReplay && <button className="replay-button" title="Replay this event to demo duplicate handling" onClick={() => onReplay(event.id)}><RotateCcw size={15} /><span>Replay</span></button>}
    </article>)}
  </div>;
}

function QuickLink({ icon, title, description, onClick }) {
  return <button className="quick-link" onClick={onClick}><span className="quick-link-icon">{icon}</span><span><strong>{title}</strong><small>{description}</small></span><ArrowRight size={15} /></button>;
}

function Metric({ icon, label, value, foot, accent, onClick }) {
  const Tag = onClick ? "button" : "div";
  return <Tag className={`metric-card ${onClick ? "metric-action" : ""}`} onClick={onClick}>
    <div className={`metric-icon metric-${accent}`}>{icon}</div>
    <div className="metric-label">{label}</div>
    <div className="metric-value">{value}</div>
    <div className="metric-foot">{foot}</div>
    <span className={`metric-glow glow-${accent}`} />
  </Tag>;
}

function PanelHeading({ icon, title, subtitle, tag }) {
  return <div className="panel-heading">
    <div className="heading-icon">{icon}</div>
    <div className="heading-copy"><h2>{title}</h2><p>{subtitle}</p></div>
    {tag && <span className="panel-tag">{tag}</span>}
  </div>;
}

function StatusBadge({ status }) {
  const good = status === "CONFIRMED";
  const failed = status.includes("FAILED") || status === "REJECTED" || status === "COMPENSATION_REQUIRED";
  return <span className={`status-badge ${good ? "badge-good" : failed ? "badge-failed" : "badge-pending"}`}>
    {good ? <CircleCheck size={13} /> : failed ? <CircleX size={13} /> : <RefreshCw size={12} />}
    {titleCase(status)}
  </span>;
}

function Empty({ children }) {
  return <div className="empty-state"><span><Package size={19} /></span>{children}</div>;
}

export default App;
