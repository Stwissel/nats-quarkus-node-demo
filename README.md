# NATS JetStream · Quarkus · Node/TypeScript — messaging demo

A three-node Docker Compose sample showing publish/subscribe, queue groups,
request/reply and JetStream persistence over [NATS](https://nats.io), with a web UI
to exercise every function.

| Container | Role |
|---|---|
| `nats` | NATS 2.12 with JetStream (persistence) enabled; monitoring on port 8222 |
| `quarkus-service` | Quarkus 4.0.0.Beta1 + SmallRye Reactive Messaging via the Quarkiverse `quarkus-messaging-nats-jetstream` connector; plain `jnats` for core request/reply and queue groups |
| `node-service` | Node 24 + TypeScript; serves the web UI and bridges it to NATS via REST + SSE (Server-Sent Events) |

## Run

```bash
docker compose up --build
```

- Web UI: **http://localhost:8088**
- NATS monitoring: http://localhost:8222
- Quarkus health: http://localhost:8089/health

All host ports bind to `127.0.0.1` only.

## What the demo shows

| Feature | How to see it |
|---|---|
| **Subscribe / unsubscribe** | *Subscriptions* card: subscribe to `demo.quarkus.>` and watch Quarkus tick every 5 s in the live log; remove the tag to unsubscribe. |
| **Send / receive** | *Publish* card: publish to `demo.events.test`. With the JetStream switch on, the message is persisted in the `DEMO` stream and the Quarkus durable consumer announces it on `demo.quarkus.seen`. |
| **Request / reply** | *Request/Reply* card: `demo.greet` is answered by Quarkus, `demo.echo` by Node. The reply and round-trip latency are shown. |
| **Queues (queue groups)** | Both services subscribe to `demo.work` in queue group `workers`. "Send 6 to queue" fires six requests and shows how NATS load-balances them between the two services. |
| **JetStream persistence** | *JetStream* card: stream statistics, durable consumers, and "Replay last 10" reads persisted messages straight back out of the stream. |

## Subjects

| Subject | Pattern | Who |
|---|---|---|
| `demo.events.>` | JetStream (stream `DEMO`, file storage) | UI publishes; Quarkus durable consumer `quarkus-events` consumes |
| `demo.quarkus.ticks` | JetStream, published by SmallRye `@Outgoing` | Quarkus emits a tick every 5 s |
| `demo.quarkus.seen` | core pub/sub | Quarkus announces each consumed event |
| `demo.greet` | core request/reply | Quarkus replies (jnats) |
| `demo.echo` | core request/reply | Node replies |
| `demo.work` | core, queue group `workers` | Quarkus + Node, load-balanced |

The `DEMO` stream is created idempotently by `node-service` on startup, which is why
`quarkus-service` starts after it (`depends_on: service_healthy`).

## Node service API (used by the UI)

- `GET /api/health` — service + NATS connection status
- `GET /api/events` — SSE feed of everything received by active subscriptions
- `GET/POST /api/subscriptions`, `DELETE /api/subscriptions/:id`
- `POST /api/publish` `{subject, payload, jetstream}`
- `POST /api/request` `{subject, payload}` → `{reply, latencyMs}`
- `GET /api/jetstream` — stream info; `GET /api/jetstream/messages?limit=N` — replay

## Notes on the Quarkus service

- Quarkus **4.0.0.Beta1** with the Quarkiverse connector **3.40.6** (built against
  Quarkus 3.40.x). One compatibility shim is required: Quarkus 4 ships Jackson 3,
  while the connector injects a Jackson 2 `ObjectMapper` — `JacksonCompat.java`
  produces one (plus an explicit `jackson-databind` 2.x dependency).
- SmallRye Reactive Messaging channels: `@Incoming("events")` (durable JetStream
  consumer) and `@Outgoing("ticks")` (Mutiny `Multi` tick source) — see
  `application.properties` for the channel ↔ stream/consumer wiring.
- Core-NATS request/reply and queue groups are not JetStream features, so
  `CoreNatsService` uses the `jnats` client directly for `demo.greet` and `demo.work`.

## Local development (without Docker)

```bash
# terminal 1 — NATS
nats-server -js -m 8222

# terminal 2 — Quarkus
cd quarkus-service && mvn quarkus:dev

# terminal 3 — Node
cd node-service && npm install && npm run dev
```
