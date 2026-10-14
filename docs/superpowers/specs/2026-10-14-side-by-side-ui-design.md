# Side-by-side NodeJS / Quarkus panes — design

Date: 2026-10-14
Status: approved (design), pending implementation plan

## 1. Goal

Run two browser panes side by side — **"NodeJS app"** on `:8088` and **"Quarkus app"**
on `:8089` — each a fully functional NATS client UI backed by its own service.
An action in one pane visibly produces a message in the other, with NATS as the
only path between them. The demo currently has one UI, served by `node-service`;
`quarkus-service` is a headless messaging participant with a single `/health`
endpoint.

Success criterion: with both panes open, subscribing to `demo.events.>` in the
Quarkus pane and publishing from the Node pane makes the message appear in the
Quarkus pane's live log, having crossed process boundaries via NATS.

## 2. Non-goals

- No HTTP communication between `node-service` and `quarkus-service`. All
  cross-service traffic goes over NATS.
- No change to the existing messaging behaviour: the `demo.greet` responder, the
  `workers` queue group, the `quarkus-events` durable consumer and the 5-second
  tick publisher all keep working exactly as they do today.
- No combined single-page view with iframes. "Side by side" means two browser
  windows, which keeps each pane on its own origin and avoids CORS entirely.
- No authentication, no multi-user state.

## 3. Decisions taken during design

| Decision | Choice | Rationale |
|---|---|---|
| Quarkus pane's backend | **Real Quarkus REST+SSE API** over its own jnats connection | A cosmetic copy pointed at node-service's API would make both panes one backend wearing two hats, demonstrating nothing about messaging. |
| Frontend organisation | **Two independent copies** of `index.html` + `app.js` | Each service stays readable end to end as a standalone unit. Accepted cost: UI edits must be made twice and the copies will drift. Recorded here once; not revisited. |
| JetStream publish path in Quarkus | **Plain jnats**, not the SmallRye `@Outgoing` channel | The `ticks` outgoing channel is pinned to subject `demo.quarkus.ticks` in `application.properties`, so it cannot publish to an arbitrary subject typed into the UI. |
| Button recolouring | **Override Web Awesome `--wa-color-brand-*` tokens** in the Quarkus page's `<style>` block | Recolours every button, tag and badge from one place instead of hand-editing ~20 `variant="brand"` attributes. |

## 4. Architecture

```
Browser pane A :8088              Browser pane B :8089
        |                                 |
   node-service                     quarkus-service
   REST + SSE  (server.ts)          REST + SSE  (ApiResource)      <- NEW
   NatsBridge  (nats.js)            SubscriptionRegistry (jnats)   <- NEW
        \                                /
         \________ NATS :4222 __________/
```

Each service holds its own NATS connection, its own dynamic subscription
registry and its own SSE fan-out. They are symmetric in capability and
independent in implementation.

## 5. Quarkus components

`CoreNatsService` currently owns the jnats `Connection` as a private field.
Three new beans need that same connection, so the connection lifecycle is
extracted into its own provider. This is the minimum change required to avoid
opening competing connections; no other refactoring is in scope.

| Class | Status | Responsibility | Depends on |
|---|---|---|---|
| `NatsConnectionProvider` | new (extracted) | Opens the jnats `Connection` on `StartupEvent`, closes it on `ShutdownEvent`, exposes `Connection get()` and a `status()` record. | — |
| `CoreNatsService` | modified | Keeps the `demo.greet` responder and the `demo.work` queue-group member. Obtains its connection from the provider instead of opening one. | `NatsConnectionProvider` |
| `SubscriptionRegistry` | new | Dynamic subscribe / unsubscribe / list with per-subscription receive counts. Pushes every received message to the broadcaster. Java mirror of `node-service/src/nats.ts:103-141`. | `NatsConnectionProvider`, `UiEventBroadcaster` |
| `UiEventBroadcaster` | new | Mutiny `BroadcastProcessor<String>` fanning JSON event frames out to all connected SSE clients. | — |
| `JetStreamAdmin` | new | Stream info and replay-last-N via `connection.jetStreamManagement()`; JetStream publish via `connection.jetStream()`. | `NatsConnectionProvider` |
| `JsonCodec` | new | The single place objects become JSON and back, wrapping the Jackson 2 `ObjectMapper` from `JacksonCompat`. | `JacksonCompat` |
| `JsonExceptionMapper` | new | Turns every uncaught error into a JSON body. The UI calls `res.json()` before checking `res.ok`, so Quarkus's default HTML error page would surface as a parse error and hide the real failure. | `JsonCodec` |
| `ApiResource` | new | `@Path("/api")` — the nine routes in section 6. Thin; delegates to the beans above. | all of the above |

`HealthResource` at `/health` stays as is — the compose healthcheck depends on it.

## 6. API contract

The copied `app.js` runs unchanged, so Quarkus must answer the same nine routes
with the same JSON shapes as `node-service/src/server.ts`. Every response must
be JSON, including errors: the client's `api()` helper calls `res.json()` on
every response and reads `body.error` on failure.

```
GET    /api/health
       -> {status:"UP", service:"quarkus-service", nats:{connected, server}}

GET    /api/events                      text/event-stream
       -> {kind:"hello", at}                                     on connect
       -> {kind:"message", subscriptionId, subject, payload,
           receivedAt, queue?, replyTo?}
       -> {kind:"subscribed", id, subject, queue?, createdAt, received}
       -> {kind:"unsubscribed", id}
       plus a ": keep-alive" comment frame every 15s

GET    /api/subscriptions
       -> [{id, subject, queue?, createdAt, received}]

POST   /api/subscriptions   {subject, queue?}
       -> 201 {id, subject, queue?, createdAt, received}
       -> 400 {error:"subject is required"}        when subject is blank
       also broadcasts {kind:"subscribed", ...} on the SSE feed

DELETE /api/subscriptions/{id}
       -> 200 {id, unsubscribed:true}
       -> 404 {error:"no subscription with id <id>"}
       also broadcasts {kind:"unsubscribed", id} on the SSE feed

POST   /api/publish         {subject, payload, jetstream}
       -> 200 {subject, jetstream, seq?}    seq present only when jetstream=true
       -> 400 {error:"subject is required"}
       -> 502 {error}                        on broker failure

POST   /api/request         {subject, payload}
       -> 200 {subject, reply, latencyMs}
       -> 400 {error:"subject is required"}
       -> 504 {error:"no reply: ..."}        on timeout (5s, matching node)

GET    /api/jetstream
       -> {stream, subjects[], storage, messages, firstSeq, lastSeq,
           consumers:[{name, durable, delivered, pending}]}
       -> 502 {error}

GET    /api/jetstream/messages?limit=N      N capped at 50, default 10
       -> [{seq, subject, payload, time}]   newest first, sequence gaps skipped
       -> 502 {error}
```

Note: the `DEMO` stream only covers subjects `demo.events.>` and
`demo.quarkus.ticks`. Publishing with the JetStream switch on to any other
subject fails at the broker. Node behaves identically today; this is existing
demo behaviour, not a regression to fix here.

## 7. Frontend

### node-service (existing page)
`node-service/public/index.html` — the `<h1 class="app-title">` text becomes
**"NodeJS app"**. Blue `brand` buttons unchanged. No other edits.

### quarkus-service (new page)
Both files are copied from `node-service/public/`. **All edits are confined to
`index.html`**; `app.js` is copied byte for byte and must stay identical to the
node original.

`index.html` edits, and nothing else:
- `<h1 class="app-title">` text **"Quarkus app"**
- a red accent, applied by overriding the Web Awesome `--wa-color-brand-*`
  tokens in the page's `<style>` block

The exact token names must be read from the live Web Awesome 3.12 stylesheet at
implementation time rather than guessed.

`app.js` needs no edits because every call in it is origin-relative
(`fetch('/api/publish')`, `new EventSource('/api/events')`), so it automatically
targets whichever service served it.

No Dockerfile change: `quarkus-service/Dockerfile` already does `COPY src ./src`,
so `META-INF/resources/**` is packaged into the jar and served at `/`
automatically.

## 8. Verification

Neither service has a test framework today, and `@QuarkusTest` against NATS
would require a testcontainers setup — a large detour for a demo. The plan
instead targets the integration seam that can actually break, written red
before green:

- **`scripts/smoke-test.sh`** — runs against `docker compose up`:
  1. exercises all nine endpoints on **both** `:8088` and `:8089`, asserting
     status codes and JSON shape parity between the two services;
  2. makes the cross-service assertion: open an SSE subscription on
     node-service, publish via the **Quarkus** API, assert the message arrives.
- **Playwright pass** over both panes: confirm each renders, the headings read
  "NodeJS app" and "Quarkus app", and the primary buttons resolve to different
  computed colours.

This is integration-level rather than unit-level coverage. Recorded as a
deliberate deviation from unit TDD, accepted at design review.

## 9. Files created and modified

Created:
```
quarkus-service/src/main/java/demo/NatsConnectionProvider.java
quarkus-service/src/main/java/demo/JsonCodec.java
quarkus-service/src/main/java/demo/JsonExceptionMapper.java
quarkus-service/src/main/java/demo/SubscriptionRegistry.java
quarkus-service/src/main/java/demo/UiEventBroadcaster.java
quarkus-service/src/main/java/demo/JetStreamAdmin.java
quarkus-service/src/main/java/demo/ApiResource.java
quarkus-service/src/main/resources/META-INF/resources/index.html
quarkus-service/src/main/resources/META-INF/resources/app.js
scripts/smoke-test.sh
```

Modified:
```
quarkus-service/src/main/java/demo/CoreNatsService.java   connection from provider
node-service/public/index.html                            heading -> "NodeJS app"
compose.yml                                               header comment: both UIs
README.md                                                 ports table + feature notes
```

## 10. Risks

| Risk | Mitigation |
|---|---|
| ~~Web Awesome brand token names are not what we assume~~ **RESOLVED during planning** | Measured against the running page: the accent derives entirely from the ten steps `--wa-color-brand-10`…`-95`, which the theme aliases into every semantic token in both light and dark mode. Re-pointing them at the palette's own `--wa-color-red-NN` scale turns the Subscribe button from `rgb(0, 113, 236)` to `rgb(220, 49, 70)`. Caveat recorded in the plan: read the computed value in a separate tick from the style change, or you get the stale colour. |
| Quarkus REST SSE semantics differ from node's raw `text/event-stream` writes | The client only needs `data:` frames; verify frame format with `curl -N` against `/api/events` early in implementation. |
| Extracting the connection breaks the existing responders | `scripts/smoke-test.sh` asserts `demo.greet` and the `workers` queue split still work from both panes. |
| The two `app.js` copies drift | Accepted and recorded in section 3. Out of scope to solve. |
