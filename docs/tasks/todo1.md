# Side-by-side NodeJS / Quarkus panes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `quarkus-service` its own REST+SSE web layer and UI so two browser panes — "NodeJS app" on `:8088` and "Quarkus app" on `:8089` — can be shown side by side, communicating only over NATS.

**Architecture:** Five new CDI beans in `quarkus-service` mirror the API half of `node-service/src/server.ts` and `nats.ts`: a shared jnats connection provider, a dynamic subscription registry, an SSE broadcaster, a JetStream admin helper and one JAX-RS resource. The existing `CoreNatsService` is modified to take its connection from the provider instead of opening its own. The UI is copied into `META-INF/resources` with a red accent.

**Tech Stack:** Quarkus 4.0.0.Beta1 (`quarkus-rest`, Mutiny), jnats 2.26.4, Jackson 2.22.3, Web Awesome 3.12, Docker Compose, bash + curl + python3 for tests.

**Spec:** `docs/superpowers/specs/2026-10-14-side-by-side-ui-design.md` — read it alongside this plan. Section 6 of the spec is the API contract and is binding.

## Global Constraints

- **Java 21**, `maven.compiler.release=21`. Records and switch patterns are available; use them.
- **No new Maven dependencies.** `quarkus-rest` already provides SSE and `RestStreamElementType`; Mutiny arrives via `quarkus-messaging`; `@Blocking` via `quarkus-core`.
- **Jackson 2 only** (`com.fasterxml.jackson`). Quarkus 4 ships Jackson 3 (`tools.jackson`); never import it. All JSON goes through `JsonCodec`, which wraps the Jackson 2 `ObjectMapper` produced by the existing `JacksonCompat`.
- **Every response body is JSON**, including every error. The UI's fetch helper calls `res.json()` before checking `res.ok`.
- **Wire shapes are frozen** by spec §6. Field names are camelCase and must match `node-service/src/server.ts` exactly (`firstSeq`, `lastSeq`, `latencyMs`, `subscriptionId`, `receivedAt`, `createdAt`).
- **`app.js` is copied byte for byte.** All Quarkus-page edits live in `index.html`.
- **Do not touch** `HealthResource` (`/health`) — the compose healthcheck depends on it — or the SmallRye channels in `application.properties`.
- Subject and queue values are `.trim()`-ed; **payload values are not**.
- Request/reply timeout is **5 seconds**, matching node.
- `/api/jetstream/messages?limit=` is capped at **50**, defaults to **10**.

## Review Focus

Input classes the spec implies but which no task's happy path exercises. Each has a test assigned to the task that owns the code.

1. **Blank or missing `subject`** on `/api/publish`, `/api/request`, `/api/subscriptions` → must be `400 {"error":"subject is required"}`, not a 500. (Tasks 2, 3)
2. **Malformed JSON request body** → must be `400 {"error":...}`, not a Quarkus HTML error page that makes the UI throw a parse error. (Task 1)
3. **JetStream publish to a subject outside the `DEMO` stream** (e.g. `foo.bar`) → must be `502 {"error":...}` returned promptly, not a hang or a 500. (Task 3)
4. **Unknown subscription id on DELETE** → must be `404 {"error":"no subscription with id <id>"}`; the UI removes tags optimistically and will mis-render on anything else. (Task 2)
5. **Unknown `/api/*` route** → must be JSON, not HTML, for the same `res.json()` reason. (Task 1)

---

### Task 1: Test harness, JSON foundation, and `/api/health`

Establishes the smoke-test script and the three beans every later task builds on, then proves the pipe end to end with the simplest endpoint.

**Files:**
- Create: `scripts/smoke-test.sh`
- Create: `quarkus-service/src/main/java/demo/NatsConnectionProvider.java`
- Create: `quarkus-service/src/main/java/demo/JsonCodec.java`
- Create: `quarkus-service/src/main/java/demo/JsonExceptionMapper.java`
- Create: `quarkus-service/src/main/java/demo/ApiResource.java`
- Modify: `quarkus-service/src/main/java/demo/CoreNatsService.java`

**Interfaces:**
- Produces: `NatsConnectionProvider.get() -> io.nats.client.Connection`, `NatsConnectionProvider.status() -> Status(boolean connected, String server)`, `JsonCodec.write(Object) -> String`, `JsonCodec.read(String) -> JsonNode`, `JsonCodec.BadRequest extends RuntimeException`, and the `ApiResource` private helpers `ok(Object)`, `error(int,String)`, `text(JsonNode,String)`, `raw(JsonNode,String)`.
- Consumes: the `ObjectMapper` produced by the existing `JacksonCompat`.

- [ ] **Step 1: Write the failing test**

Create `scripts/smoke-test.sh`:

```bash
#!/usr/bin/env bash
# Integration smoke test for the side-by-side NodeJS / Quarkus demo.
#
# Usage:  docker compose up --build -d && ./scripts/smoke-test.sh
# Needs:  curl, python3
#
# Asserts that node-service (:8088) and quarkus-service (:8089) expose the same
# API surface with the same JSON shapes, and that messages really cross between
# them over NATS.
set -uo pipefail

NODE=http://localhost:8088
QUARKUS=http://localhost:8089

pass=0
fail=0

ok()  { printf '  \033[32mPASS\033[0m %s\n' "$1"; pass=$((pass + 1)); }
bad() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; fail=$((fail + 1)); }

section() { printf '\n\033[1m%s\033[0m\n' "$1"; }

# assert_eq <expected> <actual> <description>
assert_eq() {
  if [ "$1" = "$2" ]; then ok "$3"; else bad "$3 (expected '$1', got '$2')"; fi
}

# assert_contains <haystack> <needle> <description>
assert_contains() {
  case "$1" in
    *"$2"*) ok "$3" ;;
    *)      bad "$3 (missing '$2' in: $(printf '%.120s' "$1"))" ;;
  esac
}

# field <json-path> — reads stdin, prints the value at a dotted path.
# Numeric segments index into arrays. Missing paths print __MISSING__.
field() {
  python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    print("__NOTJSON__"); sys.exit()
for k in sys.argv[1].split("."):
    try:
        d = d[int(k)] if k.lstrip("-").isdigit() else d[k]
    except Exception:
        print("__MISSING__"); sys.exit()
# json.dumps everything except plain strings, so booleans print as
# true/false rather than Python's True/False.
print(d if isinstance(d, str) else json.dumps(d))
' "$1"
}

# status <method> <url> [body] — prints the HTTP status code only.
status() {
  if [ "$#" -ge 3 ]; then
    curl -s -o /dev/null -w '%{http_code}' -X "$1" -H 'content-type: application/json' -d "$3" "$2"
  else
    curl -s -o /dev/null -w '%{http_code}' -X "$1" "$2"
  fi
}

# body <method> <url> [json-body] — prints the response body.
body() {
  if [ "$#" -ge 3 ]; then
    curl -s -X "$1" -H 'content-type: application/json' -d "$3" "$2"
  else
    curl -s -X "$1" "$2"
  fi
}

test_health() {
  section 'health — both services'
  for pair in "node-service $NODE" "quarkus-service $QUARKUS"; do
    set -- $pair
    local svc=$1 base=$2
    local out; out=$(body GET "$base/api/health")
    assert_eq 'UP'   "$(printf '%s' "$out" | field status)"        "$svc /api/health status=UP"
    assert_eq "$svc" "$(printf '%s' "$out" | field service)"       "$svc /api/health service name"
    assert_eq 'true' "$(printf '%s' "$out" | field nats.connected)" "$svc /api/health nats.connected"
  done
}

test_error_shapes() {
  section 'error responses are JSON (Review Focus 2 and 5)'
  for pair in "node-service $NODE" "quarkus-service $QUARKUS"; do
    set -- $pair
    local svc=$1 base=$2
    local out
    out=$(body GET "$base/api/does-not-exist")
    assert_contains "$out" '"error"' "$svc unknown route returns a JSON error"
    out=$(body POST "$base/api/publish" 'not json at all')
    assert_contains "$out" '"error"' "$svc malformed JSON body returns a JSON error"
  done
}

test_health
test_error_shapes

printf '\n\033[1m%d passed, %d failed\033[0m\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
```

Then make it executable:

```bash
chmod +x scripts/smoke-test.sh
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
docker compose up --build -d
./scripts/smoke-test.sh
```

Expected: the node-service assertions pass; every `quarkus-service` assertion FAILS, because `:8089` has no `/api/*` routes at all (`__NOTJSON__` or `__MISSING__` values, and HTML from Quarkus's 404 page).

- [ ] **Step 3: Create `NatsConnectionProvider`**

Create `quarkus-service/src/main/java/demo/NatsConnectionProvider.java`:

```java
package demo;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;

/**
 * Owns the single jnats {@link Connection} shared by every bean in this service.
 *
 * <p>Why lazy rather than a {@code StartupEvent} observer: several beans need
 * this connection during startup, and CDI gives no ordering guarantee between
 * observers of the same event. Opening on first {@link #get()} removes the
 * ordering question entirely.
 */
@ApplicationScoped
public class NatsConnectionProvider {

  private static final Logger LOG = Logger.getLogger(NatsConnectionProvider.class);

  /** Connection state as reported to the web UI by {@code GET /api/health}. */
  public record Status(boolean connected, String server) {}

  @ConfigProperty(name = "nats.url")
  String natsUrl;

  private Connection connection;

  /** Returns the shared connection, opening it on first call. */
  public synchronized Connection get() {
    if (connection == null) {
      try {
        connection = Nats.connect(Options.builder()
            .server(natsUrl)
            .connectionTimeout(Duration.ofSeconds(10))
            .maxReconnects(-1)
            .build());
        LOG.infof("Connected to NATS at %s", natsUrl);
      } catch (Exception e) {
        throw new IllegalStateException("cannot connect to NATS at " + natsUrl, e);
      }
    }
    return connection;
  }

  /** Never opens the connection — safe to call before anything else has. */
  public synchronized Status status() {
    if (connection == null) {
      return new Status(false, natsUrl);
    }
    return new Status(connection.getStatus() == Connection.Status.CONNECTED,
        connection.getConnectedUrl());
  }

  void onStop(@Observes ShutdownEvent ev) throws Exception {
    if (connection != null) {
      connection.close();
    }
  }
}
```

- [ ] **Step 4: Create `JsonCodec`**

Create `quarkus-service/src/main/java/demo/JsonCodec.java`:

```java
package demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * The only place this service turns objects into JSON and back.
 *
 * <p>Why not the {@code quarkus-rest-jackson} extension: Quarkus 4 ships
 * Jackson 3 ({@code tools.jackson}) while this project already pins Jackson 2
 * for the NATS connector (see {@link JacksonCompat}). Resources therefore return
 * pre-serialised {@code String} bodies built here, which keeps the two Jackson
 * versions apart and makes the wire format an explicit, reviewable choice —
 * it is a frozen contract shared with node-service.
 */
@ApplicationScoped
public class JsonCodec {

  @Inject
  ObjectMapper mapper;

  public String write(Object value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("cannot serialise " + value.getClass().getName(), e);
    }
  }

  /** Parses a request body; an absent or blank body is an empty object. */
  public JsonNode read(String body) {
    if (body == null || body.isBlank()) {
      return mapper.createObjectNode();
    }
    try {
      return mapper.readTree(body);
    } catch (JsonProcessingException e) {
      throw new BadRequest("malformed JSON body");
    }
  }

  /** Signals a client mistake; {@link JsonExceptionMapper} turns this into a 400. */
  public static class BadRequest extends RuntimeException {
    public BadRequest(String message) {
      super(message);
    }
  }
}
```

- [ ] **Step 5: Create `JsonExceptionMapper`**

Create `quarkus-service/src/main/java/demo/JsonExceptionMapper.java`:

```java
package demo;

import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.jboss.logging.Logger;

import java.util.Map;

/**
 * Guarantees every error leaves this service as JSON.
 *
 * <p>The web UI's fetch helper calls {@code res.json()} before it checks
 * {@code res.ok}, so Quarkus's default HTML error page would surface in the
 * browser as a JSON parse error and hide the real failure.
 */
@Provider
public class JsonExceptionMapper implements ExceptionMapper<Throwable> {

  private static final Logger LOG = Logger.getLogger(JsonExceptionMapper.class);

  @Inject
  JsonCodec json;

  @Override
  public Response toResponse(Throwable ex) {
    int status = switch (ex) {
      case JsonCodec.BadRequest ignored -> 400;
      case WebApplicationException wae -> wae.getResponse().getStatus();
      default -> 500;
    };
    if (status >= 500) {
      LOG.error("unhandled API error", ex);
    }
    String message = ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage();
    return Response.status(status)
        .type(MediaType.APPLICATION_JSON)
        .entity(json.write(Map.of("error", message)))
        .build();
  }
}
```

- [ ] **Step 6: Create `ApiResource` with `/api/health` only**

Create `quarkus-service/src/main/java/demo/ApiResource.java`. Later tasks add routes to this same class:

```java
package demo;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.Map;

/**
 * The web UI's REST + SSE surface.
 *
 * <p>Route for route and field for field identical to
 * {@code node-service/src/server.ts}, so the copied {@code app.js} runs against
 * either service unchanged. The shapes are a frozen contract — see section 6 of
 * {@code docs/superpowers/specs/2026-10-14-side-by-side-ui-design.md}.
 */
@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
public class ApiResource {

  @Inject
  NatsConnectionProvider nats;

  @Inject
  JsonCodec json;

  @GET
  @Path("/health")
  public String health() {
    return json.write(Map.of(
        "status", "UP",
        "service", "quarkus-service",
        "nats", nats.status()));
  }

  // --- helpers shared by every route ---------------------------------------

  private Response ok(Object body) {
    return Response.ok(json.write(body)).build();
  }

  private Response error(int status, String message) {
    return Response.status(status).entity(json.write(Map.of("error", message))).build();
  }

  /** Trimmed string field — for subjects and queue names. */
  private static String text(JsonNode node, String field) {
    return node.path(field).asText("").trim();
  }

  /** Untrimmed string field — for payloads, where whitespace is the user's. */
  private static String raw(JsonNode node, String field) {
    return node.path(field).asText("");
  }
}
```

Note: `Blocking`, `JsonNode`, `ok`, `error`, `text` and `raw` are unused until Task 2. Leave the imports and helpers in place — the next task fills them in. If your compiler setup fails the build on unused imports, add the routes from Task 2 before compiling.

- [ ] **Step 7: Modify `CoreNatsService` to use the provider**

In `quarkus-service/src/main/java/demo/CoreNatsService.java`, replace the connection handling. The two `dispatcher.subscribe(...)` blocks and the `publish` method body stay exactly as they are.

Remove these imports:

```java
import io.nats.client.Nats;
import io.nats.client.Options;
import io.quarkus.runtime.ShutdownEvent;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import java.time.Duration;
```

Add:

```java
import jakarta.inject.Inject;
```

Replace the `natsUrl` field, the `connection` field's initialisation inside `onStart`, and the whole `onStop` method:

```java
  @Inject
  NatsConnectionProvider nats;

  private Connection connection;

  void onStart(@Observes StartupEvent ev) {
    connection = nats.get();

    Dispatcher dispatcher = connection.createDispatcher();

    // ... the two existing dispatcher.subscribe(...) blocks, unchanged ...
  }
```

Delete the `onStop` method entirely — `NatsConnectionProvider` now owns the connection lifecycle. Note that `onStart` no longer declares `throws Exception`.

- [ ] **Step 8: Rebuild and run the test to verify it passes**

```bash
docker compose up --build -d
docker compose logs quarkus-service --tail 20   # confirm it started cleanly
./scripts/smoke-test.sh
```

Expected: all `test_health` and `test_error_shapes` assertions PASS for both services. Also confirm the refactor did not break the existing responder:

```bash
curl -s -X POST -H 'content-type: application/json' \
  -d '{"subject":"demo.greet","payload":"Ada"}' \
  http://localhost:8088/api/request
```

Expected: a reply containing `greetings from quarkus-service`.

- [ ] **Step 9: Commit**

```bash
git add scripts/smoke-test.sh quarkus-service/src/main/java/demo/
git commit -m "feat(quarkus): JSON foundation and /api/health

Extracts the jnats connection into a shared provider and adds the JSON codec,
error mapper and API resource the web layer is built on.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: SSE feed and subscription registry

**Files:**
- Create: `quarkus-service/src/main/java/demo/UiEventBroadcaster.java`
- Create: `quarkus-service/src/main/java/demo/SubscriptionRegistry.java`
- Modify: `quarkus-service/src/main/java/demo/ApiResource.java` (add four routes)
- Modify: `scripts/smoke-test.sh` (add `test_subscriptions`)

**Interfaces:**
- Consumes: `NatsConnectionProvider.get()`, `JsonCodec.write(Object)`, `ApiResource.ok/error/text` from Task 1.
- Produces: `UiEventBroadcaster.emit(Object)`, `UiEventBroadcaster.stream() -> Multi<String>`, `SubscriptionRegistry.subscribe(String subject, String queue) -> Info`, `SubscriptionRegistry.unsubscribe(String id) -> boolean`, `SubscriptionRegistry.list() -> List<Info>`, and the record `SubscriptionRegistry.Info(String id, String subject, String queue, String createdAt, long received)`.

- [ ] **Step 1: Write the failing test**

Add to `scripts/smoke-test.sh`, immediately before the `test_health` call at the bottom:

```bash
test_subscriptions() {
  section 'subscriptions — both services'
  for pair in "node-service $NODE" "quarkus-service $QUARKUS"; do
    set -- $pair
    local svc=$1 base=$2
    local out id

    out=$(body POST "$base/api/subscriptions" '{"subject":"demo.smoke.>"}')
    id=$(printf '%s' "$out" | field id)
    assert_eq 'demo.smoke.>' "$(printf '%s' "$out" | field subject)" "$svc POST subscription returns the subject"
    assert_eq '0'            "$(printf '%s' "$out" | field received)" "$svc new subscription has received=0"
    assert_contains "$out" 'createdAt' "$svc subscription carries createdAt"

    out=$(body GET "$base/api/subscriptions")
    assert_contains "$out" 'demo.smoke.>' "$svc GET lists the new subscription"

    assert_eq '200' "$(status DELETE "$base/api/subscriptions/$id")" "$svc DELETE removes the subscription"

    # Review Focus 4 — unknown id must be a 404 JSON error.
    assert_eq '404' "$(status DELETE "$base/api/subscriptions/does-not-exist")" "$svc DELETE unknown id is 404"
    out=$(body DELETE "$base/api/subscriptions/does-not-exist")
    assert_contains "$out" '"error"' "$svc DELETE unknown id returns a JSON error"

    # Review Focus 1 — blank subject must be a 400, not a 500.
    assert_eq '400' "$(status POST "$base/api/subscriptions" '{"subject":"  "}')" "$svc blank subject is 400"
  done
}

test_sse() {
  section 'SSE feed — both services'
  for pair in "node-service $NODE" "quarkus-service $QUARKUS"; do
    set -- $pair
    local svc=$1 base=$2
    local feed; feed=$(mktemp)

    curl -sN --max-time 6 "$base/api/events" > "$feed" &
    local pid=$!
    sleep 1
    body POST "$base/api/subscriptions" '{"subject":"demo.smoke.sse"}' > /dev/null
    sleep 1
    kill "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null

    assert_contains "$(cat "$feed")" '"kind":"hello"'      "$svc SSE sends a hello frame on connect"
    assert_contains "$(cat "$feed")" '"kind":"subscribed"' "$svc SSE announces new subscriptions"
    rm -f "$feed"
  done
}
```

and add the two calls next to the existing ones:

```bash
test_health
test_error_shapes
test_subscriptions
test_sse
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
./scripts/smoke-test.sh
```

Expected: node-service passes; every quarkus-service subscription and SSE assertion FAILS (routes do not exist).

- [ ] **Step 3: Create `UiEventBroadcaster`**

Create `quarkus-service/src/main/java/demo/UiEventBroadcaster.java`:

```java
package demo;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Fans UI events out to every connected SSE client.
 * Mirrors the {@code sseClients} set in {@code node-service/src/server.ts}.
 */
@ApplicationScoped
public class UiEventBroadcaster {

  @Inject
  JsonCodec json;

  private final BroadcastProcessor<String> processor = BroadcastProcessor.create();

  /** Serialises an event and pushes it to all current subscribers. */
  public void emit(Object event) {
    processor.onNext(json.write(event));
  }

  /** Live feed of serialised frames; each SSE client subscribes once. */
  public Multi<String> stream() {
    return processor;
  }
}
```

- [ ] **Step 4: Create `SubscriptionRegistry`**

Create `quarkus-service/src/main/java/demo/SubscriptionRegistry.java`:

```java
package demo;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Subscription;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The dynamic subscriptions the web UI creates and removes, plus the
 * announcements that keep every connected UI in step. Java mirror of the
 * subscribe/unsubscribe/list half of {@code node-service/src/nats.ts}.
 *
 * <p>Each UI subscription gets its own jnats {@link Subscription} on one shared
 * {@link Dispatcher}, so it can be cancelled individually by id.
 */
@ApplicationScoped
public class SubscriptionRegistry {

  /** Wire shape of {@code GET} and {@code POST /api/subscriptions}. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record Info(String id, String subject, String queue, String createdAt, long received) {}

  /** Wire shape of a received message on the SSE feed. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record MessageEvent(String kind, String subscriptionId, String subject, String queue,
                             String replyTo, String payload, String receivedAt) {}

  /** Wire shape of the {@code subscribed} announcement on the SSE feed. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record SubscribedEvent(String kind, String id, String subject, String queue,
                                String createdAt, long received) {}

  private record Entry(Info info, Subscription subscription, AtomicLong received) {}

  @Inject
  NatsConnectionProvider nats;

  @Inject
  UiEventBroadcaster broadcaster;

  private final Map<String, Entry> entries = new ConcurrentHashMap<>();
  private final AtomicInteger nextId = new AtomicInteger(1);
  private Dispatcher dispatcher;

  private synchronized Dispatcher dispatcher() {
    if (dispatcher == null) {
      dispatcher = nats.get().createDispatcher();
    }
    return dispatcher;
  }

  /**
   * Starts a subscription and announces it on the SSE feed.
   *
   * @param queue queue group name, or {@code null} for a plain subscription
   */
  public Info subscribe(String subject, String queue) {
    String id = String.valueOf(nextId.getAndIncrement());
    AtomicLong received = new AtomicLong();
    Dispatcher d = dispatcher();
    Subscription sub = (queue == null)
        ? d.subscribe(subject, msg -> onMessage(id, queue, received, msg))
        : d.subscribe(subject, queue, msg -> onMessage(id, queue, received, msg));

    Info info = new Info(id, subject, queue, Instant.now().toString(), 0);
    entries.put(id, new Entry(info, sub, received));
    broadcaster.emit(new SubscribedEvent("subscribed", info.id(), info.subject(),
        info.queue(), info.createdAt(), 0));
    return info;
  }

  /** Cancels a subscription and announces the removal. Returns false if unknown. */
  public boolean unsubscribe(String id) {
    Entry entry = entries.remove(id);
    if (entry == null) {
      return false;
    }
    dispatcher().unsubscribe(entry.subscription());
    broadcaster.emit(Map.of("kind", "unsubscribed", "id", id));
    return true;
  }

  /** Current subscriptions, each with its live received count. */
  public List<Info> list() {
    List<Info> out = new ArrayList<>();
    for (Entry entry : entries.values()) {
      Info i = entry.info();
      out.add(new Info(i.id(), i.subject(), i.queue(), i.createdAt(), entry.received().get()));
    }
    return out;
  }

  private void onMessage(String id, String queue, AtomicLong received, Message msg) {
    received.incrementAndGet();
    broadcaster.emit(new MessageEvent(
        "message",
        id,
        msg.getSubject(),
        queue,
        msg.getReplyTo(),
        new String(msg.getData(), StandardCharsets.UTF_8),
        Instant.now().toString()));
  }
}
```

- [ ] **Step 5: Add the four routes to `ApiResource`**

Add these injections next to the existing ones:

```java
  @Inject
  SubscriptionRegistry subscriptions;

  @Inject
  UiEventBroadcaster broadcaster;
```

Add these imports:

```java
import io.smallrye.mutiny.Multi;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PathParam;
import org.jboss.resteasy.reactive.RestStreamElementType;
import java.time.Duration;
import java.time.Instant;
```

Add the routes after `health()`:

```java
  /**
   * Server-Sent Events feed. A {@code hello} frame first, then live events, with
   * a heartbeat every 15s so idle proxies do not close the connection. The UI
   * ignores frames whose {@code kind} it does not recognise, so the heartbeat
   * needs no client support.
   */
  @GET
  @Path("/events")
  @Produces(MediaType.SERVER_SENT_EVENTS)
  @RestStreamElementType(MediaType.APPLICATION_JSON)
  public Multi<String> events() {
    Multi<String> hello = Multi.createFrom().item(
        () -> json.write(Map.of("kind", "hello", "at", Instant.now().toString())));
    Multi<String> heartbeats = Multi.createFrom().ticks().every(Duration.ofSeconds(15))
        .onOverflow().drop()
        .map(tick -> json.write(Map.of("kind", "heartbeat")));
    Multi<String> live = Multi.createBy().merging().streams(broadcaster.stream(), heartbeats);
    return Multi.createBy().concatenating().streams(hello, live);
  }

  @GET
  @Path("/subscriptions")
  @Blocking
  public Response listSubscriptions() {
    return ok(subscriptions.list());
  }

  @POST
  @Path("/subscriptions")
  @Consumes(MediaType.APPLICATION_JSON)
  @Blocking
  public Response subscribe(String requestBody) {
    JsonNode in = json.read(requestBody);
    String subject = text(in, "subject");
    if (subject.isEmpty()) {
      return error(400, "subject is required");
    }
    String queue = text(in, "queue");
    SubscriptionRegistry.Info info = subscriptions.subscribe(subject, queue.isEmpty() ? null : queue);
    return Response.status(201).entity(json.write(info)).build();
  }

  @DELETE
  @Path("/subscriptions/{id}")
  @Blocking
  public Response unsubscribe(@PathParam("id") String id) {
    if (!subscriptions.unsubscribe(id)) {
      return error(404, "no subscription with id " + id);
    }
    return ok(Map.of("id", id, "unsubscribed", true));
  }
```

- [ ] **Step 6: Rebuild and run the test to verify it passes**

```bash
docker compose up --build -d quarkus-service
./scripts/smoke-test.sh
```

Expected: `test_subscriptions` and `test_sse` PASS for both services.

- [ ] **Step 7: Commit**

```bash
git add quarkus-service/src/main/java/demo/ scripts/smoke-test.sh
git commit -m "feat(quarkus): dynamic subscriptions and SSE event feed

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 3: Publish and request/reply

**Files:**
- Create: `quarkus-service/src/main/java/demo/JetStreamAdmin.java`
- Modify: `quarkus-service/src/main/java/demo/ApiResource.java` (add two routes)
- Modify: `scripts/smoke-test.sh` (add `test_publish`, `test_request`)

**Interfaces:**
- Consumes: `NatsConnectionProvider.get()`, `ApiResource.ok/error/text/raw`.
- Produces: `JetStreamAdmin.publish(String subject, String payload) -> long` (the JetStream sequence) and the constant `JetStreamAdmin.STREAM = "DEMO"`. Task 4 adds `info()` and `lastMessages(int)` to this same class.

- [ ] **Step 1: Write the failing test**

Add to `scripts/smoke-test.sh` before the call block:

```bash
test_publish() {
  section 'publish — both services'
  for pair in "node-service $NODE" "quarkus-service $QUARKUS"; do
    set -- $pair
    local svc=$1 base=$2
    local out

    out=$(body POST "$base/api/publish" '{"subject":"demo.events.smoke","payload":"hi","jetstream":false}')
    assert_eq 'demo.events.smoke' "$(printf '%s' "$out" | field subject)" "$svc core publish echoes the subject"

    out=$(body POST "$base/api/publish" '{"subject":"demo.events.smoke","payload":"hi","jetstream":true}')
    local seq; seq=$(printf '%s' "$out" | field seq)
    case "$seq" in
      ''|*[!0-9]*) bad "$svc JetStream publish returns a numeric seq (got '$seq')" ;;
      *)           ok  "$svc JetStream publish returns a numeric seq" ;;
    esac

    # Review Focus 1 — blank subject must be a 400.
    assert_eq '400' "$(status POST "$base/api/publish" '{"subject":"","payload":"x"}')" "$svc blank publish subject is 400"

    # Review Focus 3 — JetStream publish outside the DEMO stream must fail cleanly.
    assert_eq '502' "$(status POST "$base/api/publish" '{"subject":"outside.the.stream","payload":"x","jetstream":true}')" \
      "$svc JetStream publish to an unmanaged subject is 502"
  done
}

test_request() {
  section 'request/reply — crossing between the services'
  local out

  # node asks, quarkus answers
  out=$(body POST "$NODE/api/request" '{"subject":"demo.greet","payload":"Ada"}')
  assert_contains "$out" 'quarkus-service' 'node -> demo.greet is answered by quarkus-service'

  # quarkus asks, node answers
  out=$(body POST "$QUARKUS/api/request" '{"subject":"demo.echo","payload":"Ada"}')
  assert_contains "$out" 'node-service' 'quarkus -> demo.echo is answered by node-service'

  for pair in "node-service $NODE" "quarkus-service $QUARKUS"; do
    set -- $pair
    local svc=$1 base=$2
    out=$(body POST "$base/api/request" '{"subject":"demo.work","payload":"job"}')
    assert_contains "$out" 'latencyMs' "$svc request reports latencyMs"
    assert_eq '504' "$(status POST "$base/api/request" '{"subject":"demo.nobody.listening","payload":"x"}')" \
      "$svc request with no responder is 504"
  done
}
```

and extend the call block:

```bash
test_publish
test_request
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
./scripts/smoke-test.sh
```

Expected: all quarkus-service publish and request assertions FAIL (routes missing).

- [ ] **Step 3: Create `JetStreamAdmin` with the publish method**

Create `quarkus-service/src/main/java/demo/JetStreamAdmin.java`:

```java
package demo;

import io.nats.client.JetStream;
import io.nats.client.api.PublishAck;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.nio.charset.StandardCharsets;

/**
 * JetStream operations the web UI drives: persisted publish, and (added in
 * Task 4) stream statistics and replay.
 *
 * <p>Deliberately uses plain jnats rather than the SmallRye {@code @Outgoing}
 * channel: that channel is pinned to subject {@code demo.quarkus.ticks} in
 * application.properties, so it cannot publish to a subject typed into the UI.
 */
@ApplicationScoped
public class JetStreamAdmin {

  /** The stream node-service creates on startup. */
  static final String STREAM = "DEMO";

  @Inject
  NatsConnectionProvider nats;

  /**
   * Publishes into the DEMO stream and returns the sequence the broker assigned.
   * Throws if the subject is not covered by the stream's subject filters.
   */
  public long publish(String subject, String payload) throws Exception {
    JetStream js = nats.get().jetStream();
    PublishAck ack = js.publish(subject, payload.getBytes(StandardCharsets.UTF_8));
    return ack.getSeqno();
  }
}
```

- [ ] **Step 4: Add the two routes to `ApiResource`**

Add the injection:

```java
  @Inject
  JetStreamAdmin jetStream;
```

Add the imports:

```java
import io.nats.client.Message;
import java.nio.charset.StandardCharsets;
```

Add a constant beside the class's other fields:

```java
  /** Matches the 5s timeout node-service uses. */
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);
```

Add the routes:

```java
  @POST
  @Path("/publish")
  @Consumes(MediaType.APPLICATION_JSON)
  @Blocking
  public Response publish(String requestBody) {
    JsonNode in = json.read(requestBody);
    String subject = text(in, "subject");
    if (subject.isEmpty()) {
      return error(400, "subject is required");
    }
    String payload = raw(in, "payload");
    boolean useJetStream = in.path("jetstream").asBoolean(false);
    try {
      if (useJetStream) {
        return ok(Map.of("subject", subject, "jetstream", true,
            "seq", jetStream.publish(subject, payload)));
      }
      nats.get().publish(subject, payload.getBytes(StandardCharsets.UTF_8));
      return ok(Map.of("subject", subject, "jetstream", false));
    } catch (Exception e) {
      return error(502, String.valueOf(e));
    }
  }

  @POST
  @Path("/request")
  @Consumes(MediaType.APPLICATION_JSON)
  @Blocking
  public Response request(String requestBody) {
    JsonNode in = json.read(requestBody);
    String subject = text(in, "subject");
    if (subject.isEmpty()) {
      return error(400, "subject is required");
    }
    String payload = raw(in, "payload");
    long started = System.nanoTime();
    try {
      Message reply = nats.get().request(
          subject, payload.getBytes(StandardCharsets.UTF_8), REQUEST_TIMEOUT);
      if (reply == null) {
        return error(504, "no reply: timed out after " + REQUEST_TIMEOUT.toMillis() + " ms");
      }
      return ok(Map.of(
          "subject", subject,
          "reply", new String(reply.getData(), StandardCharsets.UTF_8),
          "latencyMs", Math.round((System.nanoTime() - started) / 1_000_000.0)));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return error(504, "no reply: interrupted");
    }
  }
```

- [ ] **Step 5: Rebuild and run the test to verify it passes**

```bash
docker compose up --build -d quarkus-service
./scripts/smoke-test.sh
```

Expected: `test_publish` and `test_request` PASS for both services, including the two cross-service assertions.

- [ ] **Step 6: Commit**

```bash
git add quarkus-service/src/main/java/demo/ scripts/smoke-test.sh
git commit -m "feat(quarkus): publish and request/reply endpoints

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: JetStream statistics and replay

**Files:**
- Modify: `quarkus-service/src/main/java/demo/JetStreamAdmin.java` (add `info`, `lastMessages`)
- Modify: `quarkus-service/src/main/java/demo/ApiResource.java` (add two routes)
- Modify: `scripts/smoke-test.sh` (add `test_jetstream`)

**Interfaces:**
- Consumes: `JetStreamAdmin.STREAM`, `NatsConnectionProvider.get()`.
- Produces: `JetStreamAdmin.info() -> StreamView`, `JetStreamAdmin.lastMessages(int limit) -> List<StoredMessage>`, and the records `ConsumerView(String name, String durable, long delivered, long pending)`, `StreamView(String stream, List<String> subjects, String storage, long messages, long firstSeq, long lastSeq, List<ConsumerView> consumers)`, `StoredMessage(long seq, String subject, String payload, String time)`.

- [ ] **Step 1: Write the failing test**

Add to `scripts/smoke-test.sh`:

```bash
test_jetstream() {
  section 'JetStream stats and replay — both services'
  # Make sure the stream is not empty.
  body POST "$NODE/api/publish" '{"subject":"demo.events.smoke","payload":"for-replay","jetstream":true}' > /dev/null

  for pair in "node-service $NODE" "quarkus-service $QUARKUS"; do
    set -- $pair
    local svc=$1 base=$2
    local out

    out=$(body GET "$base/api/jetstream")
    assert_eq 'DEMO' "$(printf '%s' "$out" | field stream)" "$svc reports stream DEMO"
    assert_contains "$out" 'demo.events.>' "$svc reports the stream subjects"
    assert_contains "$out" 'firstSeq'      "$svc reports firstSeq"
    assert_contains "$out" 'lastSeq'       "$svc reports lastSeq"
    assert_contains "$out" 'quarkus-events' "$svc lists the durable consumer"

    out=$(body GET "$base/api/jetstream/messages?limit=5")
    assert_contains "$out" 'for-replay' "$svc replays persisted messages"
    local count; count=$(printf '%s' "$out" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')
    if [ "$count" -le 5 ]; then ok "$svc honours limit=5"; else bad "$svc honours limit=5 (got $count)"; fi

    # Review Focus — limit above the cap must be clamped, not passed through.
    out=$(body GET "$base/api/jetstream/messages?limit=9999")
    count=$(printf '%s' "$out" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')
    if [ "$count" -le 50 ]; then ok "$svc clamps limit to 50"; else bad "$svc clamps limit to 50 (got $count)"; fi
  done
}
```

and extend the call block with `test_jetstream`.

- [ ] **Step 2: Run the test to verify it fails**

```bash
./scripts/smoke-test.sh
```

Expected: quarkus-service JetStream assertions FAIL.

- [ ] **Step 3: Add the records and two methods to `JetStreamAdmin`**

Add these imports:

```java
import io.nats.client.JetStreamManagement;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.StreamInfo;
import java.util.ArrayList;
import java.util.List;
```

Add the records inside the class, above the `publish` method:

```java
  /** Wire shape of one consumer inside the stream view. */
  public record ConsumerView(String name, String durable, long delivered, long pending) {}

  /** Wire shape of {@code GET /api/jetstream}. */
  public record StreamView(String stream, List<String> subjects, String storage, long messages,
                           long firstSeq, long lastSeq, List<ConsumerView> consumers) {}

  /** Wire shape of one entry from {@code GET /api/jetstream/messages}. */
  public record StoredMessage(long seq, String subject, String payload, String time) {}
```

Add the two methods below `publish`:

```java
  /** Stream configuration, current state and durable consumers. */
  public StreamView info() throws Exception {
    JetStreamManagement jsm = nats.get().jetStreamManagement();
    StreamInfo si = jsm.getStreamInfo(STREAM);
    List<ConsumerView> consumers = new ArrayList<>();
    for (ConsumerInfo ci : jsm.getConsumers(STREAM)) {
      consumers.add(new ConsumerView(
          ci.getName(),
          ci.getConsumerConfiguration().getDurable(),
          ci.getDelivered().getStreamSequence(),
          ci.getNumPending()));
    }
    return new StreamView(
        si.getConfiguration().getName(),
        si.getConfiguration().getSubjects(),
        si.getConfiguration().getStorageType().toString().toLowerCase(),
        si.getStreamState().getMsgCount(),
        si.getStreamState().getFirstSequence(),
        si.getStreamState().getLastSequence(),
        consumers);
  }

  /**
   * Reads the most recent persisted messages straight out of the stream,
   * newest first. Deleted sequences leave gaps; those are skipped, exactly as
   * the node implementation does.
   */
  public List<StoredMessage> lastMessages(int limit) throws Exception {
    JetStreamManagement jsm = nats.get().jetStreamManagement();
    StreamInfo si = jsm.getStreamInfo(STREAM);
    long first = si.getStreamState().getFirstSequence();
    long last = si.getStreamState().getLastSequence();
    List<StoredMessage> out = new ArrayList<>();
    for (long seq = last; seq >= first && out.size() < limit; seq--) {
      try {
        MessageInfo m = jsm.getMessage(STREAM, seq);
        out.add(new StoredMessage(
            m.getSeq(),
            m.getSubject(),
            new String(m.getData(), StandardCharsets.UTF_8),
            m.getTime().toInstant().toString()));
      } catch (Exception gap) {
        // deleted message — skip
      }
    }
    return out;
  }
```

- [ ] **Step 4: Add the two routes to `ApiResource`**

Add the imports:

```java
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.QueryParam;
```

Add the routes:

```java
  @GET
  @Path("/jetstream")
  @Blocking
  public Response streamInfo() {
    try {
      return ok(jetStream.info());
    } catch (Exception e) {
      return error(502, String.valueOf(e));
    }
  }

  @GET
  @Path("/jetstream/messages")
  @Blocking
  public Response streamMessages(@QueryParam("limit") @DefaultValue("10") int limit) {
    try {
      return ok(jetStream.lastMessages(Math.max(0, Math.min(limit, 50))));
    } catch (Exception e) {
      return error(502, String.valueOf(e));
    }
  }
```

- [ ] **Step 5: Rebuild and run the test to verify it passes**

```bash
docker compose up --build -d quarkus-service
./scripts/smoke-test.sh
```

Expected: `test_jetstream` PASSES for both services. The full Quarkus API surface is now complete.

- [ ] **Step 6: Commit**

```bash
git add quarkus-service/src/main/java/demo/ scripts/smoke-test.sh
git commit -m "feat(quarkus): JetStream statistics and replay endpoints

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 5: The cross-pane assertion

The headline claim of the whole feature: an action in one pane shows up in the other, over NATS. Everything before this task was parity; this is the proof.

**Files:**
- Modify: `scripts/smoke-test.sh` (add `test_cross_service`)

**Interfaces:**
- Consumes: both services' `/api/subscriptions`, `/api/events` and `/api/publish` routes.
- Produces: nothing — this is a test-only task.

- [ ] **Step 1: Write the failing test**

Add to `scripts/smoke-test.sh`:

```bash
# Subscribe on one service, publish from the other, assert the message crosses
# over NATS. This is the behaviour the side-by-side demo exists to show.
cross_check() {
  local listener_name=$1 listener=$2 publisher_name=$3 publisher=$4
  local token="cross-$RANDOM"
  local feed; feed=$(mktemp)

  curl -sN --max-time 8 "$listener/api/events" > "$feed" &
  local pid=$!
  sleep 1
  body POST "$listener/api/subscriptions" '{"subject":"demo.events.cross"}' > /dev/null
  sleep 1
  body POST "$publisher/api/publish" \
    "{\"subject\":\"demo.events.cross\",\"payload\":\"$token\",\"jetstream\":true}" > /dev/null
  sleep 2
  kill "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null

  assert_contains "$(cat "$feed")" "$token" \
    "published from $publisher_name, received by $listener_name over NATS"
  rm -f "$feed"
}

test_cross_service() {
  section 'cross-service delivery — the point of the demo'
  cross_check 'quarkus-service' "$QUARKUS" 'node-service'    "$NODE"
  cross_check 'node-service'    "$NODE"    'quarkus-service' "$QUARKUS"
}
```

and extend the call block with `test_cross_service`.

- [ ] **Step 2: Run the test**

```bash
./scripts/smoke-test.sh
```

Expected: PASS in both directions. If either direction fails, that is a real defect in the work from Tasks 2-4 — do not weaken the test. The usual causes are the SSE feed not flushing frames promptly, or the subscription being created after the publish (check the `sleep` ordering before suspecting the service).

- [ ] **Step 3: Commit**

```bash
git add scripts/smoke-test.sh
git commit -m "test: assert messages cross between the two services over NATS

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 6: The two pages

**Files:**
- Modify: `node-service/public/index.html` (heading only)
- Create: `quarkus-service/src/main/resources/META-INF/resources/index.html`
- Create: `quarkus-service/src/main/resources/META-INF/resources/app.js`

**Interfaces:**
- Consumes: every route built in Tasks 1-4.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add the heading to the node page**

In `node-service/public/index.html`, the header currently reads:

```html
          <h1 class="app-title">NATS Messaging Demo</h1>
```

Change it to:

```html
          <h1 class="app-title">NodeJS app</h1>
```

Leave the `<title>`, the icon, the `wa-tag` and everything else untouched.

- [ ] **Step 2: Copy both files into the Quarkus resources**

```bash
mkdir -p quarkus-service/src/main/resources/META-INF/resources
cp node-service/public/app.js     quarkus-service/src/main/resources/META-INF/resources/app.js
cp node-service/public/index.html quarkus-service/src/main/resources/META-INF/resources/index.html
```

`app.js` is now final — do not edit it. Every call in it is origin-relative, so it targets whichever service served it.

- [ ] **Step 3: Set the heading and accent on the Quarkus page**

In `quarkus-service/src/main/resources/META-INF/resources/index.html`, change the heading:

```html
          <h1 class="app-title">Quarkus app</h1>
```

Then add this block at the very top of the existing `<style>` element, before the `html, body` rule:

```css
      /* Re-point the Web Awesome brand scale from blue to red. Every semantic
         brand token (fill-loud, border-loud, on-quiet, the focus ring) is an
         alias of these ten steps in both light and dark mode, so recolouring
         here recolours every button, tag and badge without touching markup. */
      :root {
        --wa-color-brand-10: var(--wa-color-red-10);
        --wa-color-brand-20: var(--wa-color-red-20);
        --wa-color-brand-30: var(--wa-color-red-30);
        --wa-color-brand-40: var(--wa-color-red-40);
        --wa-color-brand-50: var(--wa-color-red-50);
        --wa-color-brand-60: var(--wa-color-red-60);
        --wa-color-brand-70: var(--wa-color-red-70);
        --wa-color-brand-80: var(--wa-color-red-80);
        --wa-color-brand-90: var(--wa-color-red-90);
        --wa-color-brand-95: var(--wa-color-red-95);
      }
```

- [ ] **Step 4: Rebuild and verify both pages**

```bash
docker compose up --build -d
```

Then check the headings over HTTP:

```bash
curl -s http://localhost:8088/ | grep 'app-title'
curl -s http://localhost:8089/ | grep 'app-title'
```

Expected: `NodeJS app` and `Quarkus app` respectively.

- [ ] **Step 5: Verify the accent in a real browser**

Load each page and read the computed background of the Subscribe button. The expected values were measured against Web Awesome 3.12 during design:

- `http://localhost:8088` → `rgb(0, 113, 236)` (blue, unchanged)
- `http://localhost:8089` → `rgb(220, 49, 70)` (red)

The check to run in each page's console, or via Playwright `browser_evaluate`:

```js
(() => {
  const btn = document.getElementById('sub-add');
  const inner = btn.shadowRoot.querySelector('.button, [part~="base"]');
  return {
    heading: document.querySelector('.app-title').textContent,
    buttonBg: getComputedStyle(inner).backgroundColor,
  };
})()
```

Read the value in a **separate** evaluation from any style injection — custom-property changes need a style recalc, and reading in the same tick returns the stale color.

Also confirm, by eye, that both pages still render all five cards and that the live log fills when you subscribe to `demo.quarkus.>`.

- [ ] **Step 6: Run the full smoke test once more**

```bash
./scripts/smoke-test.sh
```

Expected: everything still passes; serving static resources must not have disturbed the API routes.

- [ ] **Step 7: Commit**

```bash
git add node-service/public/index.html quarkus-service/src/main/resources/META-INF/resources/
git commit -m "feat(ui): NodeJS app heading and a red-accented Quarkus app page

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 7: Documentation

**Files:**
- Modify: `compose.yml` (header comment and the quarkus-service port comment)
- Modify: `README.md`

**Interfaces:**
- Consumes: nothing. Produces: nothing.

- [ ] **Step 1: Update the compose comments**

In `compose.yml`, the header comment currently reads:

```yaml
# Run:  docker compose up --build
# UI:   http://localhost:8088
```

Change the UI line to:

```yaml
# Run:  docker compose up --build
# UIs:  http://localhost:8088 (NodeJS app)  ·  http://localhost:8089 (Quarkus app)
#       Open both side by side to watch messages cross between them over NATS.
```

And change the `quarkus-service` port comment from `# http://localhost:8089/health` to `# web UI + API`.

- [ ] **Step 2: Update the README**

In `README.md`, replace the Run section's bullet list:

```markdown
- Web UI: **http://localhost:8088**
- NATS monitoring: http://localhost:8222
- Quarkus health: http://localhost:8089/health
```

with:

```markdown
| URL | What |
|---|---|
| **http://localhost:8088** | **NodeJS app** — the UI served by `node-service` (blue buttons) |
| **http://localhost:8089** | **Quarkus app** — the same UI served by `quarkus-service` (red buttons) |
| http://localhost:8222 | NATS monitoring |
| http://localhost:8089/health · http://localhost:8088/api/health | service health |

Open the two apps side by side. They share no HTTP connection — everything that
passes between them goes over NATS. Subscribe to `demo.events.>` in one, publish
from the other, and watch the message arrive.

> Each service's startup log reports the port *inside* its container (`8080`).
> The reachable host ports are the ones above; `docker ps` is the authority.
```

Also update the `node-service` row of the container table:

```markdown
| `node-service` | Node 24 + TypeScript; serves the **NodeJS app** UI and bridges it to NATS via REST + SSE (Server-Sent Events) |
```

and the `quarkus-service` row:

```markdown
| `quarkus-service` | Quarkus 4.0.0.Beta1 + SmallRye Reactive Messaging via the Quarkiverse `quarkus-messaging-nats-jetstream` connector; plain `jnats` for core request/reply, queue groups, and the **Quarkus app** UI's REST + SSE API |
```

Finally add a Testing section at the end:

```markdown
## Testing

With the stack running, `./scripts/smoke-test.sh` exercises every endpoint on
both services, checks that their JSON shapes match, and asserts that a message
published from one app is received by the other over NATS.
```

- [ ] **Step 3: Verify the docs match reality**

```bash
docker compose ps --format 'table {{.Service}}\t{{.Ports}}'
curl -s http://localhost:8088/ | grep -o 'NodeJS app'
curl -s http://localhost:8089/ | grep -o 'Quarkus app'
./scripts/smoke-test.sh
```

Expected: the ports in the README match the `docker compose ps` output, both headings are found, and the smoke test is fully green.

- [ ] **Step 4: Commit**

```bash
git add compose.yml README.md
git commit -m "docs: document the two side-by-side apps and the smoke test

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Review

_To be completed after implementation: what was built, what the smoke test reports, and anything that deviated from this plan._
