package demo;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.smallrye.mutiny.Multi;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PathParam;
import org.jboss.resteasy.reactive.RestStreamElementType;
import io.nats.client.Message;
import java.nio.charset.StandardCharsets;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.QueryParam;
import java.time.Duration;
import java.time.Instant;
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

  /** Matches the 5s timeout node-service uses. */
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);

  @Inject
  NatsConnectionProvider nats;

  @Inject
  JsonCodec json;

  @Inject
  SubscriptionRegistry subscriptions;

  @Inject
  UiEventBroadcaster broadcaster;

  @Inject
  JetStreamAdmin jetStream;

  @GET
  @Path("/health")
  public String health() {
    return json.write(Map.of(
        "status", "UP",
        "service", "quarkus-service",
        "nats", nats.status()));
  }

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
