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
