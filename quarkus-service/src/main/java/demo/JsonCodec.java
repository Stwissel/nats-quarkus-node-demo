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
