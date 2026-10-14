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
    // Client errors carry their own message because the UI shows it and the user
    // can act on it. Server errors do not: an unexpected throwable's message can
    // carry internals — NatsConnectionProvider, for one, puts the broker URL in
    // its failure message, which would be credentials in any real deployment.
    // The full exception is in the log above.
    String message = status >= 500 ? "internal server error" : ClientErrors.forClient(ex);
    return Response.status(status)
        .type(MediaType.APPLICATION_JSON)
        .entity(json.write(Map.of("error", message)))
        .build();
  }
}
