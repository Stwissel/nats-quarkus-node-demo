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
