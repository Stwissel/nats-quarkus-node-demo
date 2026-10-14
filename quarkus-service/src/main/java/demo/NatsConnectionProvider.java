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
      return new Status(false, ClientErrors.redact(natsUrl));
    }
    return new Status(connection.getStatus() == Connection.Status.CONNECTED,
        ClientErrors.redact(connection.getConnectedUrl()));
  }

  void onStop(@Observes ShutdownEvent ev) throws Exception {
    if (connection != null) {
      connection.close();
    }
  }
}
