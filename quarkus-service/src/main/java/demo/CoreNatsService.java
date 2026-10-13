package demo;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

/**
 * Core NATS features that sit outside JetStream: request/reply and queue groups.
 * Uses the jnats client directly (the same client the SmallRye connector builds on).
 */
@ApplicationScoped
public class CoreNatsService {

  private static final Logger LOG = Logger.getLogger(CoreNatsService.class);

  @ConfigProperty(name = "nats.url")
  String natsUrl;

  private Connection connection;

  void onStart(@Observes StartupEvent ev) throws Exception {
    connection = Nats.connect(
        Options.builder()
            .server(natsUrl)
            .connectionTimeout(Duration.ofSeconds(10))
            .maxReconnects(-1)
            .build());
    LOG.infof("Connected to NATS at %s", natsUrl);

    Dispatcher dispatcher = connection.createDispatcher();

    // Request/reply: anyone requesting demo.greet gets a greeting from this service.
    dispatcher.subscribe("demo.greet", msg -> {
      String name = new String(msg.getData(), StandardCharsets.UTF_8).trim();
      String reply = "Hello, " + (name.isEmpty() ? "stranger" : name) + "! — greetings from quarkus-service";
      connection.publish(msg.getReplyTo(), reply.getBytes(StandardCharsets.UTF_8));
      LOG.infof("Replied on demo.greet to '%s'", name);
    });

    // Queue group: both quarkus-service and node-service join queue group "workers"
    // on demo.work, so NATS load-balances each request to exactly one of them.
    dispatcher.subscribe("demo.work", "workers", msg -> {
      String payload = new String(msg.getData(), StandardCharsets.UTF_8);
      String reply = "work item '" + payload + "' processed by quarkus-service at " + Instant.now();
      if (msg.getReplyTo() != null) {
        connection.publish(msg.getReplyTo(), reply.getBytes(StandardCharsets.UTF_8));
      }
      LOG.infof("Processed work item from queue group: %s", payload);
    });
  }

  /** Lets other beans announce things on core NATS subjects. */
  public void publish(String subject, String payload) {
    if (connection != null) {
      connection.publish(subject, payload.getBytes(StandardCharsets.UTF_8));
    }
  }

  void onStop(@Observes ShutdownEvent ev) throws Exception {
    if (connection != null) {
      connection.close();
    }
  }
}
