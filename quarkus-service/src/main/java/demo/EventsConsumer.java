package demo;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionStage;

/**
 * SmallRye Reactive Messaging incoming channel bound to the JetStream durable
 * consumer "quarkus-events" on stream DEMO (see application.properties).
 * Announces every consumed event on a core subject so the web UI can watch it.
 */
@ApplicationScoped
public class EventsConsumer {

  private static final Logger LOG = Logger.getLogger(EventsConsumer.class);

  @Inject
  CoreNatsService coreNats;

  @Incoming("events")
  public CompletionStage<Void> consume(Message<byte[]> message) {
    String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
    LOG.infof("JetStream event consumed: %s", payload);
    coreNats.publish("demo.quarkus.seen", "quarkus-service consumed JetStream event: " + payload);
    return message.ack();
  }
}
