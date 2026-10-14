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
