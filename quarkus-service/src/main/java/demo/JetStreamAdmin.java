package demo;

import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.PublishAck;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.nats.client.JetStreamManagement;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.StreamInfo;
import java.util.ArrayList;
import java.util.List;
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

  /** Wire shape of one consumer inside the stream view. */
  public record ConsumerView(String name, String durable, long delivered, long pending) {}

  /** Wire shape of {@code GET /api/jetstream}. */
  public record StreamView(String stream, List<String> subjects, String storage, long messages,
                           long firstSeq, long lastSeq, List<ConsumerView> consumers) {}

  /** Wire shape of one entry from {@code GET /api/jetstream/messages}. */
  public record StoredMessage(long seq, String subject, String payload, String time) {}

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
      } catch (JetStreamApiException gap) {
        // Skip only the broker saying "no message found" (a deleted sequence).
        // A connection failure used to look identical, so the loop kept walking
        // every remaining sequence — one round trip each — and then returned a
        // partial list as 200. Anything else, including IOException, propagates
        // to the route's 502. Matches NatsBridge.lastMessages in node-service.
        if (gap.getErrorCode() != 404) {
          throw gap;
        }
      }
    }
    return out;
  }
}
