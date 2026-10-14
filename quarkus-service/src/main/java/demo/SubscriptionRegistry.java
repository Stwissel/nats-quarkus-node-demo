package demo;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Subscription;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The dynamic subscriptions the web UI creates and removes, plus the
 * announcements that keep every connected UI in step. Java mirror of the
 * subscribe/unsubscribe/list half of {@code node-service/src/nats.ts}.
 *
 * <p>Each UI subscription gets its own jnats {@link Subscription} on one shared
 * {@link Dispatcher}, so it can be cancelled individually by id.
 */
@ApplicationScoped
public class SubscriptionRegistry {

  /** Wire shape of {@code GET} and {@code POST /api/subscriptions}. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record Info(String id, String subject, String queue, String createdAt, long received) {}

  /** Wire shape of a received message on the SSE feed. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record MessageEvent(String kind, String subscriptionId, String subject, String queue,
                             String replyTo, String payload, String receivedAt) {}

  /** Wire shape of the {@code subscribed} announcement on the SSE feed. */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record SubscribedEvent(String kind, String id, String subject, String queue,
                                String createdAt, long received) {}

  private record Entry(Info info, Subscription subscription, AtomicLong received) {}

  @Inject
  NatsConnectionProvider nats;

  @Inject
  UiEventBroadcaster broadcaster;

  private final Map<String, Entry> entries = new ConcurrentHashMap<>();
  private final AtomicInteger nextId = new AtomicInteger(1);
  private Dispatcher dispatcher;

  private synchronized Dispatcher dispatcher() {
    if (dispatcher == null) {
      dispatcher = nats.get().createDispatcher();
    }
    return dispatcher;
  }

  /**
   * Starts a subscription and announces it on the SSE feed.
   *
   * @param queue queue group name, or {@code null} for a plain subscription
   */
  public Info subscribe(String subject, String queue) {
    String id = String.valueOf(nextId.getAndIncrement());
    AtomicLong received = new AtomicLong();
    Dispatcher d = dispatcher();
    Subscription sub = (queue == null)
        ? d.subscribe(subject, msg -> onMessage(id, queue, received, msg))
        : d.subscribe(subject, queue, msg -> onMessage(id, queue, received, msg));

    Info info = new Info(id, subject, queue, Instant.now().toString(), 0);
    entries.put(id, new Entry(info, sub, received));
    broadcaster.emit(new SubscribedEvent("subscribed", info.id(), info.subject(),
        info.queue(), info.createdAt(), 0));
    return info;
  }

  /** Cancels a subscription and announces the removal. Returns false if unknown. */
  public boolean unsubscribe(String id) {
    Entry entry = entries.remove(id);
    if (entry == null) {
      return false;
    }
    dispatcher().unsubscribe(entry.subscription());
    broadcaster.emit(Map.of("kind", "unsubscribed", "id", id));
    return true;
  }

  /** Current subscriptions, each with its live received count. */
  public List<Info> list() {
    List<Info> out = new ArrayList<>();
    for (Entry entry : entries.values()) {
      Info i = entry.info();
      out.add(new Info(i.id(), i.subject(), i.queue(), i.createdAt(), entry.received().get()));
    }
    return out;
  }

  private void onMessage(String id, String queue, AtomicLong received, Message msg) {
    received.incrementAndGet();
    broadcaster.emit(new MessageEvent(
        "message",
        id,
        msg.getSubject(),
        queue,
        msg.getReplyTo(),
        new String(msg.getData(), StandardCharsets.UTF_8),
        Instant.now().toString()));
  }
}
