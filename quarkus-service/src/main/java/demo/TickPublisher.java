package demo;

import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SmallRye Reactive Messaging outgoing channel: a tick every 5 seconds,
 * published through the JetStream connector to subject demo.quarkus.ticks.
 * The connector serialises the record to JSON with Jackson.
 */
@ApplicationScoped
public class TickPublisher {

  public record Tick(String service, long tick, String at) {}

  private final AtomicLong counter = new AtomicLong();

  @Outgoing("ticks")
  public Multi<Tick> ticks() {
    return Multi.createFrom().ticks().every(Duration.ofSeconds(5))
        .onOverflow().drop()
        .map(t -> new Tick("quarkus-service", counter.incrementAndGet(), Instant.now().toString()));
  }
}
