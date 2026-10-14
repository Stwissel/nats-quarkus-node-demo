package demo;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Fans UI events out to every connected SSE client.
 * Mirrors the {@code sseClients} set in {@code node-service/src/server.ts}.
 */
@ApplicationScoped
public class UiEventBroadcaster {

  @Inject
  JsonCodec json;

  private final BroadcastProcessor<String> processor = BroadcastProcessor.create();

  /** Serialises an event and pushes it to all current subscribers. */
  public void emit(Object event) {
    processor.onNext(json.write(event));
  }

  /** Live feed of serialised frames; each SSE client subscribes once. */
  public Multi<String> stream() {
    return processor;
  }
}
