package demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/**
 * Quarkus 4 ships Jackson 3 (tools.jackson), but the Quarkiverse NATS JetStream
 * connector still injects a Jackson 2 ObjectMapper. This producer bridges the gap.
 */
public class JacksonCompat {

  @Produces
  @ApplicationScoped
  public ObjectMapper jackson2ObjectMapper() {
    return new ObjectMapper();
  }
}
