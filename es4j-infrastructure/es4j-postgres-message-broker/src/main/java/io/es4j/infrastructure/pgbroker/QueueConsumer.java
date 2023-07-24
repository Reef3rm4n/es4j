package io.es4j.infrastructure.pgbroker;

import io.es4j.infrastructure.pgbroker.models.ConsumerTransaction;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import java.time.Duration;
import java.util.List;


/**
 * When using mono consumer the address entry will be processed only ONCE by either the default implementation
 * which is the one that returns tenant null or the tenant specific implementation which is the implementation that
 * returns a matching tenant in the tenants() method
 *
 * @param <T> The payload, address entry type
 */
public interface QueueConsumer<T> {

  default Uni<Void> start(Vertx vertx, JsonObject componentConfiguration) {
    return Uni.createFrom().voidItem();
  }

  Uni<Void> process(T payload, ConsumerTransaction consumerTransaction);

  default Boolean blocking() {
    return Boolean.FALSE;
  }

  default int schemaVersion() {
    return 0;
  }

  default T migrate(JsonObject message, int fromSchemaVersion) {
    throw new IllegalStateException("Not implemented,unable to migrate from schema-version=%s message=%s".formatted(fromSchemaVersion, message.encode()));
  }

  String address();

  default List<Class<? extends Throwable>> retryOn() {
    return List.of();
  }

  default Duration retryBackOff() {
    return Duration.ofMillis(10);
  }

  default Integer numberOfAttempts() {
    return 5;
  }


}
