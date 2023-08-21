package io.es4j.infrastructure.pgbroker;

import io.es4j.infrastructure.pgbroker.models.ConsumerTransaction;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import java.time.Duration;
import java.util.List;


public interface TopicSubscription<T> {

  default Uni<Void> start(Vertx vertx, JsonObject infrastructureConfiguration) {
    return Uni.createFrom().voidItem();
  }

  Uni<Void> process(T payload, ConsumerTransaction consumerTransaction);

  default List<Class<? extends Throwable>> retryOn() {
    return List.of();
  }

  default Duration retryBackOff() {
    return Duration.ofMillis(10);
  }

  default Integer numberOfAttempts() {
    return 5;
  }

  default Boolean blocking() {
    return Boolean.FALSE;
  }

  String address();

  default int schemaVersion() {
    return 0;
  }

  default T migrate(JsonObject message, int fromSchemaVersion) {
    throw new IllegalStateException("Not implemented,unable to migrate from schema-version=%s message=%s".formatted(fromSchemaVersion, message.encode()));
  }

}
