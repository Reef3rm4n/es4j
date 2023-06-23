package io.eventx.infrastructure;

import io.eventx.Aggregate;
import io.eventx.infrastructure.models.AggregateEventStream;
import io.eventx.infrastructure.models.AppendInstruction;
import io.eventx.infrastructure.models.Event;
import io.eventx.infrastructure.models.EventStream;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import java.util.List;
import java.util.function.Consumer;

public interface SecondaryEventStore {

  <T extends Aggregate> Uni<List<Event>> fetch(AggregateEventStream<T> aggregateEventStream);

  <T extends Aggregate> Uni<Void> stream(AggregateEventStream<T> aggregateEventStream, Consumer<Event> consumer);

  Uni<List<Event>> fetch(EventStream eventStream);

  Uni<Void> stream(EventStream eventStream, Consumer<Event> consumer);

  // todo make this return the last eventID so that journal might be used more efficiently from the start.
  <T extends Aggregate> Uni<Void> append(AppendInstruction<T> appendInstruction);

  Uni<Void> stop();

  void start(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration);

  Uni<Void> setup(Class<? extends Aggregate> aggregateClass, Vertx vertx, JsonObject configuration);


}
