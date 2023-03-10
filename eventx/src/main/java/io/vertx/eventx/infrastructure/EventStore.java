package io.vertx.eventx.infrastructure;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.infrastructure.models.AppendInstruction;
import io.vertx.eventx.infrastructure.models.Event;
import io.vertx.eventx.infrastructure.models.AggregateEventStream;
import io.vertx.eventx.infrastructure.models.EventStream;


import java.util.List;
import java.util.function.Consumer;

public interface EventStore {

  <T extends Aggregate> Uni<List<Event>> fetch(AggregateEventStream<T> aggregateEventStream);
  <T extends Aggregate> Uni<Void> stream(AggregateEventStream<T> aggregateEventStream, Consumer<Event> consumer);
  Uni<List<Event>> fetch(EventStream eventStream);
  Uni<Void> stream(EventStream eventStream, Consumer<Event> consumer);

  // todo make this return the last eventID so that journal might be used more efficiently from the start.
  <T extends Aggregate> Uni<Void> append(AppendInstruction<T> appendInstruction);

  Uni<Void> close();
  Uni<Void> start();


}
