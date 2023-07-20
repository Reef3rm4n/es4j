package io.es4j.infrastructure;

import io.es4j.Aggregate;
import io.es4j.Es4jDeployment;
import io.es4j.infrastructure.models.*;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;


import java.util.List;
import java.util.function.Consumer;

public interface EventStore {

  <T extends Aggregate> Uni<List<Event>> fetch(AggregateEventStream<T> aggregateEventStream);
  <T extends Aggregate> Uni<Void> stream(AggregateEventStream<T> aggregateEventStream, Consumer<Event> consumer);
  Uni<List<Event>> fetch(EventStream eventStream);
  Uni<Void> stream(EventStream eventStream, Consumer<Event> consumer);

  // todo make this return the last eventID so that journal might be used more efficiently from the start.
  <T extends Aggregate> Uni<Void> append(AppendInstruction<T> appendInstruction);
  <T extends Aggregate> Uni<Void> startStream(StartStream<T> appendInstruction);

  Uni<Void> stop();
  void start(Es4jDeployment es4jDeployment, Vertx vertx, JsonObject configuration);
  Uni<Void> setup(Es4jDeployment es4jDeployment, Vertx vertx, JsonObject configuration);
  <T extends Aggregate> Uni<Void> trim(PruneEventStream<T> trim);

}
