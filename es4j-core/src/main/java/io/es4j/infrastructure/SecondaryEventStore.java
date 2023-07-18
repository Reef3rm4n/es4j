package io.es4j.infrastructure;

import io.es4j.Aggregate;
import io.es4j.Deployment;
import io.es4j.infrastructure.models.AggregateEventStream;
import io.es4j.infrastructure.models.AppendInstruction;
import io.es4j.infrastructure.models.Event;
import io.es4j.infrastructure.models.EventStream;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

import java.util.List;
import java.util.function.Consumer;

public interface SecondaryEventStore {


  Uni<List<Event>> fetch(EventStream eventStream);

  Uni<Void> stream(EventStream eventStream, Consumer<Event> consumer);

  <T extends Aggregate> Uni<Void> append(AppendInstruction<T> appendInstruction);

  Uni<Void> stop();

  void start(Deployment deployment, Vertx vertx, JsonObject configuration);

  Uni<Void> setup(Deployment deployment, Vertx vertx, JsonObject configuration);


}
