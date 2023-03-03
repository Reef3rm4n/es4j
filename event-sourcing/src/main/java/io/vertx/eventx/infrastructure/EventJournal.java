package io.vertx.eventx.infrastructure;

import io.smallrye.mutiny.Uni;
import io.vertx.eventx.Aggregate;
import io.vertx.eventx.infrastructure.models.AppendInstruction;
import io.vertx.eventx.infrastructure.models.Event;
import io.vertx.eventx.infrastructure.models.StreamInstruction;

import java.util.List;

public interface EventJournal {

  // todo create an abstraction on top of the current postgres implementation of journal
  // use event store db like api for the abstractions.

  <T extends Aggregate> Uni<List<Event>> stream(StreamInstruction<T> streamInstruction);

  <T extends Aggregate> Uni<Void> append(AppendInstruction<T> appendInstruction);

}
