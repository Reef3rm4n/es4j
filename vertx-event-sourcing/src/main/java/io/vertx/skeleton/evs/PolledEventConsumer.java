package io.vertx.skeleton.evs;

import io.vertx.skeleton.evs.objects.PolledEvent;
import io.smallrye.mutiny.Uni;

import java.util.List;

public interface PolledEventConsumer {


  Uni<Void> consumeEvents(List<PolledEvent> entityEvent);
  String eventJournal();
  default List<Class<?>> events() {
    return null;
  }
}
