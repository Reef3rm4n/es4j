package io.vertx.skeleton.evs;

import io.vertx.skeleton.evs.objects.PolledEvent;
import io.smallrye.mutiny.Uni;

import java.util.List;

public interface EventConsumer {


  Uni<Void> consumeEvents(List<PolledEvent> entityEvent);
  Class<? extends EntityAggregate> entity();
  default List<Class<?>> events() {
    return null;
  }

}
