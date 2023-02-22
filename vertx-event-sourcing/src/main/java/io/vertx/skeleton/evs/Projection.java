package io.vertx.skeleton.evs;

import io.vertx.skeleton.evs.objects.PolledEvent;
import io.smallrye.mutiny.Uni;

import java.util.List;

public interface Projection<T> {


  Uni<Void> update(T state, List<Event> events);


}
