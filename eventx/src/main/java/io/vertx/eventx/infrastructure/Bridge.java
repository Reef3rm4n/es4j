package io.vertx.eventx.infrastructure;
import io.smallrye.mutiny.Uni;


public interface Bridge {

  Uni<Void> start();

  Uni<Void> close();



}
